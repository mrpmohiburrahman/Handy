//! Live transcription over the Gemini Live API (`gemini-3.5-transcribe-live`).
//!
//! A drop-in alternative to the local streaming engine. Gemini's two
//! transcription fields map straight onto the overlay's two text zones:
//! `interimInputTranscription` is the volatile `tentative` suffix the model may
//! still rewrite, `inputTranscription` is the authoritative text appended to
//! `committed`. Because the shapes already line up, nothing downstream of
//! [`crate::managers::transcription::StreamTextEvent`] changes.
//!
//! The public surface deliberately mirrors transcribe-cpp's `Stream`
//! (`feed` / `finalize` / `cancel`) so the streaming worker treats both
//! backends the same way.
//!
//! ponytail: spike. The API key, transcription mode and custom vocabulary come
//! from the environment (`HANDY_GEMINI_KEY`, `HANDY_GEMINI_MODE`,
//! `HANDY_GEMINI_VOCABULARY`), the model is fixed, and a mid-session network
//! failure ends the run with whatever text arrived rather than falling back to
//! the local engine. Promote to settings fields + a model-picker entry if this
//! proves out.

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::sync::mpsc;
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::tungstenite::Message;

const MODEL: &str = "models/gemini-3.5-transcribe-live";
const WS_HOST: &str = "wss://generativelanguage.googleapis.com/ws/\
                       google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";

/// Language hints sent to the model. Empty means automatic detection, which is
/// what Google documents for multilingual speech and code-switching. A hint
/// only *biases* recognition and cannot restrict it (probing Hindi returns
/// Devanagari with or without `["bn-BD", "en-US"]`), so pinning two languages
/// bought no filtering and risked steering mixed Bangla/English sentences.
/// The allowlist is enforced on the returned text instead — see
/// [`filter_transcript`].
const LANGUAGE_CODES: [&str; 0] = [];

/// ISO 639-1 codes of the languages a returned transcript may be in.
const ACCEPTED_LANGUAGES: [&str; 2] = ["bn", "en"];

/// Environment variable selecting the transcription mode (`SMART` or
/// `VERBATIM`).
const MODE_ENV: &str = "HANDY_GEMINI_MODE";

/// Environment variable holding comma- or newline-separated terms to bias
/// recognition toward (names, brands, jargon).
const VOCABULARY_ENV: &str = "HANDY_GEMINI_VOCABULARY";

/// The API accepts at most this many custom vocabulary terms.
const MAX_VOCABULARY_TERMS: usize = 1000;

/// How the model words the finalized transcript.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TranscriptionMode {
    /// Literal transcript: filler words, repetitions and false starts are kept.
    Verbatim,
    /// Cleaned for reading: fillers and false starts removed, spoken
    /// corrections resolved, punctuation and casing applied.
    Smart,
}

impl TranscriptionMode {
    /// Dictation is pasted straight into other apps, so the cleaned-up form is
    /// the default; anything unrecognised falls back to it rather than failing.
    fn parse(value: Option<&str>) -> Self {
        match value.map(|v| v.trim().to_ascii_uppercase()).as_deref() {
            Some("VERBATIM") => Self::Verbatim,
            Some("SMART") | Some("") | None => Self::Smart,
            Some(other) => {
                warn!(
                    "{} is '{}'; expected SMART or VERBATIM. Using SMART",
                    MODE_ENV, other
                );
                Self::Smart
            }
        }
    }

    fn as_api_value(self) -> &'static str {
        match self {
            Self::Verbatim => "VERBATIM",
            Self::Smart => "SMART",
        }
    }
}

/// Split a vocabulary setting into distinct, non-empty terms, keeping the
/// order they were written in and dropping anything past the API limit.
fn parse_vocabulary(raw: &str) -> Vec<String> {
    let mut terms: Vec<String> = Vec::new();
    for term in raw.split([',', '\n']).map(str::trim) {
        if !term.is_empty() && !terms.iter().any(|seen| seen == term) {
            terms.push(term.to_string());
        }
    }
    if terms.len() > MAX_VOCABULARY_TERMS {
        warn!(
            "{} lists {} terms; only the first {} are sent",
            VOCABULARY_ENV,
            terms.len(),
            MAX_VOCABULARY_TERMS
        );
        terms.truncate(MAX_VOCABULARY_TERMS);
    }
    terms
}

/// The first frame of a session: model, output modality and transcription
/// options. `customVocabulary` is left out entirely when there are no terms.
fn setup_message(mode: TranscriptionMode, vocabulary: &[String]) -> Value {
    let mut transcription = json!({
        "languageCodes": LANGUAGE_CODES,
        "mode": mode.as_api_value(),
    });
    if !vocabulary.is_empty() {
        transcription["customVocabulary"] = json!(vocabulary);
    }
    json!({
        "setup": {
            "model": MODEL,
            "generationConfig": { "responseModalities": ["TEXT"] },
            "inputAudioTranscription": transcription
        }
    })
}

/// Letters that can only belong to a language we did not ask for. Bangla is
/// Bengali script and English is Latin, so any other alphabet — Devanagari,
/// Tamil, Cyrillic, CJK — is decisive on its own and needs no detector.
/// Romanized Bangla and Hindi stay Latin and fall through to the detector.
fn is_foreign_script(c: char) -> bool {
    c.is_alphabetic() && !is_wanted_script(c)
}

fn is_wanted_script(c: char) -> bool {
    c.is_ascii()
        || ('\u{00C0}'..='\u{024F}').contains(&c) // Latin-1 Supplement + Extended A/B
        || ('\u{1E00}'..='\u{1EFF}').contains(&c) // Latin Extended Additional
        || ('\u{0980}'..='\u{09FF}').contains(&c) // Bengali
}

/// Reduce a finalized transcript to the part that plausibly belongs to a
/// language we asked for.
///
/// The model transcribes whatever it hears, so the allowlist the API refuses to
/// enforce is applied here instead. Script is checked first because it is
/// decisive: probing the live model with Hindi returns Devanagari, and the
/// detector alone answers `None` for Devanagari (it cannot separate Hindi from
/// Marathi) which would let the text through. Only the offending words are
/// removed: one misheard word in another alphabet used to discard the whole
/// dictation, which looked exactly like the model having heard nothing.
///
/// The detector then catches Latin-script languages it is confident about,
/// e.g. French, and those still drop the text as a whole — there is no
/// per-word signal to cut along.
///
/// Fails open — only a confident detection of some *other* language rejects the
/// text, so romanized Bangla and short or ambiguous utterances are never
/// silently dropped.
pub fn filter_transcript(text: &str) -> String {
    let kept = strip_foreign_script(text);
    match crate::audio_toolkit::lang_id::detect_output_language(&kept, &[]) {
        Some(language) if !ACCEPTED_LANGUAGES.contains(&language.as_str()) => {
            info!(
                "Gemini Live transcript dropped: detected language '{}' is neither Bangla nor English",
                language
            );
            String::new()
        }
        _ => kept,
    }
}

/// Remove every word containing a letter from an alphabet we did not ask for.
/// Text without such a letter is returned untouched, so the line breaks and
/// spacing of a formatted transcript survive.
fn strip_foreign_script(text: &str) -> String {
    if !text.chars().any(is_foreign_script) {
        return text.to_string();
    }
    let mut removed = 0usize;
    let lines: Vec<String> = text
        .lines()
        .map(|line| {
            line.split_whitespace()
                .filter(|word| {
                    let foreign = word.chars().any(is_foreign_script);
                    removed += usize::from(foreign);
                    !foreign
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|line| !line.is_empty())
        .collect();
    info!(
        "Gemini Live transcript: removed {} word(s) in a script that is neither Bengali nor Latin",
        removed
    );
    lines.join("\n")
}

/// Gemini wants ~100 ms per chunk (1,024-2,048 frames at 16 kHz). The audio
/// recorder hands us much smaller frames, so `feed` coalesces them to this size
/// before anything goes over the socket.
const CHUNK_SAMPLES: usize = 1600;

/// How long `connect` waits for the socket to come up before giving up. Audio
/// is already being captured by then, so this has to stay short.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);

/// How long `finalize` waits for Gemini to flush its remaining transcripts after
/// `audioStreamEnd`. Must stay well under the caller's own finalize timeout
/// (`STREAM_FINALIZE_REPLY_TIMEOUT`) so the slow path is still attributed here.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to keep reading after `audioStreamEnd` when the model has already
/// closed out everything it heard — just long enough to catch a frame that was
/// already in flight. See `drain_budget`.
const FLUSH_GRACE: Duration = Duration::from_millis(600);

/// The API key for the spike, or `None` to leave the local engine in charge.
/// Presence of a key is what switches Handy over to Gemini.
pub fn api_key() -> Option<String> {
    ensure_env_loaded();
    ["HANDY_GEMINI_KEY", "GEMINI_API_KEY"]
        .iter()
        .find_map(|name| std::env::var(name).ok())
        .map(|key| key.trim().trim_matches('"').to_string())
        .filter(|key| !key.is_empty())
}

/// Make every `HANDY_*` knob written in a `.env` file visible to the process,
/// no matter how the app was launched. Called once; later calls are no-ops.
///
/// A repo-root `.env` is found in three situations that all differ by working
/// directory, so we try each candidate location in turn:
///
/// 1. Walk up from the current working directory — covers `tauri dev`, where
///    the CWD is the project root.
/// 2. Walk up from the executable — covers a bundle (`Handy.app`) launched from
///    Finder or the tray, whose CWD is `/` and therefore never reaches the repo.
///    The executable still lives under the repo, so the walk finds the same
///    `.env`.
/// 3. The portable / app-data directory — covers installed and portable builds
///    that keep their config outside the signed bundle.
///
/// `dotenvy` keeps the first value it sees, so an explicit environment variable
/// always wins over a file.
pub(crate) fn ensure_env_loaded() {
    static LOADED: OnceLock<bool> = OnceLock::new();
    LOADED.get_or_init(|| {
        if let Ok(path) = dotenvy::dotenv() {
            eprintln!("[env] loaded {}", path.display());
        }
        if let Ok(exe) = std::env::current_exe() {
            for dir in exe.ancestors().skip(1) {
                let candidate = dir.join(".env");
                if candidate.is_file() {
                    match dotenvy::from_path(&candidate) {
                        Ok(()) => eprintln!("[env] loaded {}", candidate.display()),
                        Err(e) => eprintln!("[env] could not read {}: {}", candidate.display(), e),
                    }
                    break;
                }
            }
        }
        if let Some(env_path) = crate::portable::app_data_dir_early().map(|dir| dir.join(".env")) {
            match dotenvy::from_path(&env_path) {
                Ok(()) => eprintln!("[env] loaded {}", env_path.display()),
                Err(e) if !e.not_found() => {
                    eprintln!("[env] could not read {}: {}", env_path.display(), e)
                }
                Err(_) => {}
            }
        }
        true
    });
}

/// Commands sent to the async socket task.
enum Cmd {
    Audio(Vec<u8>),
    /// Close the input stream and start draining final transcripts.
    End,
    Abort,
}

/// A live transcription session against the Gemini Live API.
pub struct GeminiSession {
    cmd_tx: UnboundedSender<Cmd>,
    /// Receives the accumulated transcript once the socket task finishes.
    done_rx: mpsc::Receiver<Result<String>>,
    /// Samples not yet big enough to be worth a frame on the wire.
    pending: Mutex<Vec<f32>>,
    /// Shared with the socket task so `finalize` can still salvage text if the
    /// task never reports a clean end (e.g. the server never closes the turn).
    transcript: Arc<Mutex<Transcript>>,
}

impl GeminiSession {
    /// Open a session and wait for the socket to be established.
    ///
    /// `on_text` is invoked as `(committed, tentative)` on every transcription
    /// update, matching the overlay's two text zones.
    pub fn connect(
        api_key: String,
        on_text: impl Fn(String, String) + Send + 'static,
    ) -> Result<Self> {
        let (cmd_tx, cmd_rx) = unbounded_channel();
        let (done_tx, done_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let transcript = Arc::new(Mutex::new(Transcript::default()));

        let task_transcript = Arc::clone(&transcript);
        tauri::async_runtime::spawn(async move {
            let outcome = run_socket(api_key, on_text, cmd_rx, ready_tx, task_transcript).await;
            if let Err(e) = &outcome {
                error!("Gemini Live session ended with an error: {}", e);
            }
            // A disconnected receiver just means the worker gave up first.
            let _ = done_tx.send(outcome);
        });

        match ready_rx.recv_timeout(CONNECT_TIMEOUT) {
            Ok(Ok(())) => Ok(Self {
                cmd_tx,
                done_rx,
                pending: Mutex::new(Vec::new()),
                transcript,
            }),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(anyhow!(
                "Timed out after {:?} connecting to the Gemini Live API",
                CONNECT_TIMEOUT
            )),
        }
    }

    /// Queue a 16 kHz mono frame. Cheap: samples are buffered here and only
    /// converted to PCM16 once a full chunk has accumulated.
    pub fn feed(&self, frame: &[f32]) {
        let mut pending = self.pending.lock().unwrap();
        pending.extend_from_slice(frame);
        while pending.len() >= CHUNK_SAMPLES {
            let chunk: Vec<f32> = pending.drain(..CHUNK_SAMPLES).collect();
            if self.cmd_tx.send(Cmd::Audio(to_pcm16(&chunk))).is_err() {
                // Socket task is gone; finalize will surface the real error.
                return;
            }
        }
    }

    /// Flush the tail, close the input stream, and return the full transcript.
    pub fn finalize(&self) -> Result<String> {
        {
            let mut pending = self.pending.lock().unwrap();
            if !pending.is_empty() {
                let tail = std::mem::take(&mut *pending);
                let _ = self.cmd_tx.send(Cmd::Audio(to_pcm16(&tail)));
            }
        }
        let _ = self.cmd_tx.send(Cmd::End);
        match self.done_rx.recv_timeout(DRAIN_TIMEOUT) {
            Ok(result) => result,
            // The task never reported a clean end. Salvage whatever it did
            // receive rather than discarding a good transcript and forcing a
            // batch re-run the user already spoke for.
            Err(_) => {
                let text = self.transcript.lock().unwrap().full();
                warn!(
                    "Gemini Live did not close its turn within {:?}; using the {} character(s) received so far",
                    DRAIN_TIMEOUT,
                    text.len()
                );
                Ok(text)
            }
        }
    }

    /// Abandon the session without producing text.
    pub fn cancel(&self) {
        let _ = self.cmd_tx.send(Cmd::Abort);
    }
}

/// Convert 16 kHz mono f32 samples to the raw little-endian PCM16 Gemini wants.
fn to_pcm16(frame: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(frame.len() * 2);
    for &sample in frame {
        let scaled = (sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16;
        out.extend_from_slice(&scaled.to_le_bytes());
    }
    out
}

/// Accumulated transcript state, kept in the shape the overlay renders.
#[derive(Default)]
struct Transcript {
    committed: String,
    tentative: String,
}

impl Transcript {
    /// Apply one `serverContent` payload. Returns true if either zone changed.
    fn apply(&mut self, content: &Value) -> bool {
        let mut changed = false;

        // A finalized segment supersedes whatever interim text was showing.
        if let Some(text) = content
            .get("inputTranscription")
            .and_then(|t| t.get("text"))
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
        {
            if !self.committed.is_empty() {
                self.committed.push(' ');
            }
            self.committed.push_str(text);
            self.tentative.clear();
            changed = true;
        }

        if let Some(text) = content
            .get("interimInputTranscription")
            .and_then(|t| t.get("text"))
            .and_then(Value::as_str)
        {
            if self.tentative != text {
                self.tentative = text.to_string();
                changed = true;
            }
        }

        changed
    }

    /// Everything transcribed so far. Any interim tail is kept: it is the only
    /// record of speech Gemini never got to finalize.
    fn full(&self) -> String {
        let mut text = self.committed.clone();
        if !self.tentative.is_empty() {
            if !text.is_empty() {
                text.push(' ');
            }
            text.push_str(&self.tentative);
        }
        text
    }
}

/// Own the socket for one session: connect, stream audio up, stream text down.
async fn run_socket(
    api_key: String,
    on_text: impl Fn(String, String) + Send + 'static,
    mut cmd_rx: UnboundedReceiver<Cmd>,
    ready_tx: mpsc::Sender<Result<()>>,
    transcript: Arc<Mutex<Transcript>>,
) -> Result<String> {
    let url = format!("{}?key={}", WS_HOST, api_key);
    let (socket, _) = match tokio_tungstenite::connect_async(&url).await {
        Ok(pair) => pair,
        Err(e) => {
            let err = anyhow!("Failed to connect to the Gemini Live API: {}", e);
            let _ = ready_tx.send(Err(anyhow!("{}", err)));
            return Err(err);
        }
    };
    // Split so the send half and the receive half are independent borrows:
    // `select!` below holds the read future alive across the write arm's body.
    let (mut write, mut read) = socket.split();

    let mode = TranscriptionMode::parse(std::env::var(MODE_ENV).ok().as_deref());
    let vocabulary = parse_vocabulary(&std::env::var(VOCABULARY_ENV).unwrap_or_default());
    debug!(
        "Gemini Live setup: mode={}, {} custom vocabulary term(s)",
        mode.as_api_value(),
        vocabulary.len()
    );
    let setup = setup_message(mode, &vocabulary);
    if let Err(e) = write.send(Message::text(setup.to_string())).await {
        let err = anyhow!("Failed to send the Gemini Live setup message: {}", e);
        let _ = ready_tx.send(Err(anyhow!("{}", err)));
        return Err(err);
    }

    // Wait for `setupComplete` before declaring the session usable. Having sent
    // the setup frame proves nothing: a bad key, an unknown model or a rejected
    // config all arrive here as an error frame or a close. Reporting success at
    // send time turns every one of them into a silently empty transcript and a
    // confusing "model is not loaded" batch fallback downstream.
    let handshake = loop {
        match read.next().await {
            Some(Ok(Message::Close(frame))) => {
                break Err(anyhow!(
                    "Gemini Live rejected the session: {}",
                    close_detail(frame.map(|f| (u16::from(f.code), f.reason.to_string())))
                ));
            }
            Some(Ok(message)) => {
                let Some(payload) = frame_payload(&message) else {
                    continue;
                };
                let Ok(value) = serde_json::from_str::<Value>(&payload) else {
                    continue;
                };
                if let Some(message) = server_error(&value) {
                    break Err(anyhow!("Gemini Live rejected the session: {}", message));
                }
                if value.get("setupComplete").is_some() {
                    break Ok(());
                }
                debug!("Gemini Live frame before setupComplete: {}", payload);
            }
            Some(Err(e)) => break Err(anyhow!("Gemini Live socket failed during setup: {}", e)),
            None => break Err(anyhow!("Gemini Live closed the connection during setup")),
        }
    };
    if let Err(e) = handshake {
        let detail = format!("{}", e);
        let _ = ready_tx.send(Err(anyhow!("{}", detail)));
        return Err(e);
    }

    info!("Gemini Live session established ({})", MODEL);
    let _ = ready_tx.send(Ok(()));

    // Once the input stream is closed we are only waiting for Gemini to flush.
    let mut input_closed = false;
    // Whether the model still owes us a completion for speech it has begun
    // transcribing. Cleared by `generationComplete`.
    let mut pending_speech = false;
    // Set when the input closes: the point at which we stop waiting for more.
    let mut deadline: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                Some(Cmd::Audio(pcm)) => {
                    let msg = json!({
                        "realtimeInput": {
                            "audio": {
                                "data": STANDARD.encode(&pcm),
                                "mimeType": "audio/pcm;rate=16000"
                            }
                        }
                    });
                    if let Err(e) = write.send(Message::text(msg.to_string())).await {
                        warn!("Gemini Live audio send failed: {}", e);
                        break;
                    }
                }
                Some(Cmd::End) => {
                    let msg = json!({ "realtimeInput": { "audioStreamEnd": true } });
                    if let Err(e) = write.send(Message::text(msg.to_string())).await {
                        warn!("Gemini Live audioStreamEnd failed: {}", e);
                        break;
                    }
                    input_closed = true;
                    deadline = Some(tokio::time::Instant::now() + drain_budget(pending_speech));
                }
                Some(Cmd::Abort) | None => {
                    let _ = write.close().await;
                    return Ok(String::new());
                }
            },

            // Disabled until the input closes, so this never fires mid-dictation.
            _ = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            }, if deadline.is_some() => {
                debug!("Gemini Live went quiet after audioStreamEnd; finishing with what arrived");
                break;
            }

            frame = read.next() => {
                let Some(frame) = frame else {
                    warn!("Gemini Live closed the connection without closing the turn");
                    break;
                };
                let payload = match frame {
                    Ok(Message::Close(close)) => {
                        let detail =
                            close_detail(close.map(|f| (u16::from(f.code), f.reason.to_string())));
                        // After audioStreamEnd a close is the normal end of the
                        // session; before it, it means we lost the stream.
                        if input_closed {
                            debug!("Gemini Live closed after audioStreamEnd: {}", detail);
                        } else {
                            warn!("Gemini Live closed mid-session: {}", detail);
                        }
                        break;
                    }
                    Ok(message) => match frame_payload(&message) {
                        Some(payload) => payload,
                        None => continue,
                    },
                    Err(e) => {
                        warn!("Gemini Live socket error: {}", e);
                        break;
                    }
                };

                let Ok(value) = serde_json::from_str::<Value>(&payload) else {
                    debug!("Ignoring non-JSON Gemini Live frame");
                    continue;
                };

                if let Some(message) = server_error(&value) {
                    error!("Gemini Live reported an error: {}", message);
                    break;
                }

                if let Some(content) = value.get("serverContent") {
                    // Scope the guard: it must not be held across the callback,
                    // and never across an await (the future has to stay Send).
                    let update = {
                        let mut t = transcript.lock().unwrap();
                        t.apply(content)
                            .then(|| (t.committed.clone(), t.tentative.clone()))
                    };
                    if let Some((committed, tentative)) = update {
                        on_text(committed, tentative);
                    }
                    if has_transcription(content) {
                        pending_speech = true;
                        // Something is genuinely still being flushed, so grant
                        // the full budget even if we armed the short grace.
                        if input_closed {
                            deadline = Some(tokio::time::Instant::now() + DRAIN_TIMEOUT);
                        }
                    }
                    if is_turn_complete(content) {
                        pending_speech = false;
                        // Mid-session completions are just pauses in dictation,
                        // not the end of the run.
                        if input_closed {
                            break;
                        }
                    }
                }
            }
        }
    }

    let _ = write.close().await;
    let text = transcript.lock().unwrap().full();
    if text.is_empty() {
        warn!("Gemini Live session produced no transcript");
    }
    Ok(text)
}

/// Text payload of a frame, if it carries one. Gemini sends JSON as either a
/// text or a binary frame depending on the transport, so both are unwrapped the
/// same way; control frames carry nothing and yield `None`.
fn frame_payload(message: &Message) -> Option<String> {
    match message {
        Message::Text(text) => Some(text.to_string()),
        Message::Binary(bytes) => String::from_utf8(bytes.to_vec()).ok(),
        _ => None,
    }
}

/// The human-readable message from a server error frame, if this is one.
/// Covers both `{"error": {...}}` and a bare `{"error": "..."}`.
fn server_error(value: &Value) -> Option<String> {
    let error = value.get("error")?;
    if let Some(text) = error.as_str() {
        return Some(text.to_string());
    }
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    match error.get("status").and_then(Value::as_str) {
        Some(status) => Some(format!("{} ({})", message, status)),
        None => Some(message.to_string()),
    }
}

/// Whether a `serverContent` payload closes the current turn.
///
/// The Live Transcription model signals this as `generationComplete`; the
/// conversational Live API spells it `turnComplete`. Both are accepted so the
/// run ends on whichever arrives — waiting only for `turnComplete` stalled every
/// dictation until the drain timeout, because it is never sent by this model.
fn is_turn_complete(content: &Value) -> bool {
    ["generationComplete", "turnComplete"]
        .iter()
        .any(|key| content.get(key).and_then(Value::as_bool).unwrap_or(false))
}

/// How long to keep reading after `audioStreamEnd`.
///
/// This model emits `generationComplete` at every segment boundary, including
/// while the user is still holding the key. Speaking one sentence and pausing
/// before releasing therefore completes the turn *before* the input closes, and
/// the server then sends nothing further — captured frames show 19s of silence
/// after the last completion. Waiting for another one stalled every dictation
/// for the full `DRAIN_TIMEOUT`.
///
/// So: only wait the full budget when the model is mid-utterance and genuinely
/// owes us a flush. Otherwise a brief grace is enough to catch a frame already
/// in flight.
fn drain_budget(pending_speech: bool) -> Duration {
    if pending_speech {
        DRAIN_TIMEOUT
    } else {
        FLUSH_GRACE
    }
}

/// Whether a `serverContent` payload carries transcription activity, i.e. the
/// model has started on speech it has not yet closed out with a completion.
fn has_transcription(content: &Value) -> bool {
    ["interimInputTranscription", "inputTranscription"]
        .iter()
        .any(|key| content.get(key).is_some())
}

/// Render a close frame's code and reason for logging.
fn close_detail(frame: Option<(u16, String)>) -> String {
    match frame {
        Some((code, reason)) if !reason.is_empty() => format!("close {} — {}", code, reason),
        Some((code, _)) => format!("close {} (no reason given)", code),
        None => "connection closed with no close frame".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pcm16_is_little_endian_and_clamped() {
        assert_eq!(to_pcm16(&[0.0]), vec![0, 0]);
        // Full scale, and anything beyond it, pins to i16::MAX (0x7FFF).
        assert_eq!(to_pcm16(&[1.0]), vec![0xFF, 0x7F]);
        assert_eq!(to_pcm16(&[9.0]), vec![0xFF, 0x7F]);
        assert_eq!(to_pcm16(&[-9.0]), vec![0x01, 0x80]);
        assert_eq!(to_pcm16(&[0.0, 1.0]).len(), 4);
    }

    #[test]
    fn interim_text_is_volatile_and_finals_accumulate() {
        let mut t = Transcript::default();

        assert!(t.apply(&json!({ "interimInputTranscription": { "text": "hello wor" } })));
        assert_eq!(t.tentative, "hello wor");
        assert_eq!(t.committed, "");

        // Re-sending identical interim text is not an update.
        assert!(!t.apply(&json!({ "interimInputTranscription": { "text": "hello wor" } })));

        // A final segment clears the interim tail it supersedes.
        assert!(t.apply(&json!({ "inputTranscription": { "text": "Hello world." } })));
        assert_eq!(t.committed, "Hello world.");
        assert_eq!(t.tentative, "");

        // Later segments are appended, not replaced.
        t.apply(&json!({ "inputTranscription": { "text": "How are you?" } }));
        assert_eq!(t.committed, "Hello world. How are you?");

        // An un-finalized tail still makes it into the pasted text.
        t.apply(&json!({ "interimInputTranscription": { "text": "and th" } }));
        assert_eq!(t.full(), "Hello world. How are you? and th");
    }

    #[test]
    fn server_errors_are_recognised_and_described() {
        // The shape the API actually returns for a rejected key.
        assert_eq!(
            server_error(&json!({
                "error": { "code": 400, "message": "API key not valid. Please pass a valid API key.", "status": "INVALID_ARGUMENT" }
            })),
            Some("API key not valid. Please pass a valid API key. (INVALID_ARGUMENT)".to_string())
        );
        assert_eq!(
            server_error(&json!({ "error": "boom" })),
            Some("boom".to_string())
        );
        // A normal transcription frame is not an error.
        assert_eq!(
            server_error(&json!({ "serverContent": { "inputTranscription": { "text": "hi" } } })),
            None
        );
    }

    #[test]
    fn close_frames_keep_their_reason() {
        // 1007 + this reason is what an invalid key produces; it must survive
        // into the log rather than being swallowed.
        assert_eq!(
            close_detail(Some((1007, "API key not valid.".to_string()))),
            "close 1007 — API key not valid."
        );
        assert_eq!(
            close_detail(Some((1000, String::new()))),
            "close 1000 (no reason given)"
        );
        assert_eq!(close_detail(None), "connection closed with no close frame");
    }

    #[test]
    fn empty_payloads_do_not_dirty_the_transcript() {
        let mut t = Transcript::default();
        assert!(!t.apply(&json!({ "turnComplete": true })));
        assert!(!t.apply(&json!({ "inputTranscription": { "text": "" } })));
        assert_eq!(t.full(), "");
    }

    #[test]
    fn turn_completion_accepts_the_signal_this_model_actually_sends() {
        // Captured from a real gemini-3.5-transcribe-live session: this model
        // ends a turn with `generationComplete`, never `turnComplete`.
        assert!(is_turn_complete(&json!({ "generationComplete": true })));
        // The conversational Live API's spelling still works.
        assert!(is_turn_complete(&json!({ "turnComplete": true })));
        // Ordinary transcription frames must not end the run.
        assert!(!is_turn_complete(
            &json!({ "inputTranscription": { "text": "hello" } })
        ));
        assert!(!is_turn_complete(&json!({})));
        assert!(!is_turn_complete(&json!({ "generationComplete": false })));
    }

    #[test]
    fn drain_budget_is_short_once_the_model_has_closed_its_turn() {
        // Captured from a real session: `generationComplete` for the last
        // segment arrives BEFORE audioStreamEnd when the user pauses before
        // releasing, and nothing follows it. Waiting the full drain there added
        // 10s to every dictation.
        assert_eq!(drain_budget(false), FLUSH_GRACE);
        // Released mid-utterance: the model still has audio to flush.
        assert_eq!(drain_budget(true), DRAIN_TIMEOUT);
        assert!(FLUSH_GRACE < DRAIN_TIMEOUT);
    }

    #[test]
    fn transcription_activity_is_distinguished_from_completion() {
        assert!(has_transcription(
            &json!({ "interimInputTranscription": { "text": "Sec" } })
        ));
        assert!(has_transcription(
            &json!({ "inputTranscription": { "text": "Second." } })
        ));
        // The signals that close a turn are not themselves activity, and this
        // model sends them in their own frame.
        assert!(!has_transcription(&json!({ "generationComplete": true })));
        assert!(!has_transcription(&json!({})));
    }

    #[test]
    fn transcripts_in_the_wanted_languages_are_kept() {
        let english = "First sentence about apples and second sentence about oranges.";
        assert_eq!(filter_transcript(english), english);
        let bangla = "আমার নাম লিখা এবং আমি বাংলা বলি।";
        assert_eq!(filter_transcript(bangla), bangla);
        // Code-switching inside one sentence is the normal case, not an error.
        let mixed = "আমি আজ অফিসে একটা meeting এ যাব, তারপর pull request review করব।";
        assert_eq!(filter_transcript(mixed), mixed);
    }

    #[test]
    fn foreign_scripts_are_rejected() {
        // The exact line the live Hindi probe returned.
        assert_eq!(
            filter_transcript("मेरा नाम लेखा है और मैं हिंदी बोलती हूं।"),
            "",
            "Devanagari must be rejected"
        );
        assert_eq!(filter_transcript("இது ஒரு தமிழ் வாக்கியம்"), "");
        assert_eq!(filter_transcript("这是一个完全用中文写成的句子"), "");
    }

    #[test]
    fn one_foreign_word_does_not_cost_the_whole_transcript() {
        // A single misheard word in another alphabet used to discard the lot.
        assert_eq!(
            filter_transcript("Please send the invoice नमस्ते to the client before Friday."),
            "Please send the invoice to the client before Friday."
        );
        assert_eq!(
            filter_transcript("আমি আজ বাজারে যাব मैं এবং কিছু সবজি কিনব।"),
            "আমি আজ বাজারে যাব এবং কিছু সবজি কিনব।"
        );
    }

    #[test]
    fn formatting_survives_the_filter() {
        // SMART mode returns lists and paragraphs; clean text passes untouched.
        let list = "Shopping list:\n- apples\n- oranges\n\nCall the office tomorrow.";
        assert_eq!(filter_transcript(list), list);
        // With a foreign word present, line structure is still kept.
        assert_eq!(
            filter_transcript("Shopping list:\n- apples 苹果\n- oranges"),
            "Shopping list:\n- apples\n- oranges"
        );
    }

    #[test]
    fn detected_foreign_latin_languages_are_rejected() {
        assert_eq!(
            filter_transcript("je m'appelle likha et je parle francais tous les jours"),
            ""
        );
    }

    #[test]
    fn ambiguous_transcripts_fail_open() {
        // Too short for a confident detection, so it is kept rather than
        // silently swallowing a real dictation.
        assert_eq!(filter_transcript("ok"), "ok");
        assert_eq!(filter_transcript(""), "");
        // Romanized Bangla is Latin-script and the detector declines it, which
        // must not cost the user their sentence.
        let romanized = "amar nam likha hai ebong ami bangla boli";
        assert_eq!(filter_transcript(romanized), romanized);
    }

    #[test]
    fn setup_asks_for_auto_detected_smart_transcription() {
        let setup = setup_message(TranscriptionMode::Smart, &[]);
        let transcription = &setup["setup"]["inputAudioTranscription"];
        // Empty list = automatic detection, the documented multilingual mode.
        assert_eq!(transcription["languageCodes"], json!([]));
        assert_eq!(transcription["mode"], "SMART");
        // No terms configured: the field is absent rather than an empty list.
        assert!(transcription.get("customVocabulary").is_none());
        assert_eq!(setup["setup"]["model"], MODEL);
        assert_eq!(
            setup["setup"]["generationConfig"]["responseModalities"],
            json!(["TEXT"])
        );
    }

    #[test]
    fn setup_carries_the_configured_mode_and_vocabulary() {
        let vocabulary = parse_vocabulary("Tauri, Mohibur Rahman");
        let setup = setup_message(TranscriptionMode::Verbatim, &vocabulary);
        let transcription = &setup["setup"]["inputAudioTranscription"];
        assert_eq!(transcription["mode"], "VERBATIM");
        assert_eq!(
            transcription["customVocabulary"],
            json!(["Tauri", "Mohibur Rahman"])
        );
    }

    #[test]
    fn transcription_mode_defaults_to_smart() {
        assert_eq!(TranscriptionMode::parse(None), TranscriptionMode::Smart);
        assert_eq!(TranscriptionMode::parse(Some("")), TranscriptionMode::Smart);
        assert_eq!(
            TranscriptionMode::parse(Some(" verbatim ")),
            TranscriptionMode::Verbatim
        );
        assert_eq!(
            TranscriptionMode::parse(Some("Smart")),
            TranscriptionMode::Smart
        );
        // A typo must not break dictation; it falls back to the default.
        assert_eq!(
            TranscriptionMode::parse(Some("verbose")),
            TranscriptionMode::Smart
        );
    }

    #[test]
    fn vocabulary_is_split_trimmed_and_deduplicated() {
        assert_eq!(
            parse_vocabulary(" Tauri , Mohibur Rahman,,Handy\nTauri\n"),
            vec!["Tauri", "Mohibur Rahman", "Handy"]
        );
        assert!(parse_vocabulary("").is_empty());
        assert!(parse_vocabulary(" , ,\n").is_empty());

        let too_many = (0..MAX_VOCABULARY_TERMS + 5)
            .map(|i| format!("term{}", i))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(parse_vocabulary(&too_many).len(), MAX_VOCABULARY_TERMS);
    }

    /// Full round trip through `GeminiSession` against the live endpoint, using
    /// two sentences separated by a pause so segment accumulation is exercised.
    /// Proves the committed/tentative model and the turn-completion signal on
    /// the real service, not a hand-written reimplementation of it.
    ///
    /// Run with: `cargo test --lib gemini_live -- --ignored --nocapture`
    #[test]
    #[ignore = "requires network and a valid HANDY_GEMINI_KEY in .env"]
    fn end_to_end_transcribes_two_segments() {
        let _ = dotenvy::dotenv();
        let key = match api_key() {
            Some(key) => key,
            None => panic!("set HANDY_GEMINI_KEY in .env to run this test"),
        };

        // Build 16 kHz mono speech with a pause, so the model finalizes twice.
        let wav = std::env::temp_dir().join("handy_e2e_probe.wav");
        let script = "First sentence about apples. \
                      [[slnc 2500]] Second sentence about oranges.";
        let built = std::process::Command::new("say")
            .args([
                "-o",
                wav.to_str().unwrap(),
                "--data-format=LEI16@16000",
                script,
            ])
            .status();
        assert!(matches!(built, Ok(s) if s.success()), "`say` failed");

        let bytes = std::fs::read(&wav).expect("read probe wav");
        let samples: Vec<f32> = bytes[44..]
            .chunks_exact(2)
            .map(|b| i16::from_le_bytes([b[0], b[1]]) as f32 / i16::MAX as f32)
            .collect();

        let session =
            GeminiSession::connect(key, |c, t| println!("  overlay: [{}] ({})", c, t)).unwrap();
        // Pace at roughly real time; the model's pause detection needs the gap
        // to actually land in time, not arrive as one burst.
        for chunk in samples.chunks(CHUNK_SAMPLES) {
            session.feed(chunk);
            std::thread::sleep(Duration::from_millis(90));
        }
        let finalize_started = std::time::Instant::now();
        let text = session.finalize().expect("finalize");
        let finalize_took = finalize_started.elapsed();
        println!(
            "FINAL TRANSCRIPT: {} (finalize took {:?})",
            text, finalize_took
        );

        // Guards the regression this test previously passed straight through:
        // the transcript was correct, but only because `finalize` sat out the
        // full DRAIN_TIMEOUT and salvaged it. Every dictation paid 10s.
        assert!(
            finalize_took < DRAIN_TIMEOUT,
            "finalize fell back to the drain timeout ({:?}) instead of ending on \
             the completion signal",
            finalize_took
        );

        let lower = text.to_lowercase();
        assert!(lower.contains("apples"), "missing first segment: {}", text);
        assert!(
            lower.contains("oranges"),
            "missing second segment: {}",
            text
        );
        // Segments must accumulate, not overwrite one another.
        assert!(
            lower.find("apples") < lower.find("oranges"),
            "segments out of order: {}",
            text
        );
    }

    /// Hits the real endpoint. A rejected key must fail at `connect` with the
    /// server's own reason — the earlier version reported success as soon as the
    /// setup frame was *sent*, so auth failures surfaced as an empty transcript
    /// and a baffling "model is not loaded" batch fallback instead.
    ///
    /// Run with: `cargo test --lib gemini_live -- --ignored --nocapture`
    #[test]
    #[ignore = "requires network access to generativelanguage.googleapis.com"]
    fn invalid_key_fails_loudly_at_connect() {
        let message = match GeminiSession::connect(
            "AIzaSyNotARealKeyAAAAAAAAAAAAAAAAAAAAAA".to_string(),
            |_, _| {},
        ) {
            Ok(_) => panic!("an invalid key must not yield a usable session"),
            Err(e) => e.to_string(),
        };
        println!("connect() error: {}", message);
        assert!(
            message.contains("API key not valid"),
            "expected the server's own reason, got: {}",
            message
        );
    }
}
