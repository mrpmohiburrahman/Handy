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
//! ponytail: spike. The API key comes from the environment, the model and
//! language are fixed, and a mid-session network failure ends the run with
//! whatever text arrived rather than falling back to the local engine. Promote
//! to a settings field + model-picker entry if this proves out.

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::tungstenite::Message;

const MODEL: &str = "models/gemini-3.5-transcribe-live";
const WS_HOST: &str = "wss://generativelanguage.googleapis.com/ws/\
                       google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";

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
    ["HANDY_GEMINI_KEY", "GEMINI_API_KEY"]
        .iter()
        .find_map(|name| std::env::var(name).ok())
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty())
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

    let setup = json!({
        "setup": {
            "model": MODEL,
            "generationConfig": { "responseModalities": ["TEXT"] },
            // Empty language list = automatic language detection.
            "inputAudioTranscription": { "languageCodes": [] }
        }
    });
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
        println!("FINAL TRANSCRIPT: {} (finalize took {:?})", text, finalize_took);

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
