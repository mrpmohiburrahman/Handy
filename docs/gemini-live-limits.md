# Gemini Live free-tier limits (`gemini-3.5-transcribe-live`)

What the free tier actually allows for push-to-talk dictation, and which limits
are real constraints rather than theoretical ones.

**Verified 2026-08-27** against project `gen-lang-client-0068265768`. Google
changes rate limits without notice and no longer publishes per-model numbers in
its docs — treat the numbers here as a snapshot and re-check the dashboard
(see [Re-verifying](#re-verifying)) before relying on them.

## Reading the AI Studio dashboard

<https://aistudio.google.com/rate-limit> lists one row per model with the
columns `Model | Category | RPM | TPM | RPD`. For `Gemini 3.5 Transcribe Live`
(category `Live API`) the observed row was:

| Column | Observed | Limit | Binding? |
| --- | --- | --- | --- |
| **RPM** — requests per minute | 13 | Unlimited | no |
| **TPM** — input tokens per minute | 1.33K | **20,000** | **yes** |
| **RPD** — requests per day | 39 | Unlimited | no |

Two of the three are uncapped, so everything reduces to **20,000 input tokens
per minute**.

Two properties of these limits are easy to get wrong:

- They apply **per project, not per API key**. Anything else running on the same
  Cloud project shares the same 20K.
- **RPD resets at midnight Pacific**, not on a rolling 24-hour window.

## The 20K TPM limit in seconds of speech

Google documents audio input at **32 tokens per second** (its own worked example
puts a 60-second clip at ~1,920 tokens). So:

```
1 minute of continuous dictation  = 60 × 32          = 1,920 tokens
TPM limit                                            = 20,000 tokens
Headroom                          = 20,000 / 1,920   ≈ 10.4×
```

**A minute of non-stop talking consumes about 10% of the per-minute allowance.**

The cap is unreachable by dictating, because one microphone only produces 60
seconds of audio per 60 seconds of wall clock. Hitting 20K TPM would take
roughly **ten concurrent streams**.

This checks out against the observed dashboard figures: 1.33K tokens ÷ 32 ≈ **41
seconds** of audio across 13 requests — 6.6% of the limit.

> **Conclusion:** quota is a non-issue for single-user dictation. Do not enable
> billing to get headroom. The only reason to pay is the data-handling terms
> below.

## The limits that do constrain us

Not quota — the Live API's session mechanics:

| Constraint | Value | Consequence |
| --- | --- | --- |
| Audio-only session lifetime | 15 minutes | session terminates |
| Connection lifetime | ~10 minutes | connection drops, session dies with it |

A `GoAway` message precedes the connection ending. `contextWindowCompression`
extends sessions indefinitely, and session resumption survives reconnects.

**Neither currently applies to us.** `GeminiSession` in
[`src-tauri/src/gemini_live.rs`](../src-tauri/src/gemini_live.rs) opens a fresh
WebSocket per dictation and closes it on key release, so a session only lives as
long as one press. Reaching either cap would mean holding push-to-talk for ten
unbroken minutes.

The trade-off is a fresh TLS + setup handshake per dictation, measured at
**~0.4–0.6s**. If continuous always-on dictation is ever added, that inverts:
one long-lived session would need compression and resumption, and this section
stops being academic.

## Restricting the transcription language

`inputAudioTranscription.languageCodes` is the only language control this model
exposes, and it is a **bias, not a filter**. Setting `["bn-BD", "en-US"]` does
not stop the model transcribing other languages.

The app now sends `[]` (automatic detection). Google documents this value for
multilingual speech and code-switching, and the hint gave no filtering. See
[Transcription quality settings](#transcription-quality-settings).

Measured 2026-09-28, one Hindi utterance fed as 16 kHz PCM (`say -v Lekha`):

| `languageCodes` | Transcript |
| --- | --- |
| `[]` (auto-detect) | `मेरा नाम लेखा है और मैं हिंदी बोलती हूं।` |
| `["bn-BD", "en-US"]` | `मेरा नाम लेखा है और मैं हिंदी बोलती हूं।` |

Byte-identical: the codes changed nothing, and the third language came back in
its own script rather than being folded into Bangla or English. The docs list
`systemInstruction` as a Live *Agent* feature, not a transcription one, so there
is no prompt-side workaround either.

An allowlist therefore has to be enforced client-side.
[`gemini_live.rs`](../src-tauri/src/gemini_live.rs) does exactly that in
`filter_transcript`, which the Gemini finalize path runs before returning text:
a script check removes each word that has a letter outside Latin or Bengali
(Devanagari, Tamil, CJK, …), then `whatlang` — via
[`audio_toolkit/lang_id.rs`](../src-tauri/src/audio_toolkit/lang_id.rs) — rejects
Latin-script languages it can name confidently, such as French. It fails open,
so only a confident foreign detection drops the text and romanized Bangla
survives the detector's `None`.

The script check removes words, not the transcript. Before 2026-11-14 one
foreign-script word deleted the full dictation, and the result looked the same
as a model that heard nothing.

The gaps are the parts neither check can see: Assamese shares Bengali script,
and a Latin-script third language the detector declines (Indonesian probes as
`None`) passes through.

## Transcription quality settings

Four settings control the quality of the text. None of them needs the paid tier.

| Setting | Value | Where |
| --- | --- | --- |
| Local VAD | bypassed on the Gemini path | `select_vad_policy` in [`actions.rs`](../src-tauri/src/actions.rs) |
| `languageCodes` | `[]` (automatic detection) | `LANGUAGE_CODES` in [`gemini_live.rs`](../src-tauri/src/gemini_live.rs) |
| `mode` | `SMART`, or `VERBATIM` with `HANDY_GEMINI_MODE` | `.env` |
| `customVocabulary` | terms from `HANDY_GEMINI_VOCABULARY` | `.env` |

**Local VAD.** Gemini does voice detection on the server, with padding before
speech and a tolerance for pauses. The local Silero VAD removed audio before the
server received it. Quiet starts and ends of words were cut, and Google states
that fragmented audio decreases transcription quality. The Gemini path now
records with `VadPolicy::Disabled`. The VAD setting has no effect while a Gemini
key is set.

**Mode and vocabulary.** Measured 2026-11-14 with one 10.5 s English clip
(`say -v Samantha`). The spoken text was: "Um, so I want to, uh, deploy the Handy
app with Tauri and Kubernetes on Tuesday. No wait, on Wednesday. Then send the
invoice to Mohibur Rahman."

| Settings | Transcript |
| --- | --- |
| `["bn-BD", "en-US"]`, `VERBATIM` (before) | Um, so I want to uh deploy the handy app with Tori and Kubernetes on Tuesday. No, wait, on Wednesday. Then send the invoice to Mahibur Rahman. |
| `[]`, `VERBATIM` | Um, so I want to uh deploy the handy app with Tori and Kubernetes on Tuesday. No, wait, on Wednesday. Then send the invoice to Mahibur Rahman. |
| `[]`, `SMART` | So I want to deploy the Handy app with Tory on Kubernetes on Wednesday. Then send the invoice to Mahibur Rahman. |
| `[]`, `SMART`, vocabulary (now) | I want to deploy the Handy app with Tauri and Kubernetes on Wednesday. Then send the invoice to Mohibur Rahman. |

The vocabulary was `Tauri, Mohibur Rahman, Handy, Kubernetes`. The time to the
final text was the same for all four rows (11.0 s to 11.2 s).

The language hint changed nothing on clean synthetic speech in English or in
Bangla. Its effect on real mixed speech is not measured.

## Data handling: the real free-vs-paid difference

Google's API terms treat the two tiers very differently, and for dictation this
matters more than quota.

| | Unpaid (free quota) | Paid |
| --- | --- | --- |
| Content used to improve Google products | **yes** | no |
| Human reviewers may read/annotate I/O | **yes** (disconnected from account, key and project first) | no |
| Logging | — | brief, for abuse detection only |

The terms state directly that sensitive, confidential or personal information
should not be submitted to the Unpaid Services.

That is a sharper constraint for dictation than for ordinary API use: the input
is *whatever the user happens to say* — credentials read aloud, medical details,
client information, private messages — with no opportunity to screen it the way
a deliberately composed prompt could be.

**Exception:** in the EEA, Switzerland and the UK, the paid-tier data terms apply
to all services, including free quota.

## Re-verifying

Confirm the key works and is not rate-limited (`200` good, `429` quota,
`400` invalid key):

```bash
KEY=$(grep '^HANDY_GEMINI_KEY=' .env | cut -d= -f2- | tr -d ' "')
curl -s -o /dev/null -w '%{http_code}\n' \
  "https://generativelanguage.googleapis.com/v1beta/models?key=$KEY"
```

Exercise the Live endpoint end to end, including segment accumulation:

```bash
cd src-tauri && cargo test --lib gemini_live -- --ignored --nocapture
```

Current usage is only visible on the dashboard — the Live API does not return
`usageMetadata` frames for this model, so token spend cannot be measured from
the session itself. Confirmed by inspecting every frame of a captured session:
the only keys returned are `setupComplete` and `serverContent`
(`interimInputTranscription`, `inputTranscription`, `generationComplete`).

## Sources

- Rate limits, tiers, RPM/TPM/RPD definitions — <https://ai.google.dev/gemini-api/docs/rate-limits>
- Audio at 32 tokens/second — <https://ai.google.dev/gemini-api/docs/tokens>
- Session and connection lifetimes, compression, resumption — <https://ai.google.dev/gemini-api/docs/live-api/session-management>
- Live transcription model and protocol — <https://ai.google.dev/gemini-api/docs/live-api/live-transcribe>
- Unpaid vs Paid data handling — <https://ai.google.dev/gemini-api/terms>
