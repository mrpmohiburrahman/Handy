//! Baseline benchmark instrumentation.
//!
//! ponytail: measurement scaffolding for
//! `.scratch/rn-macos-rewrite/issues/06-baseline-benchmark.md`, not product
//! code. Global stamps rather than a `t0` threaded through six call
//! signatures: the coordinator serialises dictations one at a time, so a
//! static is honest here and keeps the diff reviewable. If concurrent
//! dictations ever exist, thread the `Instant` through `InputEvent` instead.
//!
//! Every span below is anchored to `mark_hotkey`, which is stamped where a
//! transcribe key press is first visible to this crate. There is an
//! **unmeasurable prefix** in front of that anchor: on macOS the handy-keys
//! manager thread drains its hotkey channel only after
//! `cmd_rx.recv_timeout(10ms)` returns (`shortcut/handy_keys.rs`), so a real
//! key press waits a uniform 0-10 ms (mean ~5 ms) before this crate sees it.
//! That term is additive to every number here and cannot be observed from
//! inside the app: the crate's `HotkeyEvent` carries no timestamp.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static PROCESS_START: OnceLock<Instant> = OnceLock::new();
static HOTKEY: Mutex<Option<Instant>> = Mutex::new(None);
static PCM_LOGGED: AtomicBool = AtomicBool::new(false);
static TEXT_LOGGED: AtomicBool = AtomicBool::new(false);
static VOICE_LOGGED: AtomicBool = AtomicBool::new(false);

/// Stamped as the first statement of `main`, before argument parsing.
pub fn mark_process_start() {
    let _ = PROCESS_START.set(Instant::now());
}

pub fn since_process_start() -> Option<Duration> {
    PROCESS_START.get().map(Instant::elapsed)
}

/// Opens a measurement session. Called on the press edge of a transcribe
/// binding only, before the `debug!` that logs the event — formatting plus a
/// file write is not free, and it would land inside the span being measured.
pub fn mark_hotkey() {
    if let Ok(mut t0) = HOTKEY.lock() {
        *t0 = Some(Instant::now());
    }
    PCM_LOGGED.store(false, Ordering::Relaxed);
    TEXT_LOGGED.store(false, Ordering::Relaxed);
    VOICE_LOGGED.store(false, Ordering::Relaxed);
}

pub fn since_hotkey() -> Option<Duration> {
    HOTKEY.lock().ok()?.map(|t| t.elapsed())
}

/// `Some(elapsed)` exactly once per key press, for the first PCM frame.
pub fn first_pcm() -> Option<Duration> {
    if PCM_LOGGED.swap(true, Ordering::Relaxed) {
        return None;
    }
    since_hotkey()
}

/// `Some(elapsed)` exactly once per key press, for the first frame VAD calls
/// speech. This is the anchor that strips human reaction time out of metric 2:
/// the clock from the hotkey cannot start producing transcript until someone
/// actually speaks, and how fast they react is not a property of the app.
pub fn first_voice() -> Option<Duration> {
    if VOICE_LOGGED.swap(true, Ordering::Relaxed) {
        return None;
    }
    since_hotkey()
}

/// `Some(elapsed)` exactly once per key press, for the first transcript text.
pub fn first_text() -> Option<Duration> {
    if TEXT_LOGGED.swap(true, Ordering::Relaxed) {
        return None;
    }
    since_hotkey()
}

/// Wall-clock milliseconds since the Unix epoch. This is the only clock shared
/// with the overlay webview (`Date.now()`) — an `Instant` cannot cross IPC, so
/// the Rust-emit-to-JS-receive hop is measured by differencing wall stamps.
pub fn wall_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0)
}

pub fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
