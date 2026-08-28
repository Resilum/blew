//! Shared L2CAP transport construction for Apple platforms.
//!
//! CoreBluetooth vends `NSInputStream` / `NSOutputStream` objects for each
//! `CBL2CAPChannel`. Those streams are run-loop driven and thread-affine enough
//! that the backend needs an explicit owner for all stream lifecycle work, so a
//! single reactor thread owns every channel's streams and Tokio only bridges
//! bytes to and from it.
//!
//! # Flow control
//!
//! Every queue here is bounded, and that is load-bearing rather than tidiness.
//! L2CAP CoC is credit-based: the receiver grants credits and each K-frame
//! spends one. If the reactor stops reading a socket, credits stop being
//! returned and the *peer* stops transmitting. So the correct response to a
//! slow application is simply to stop reading — the protocol throttles the
//! sender for us. An unbounded local queue defeats exactly that, converting the
//! peer's flow control into unbounded memory growth on this side.
//!
//! Outbound works the same way in reverse: the reactor only pulls from a
//! channel's outbound queue when the stream reports space, so a slow peer backs
//! pressure up through the bounded queue into the caller's `write()`.

#![allow(clippy::cast_possible_truncation)]

use std::collections::{HashMap, VecDeque};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, mpsc};
use std::time::{Duration, Instant};

use objc2::rc::autoreleasepool;
use objc2_core_bluetooth::CBL2CAPChannel;
use objc2_foundation::{
    NSDate, NSDefaultRunLoopMode, NSInputStream, NSOutputStream, NSRunLoop, NSStreamStatus,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Handle;
use tokio::sync::mpsc as tokio_mpsc;
use tracing::{debug, trace, warn};

use crate::l2cap::types::{L2capCloseReason, L2capConfig};
use crate::l2cap::{CloseReasonSlot, DuplexBridge, L2capChannel};
use crate::platform::apple::helpers::{ObjcSend, retain_send};

const RUN_LOOP_POLL_SECS: f64 = 0.05;
/// Smallest number of queued chunks per direction, so a tiny `buffer_size` can
/// never produce a zero-capacity channel (which would deadlock).
const MIN_QUEUE_CHUNKS: usize = 2;

type InputStream = Arc<ObjcSend<NSInputStream>>;
type OutputStream = Arc<ObjcSend<NSOutputStream>>;

static REACTOR_TX: OnceLock<mpsc::Sender<ReactorCmd>> = OnceLock::new();
static NEXT_CHANNEL_ID: AtomicU64 = AtomicU64::new(1);

/// Queue depth in chunks for `buffer_size` bytes of `read_chunk_size` chunks.
fn queue_capacity(config: &L2capConfig) -> usize {
    config
        .buffer_size
        .div_ceil(config.read_chunk_size.max(1))
        .max(MIN_QUEUE_CHUNKS)
}

struct RegisterChannel {
    id: u64,
    channel_ref: Arc<ObjcSend<CBL2CAPChannel>>,
    input: InputStream,
    output: OutputStream,
    inbound_tx: tokio_mpsc::Sender<Vec<u8>>,
    outbound_rx: tokio_mpsc::Receiver<Vec<u8>>,
    read_chunk_size: usize,
    close_reason: CloseReasonSlot,
    linger_timeout: Option<Duration>,
}

enum ReactorCmd {
    Register(Box<RegisterChannel>),
    Close { id: u64 },
}

struct ReactorChannel {
    channel_ref: Arc<ObjcSend<CBL2CAPChannel>>,
    input: InputStream,
    output: OutputStream,
    inbound_tx: tokio_mpsc::Sender<Vec<u8>>,
    outbound_rx: tokio_mpsc::Receiver<Vec<u8>>,
    /// Bytes accepted from the app but not yet taken by the output stream.
    pending: VecDeque<Vec<u8>>,
    /// How much of `pending.front()` the stream has already taken.
    pending_offset: usize,
    /// The app closed its write side; once `pending` empties we are drained.
    outbound_done: bool,
    /// Set when the app closed or dropped the channel. The reactor keeps
    /// draining `pending` after this, which is what gives `Drop` the same
    /// delivery behaviour as `close()`.
    closing_since: Option<Instant>,
    linger_timeout: Option<Duration>,
    read_chunk_size: usize,
    close_reason: CloseReasonSlot,
}

/// Whether a closing channel is finished: everything queued has been written,
/// or the linger deadline has passed.
///
/// Split out from `ReactorChannel` so the policy is testable without a radio.
fn linger_finished(
    closing_since: Option<Instant>,
    outbound_done: bool,
    pending_empty: bool,
    linger_timeout: Option<Duration>,
    now: Instant,
) -> bool {
    let Some(since) = closing_since else {
        return false;
    };
    if outbound_done && pending_empty {
        return true;
    }
    match linger_timeout {
        Some(limit) => now.duration_since(since) >= limit,
        None => false,
    }
}

struct L2capReactor {
    cmd_rx: mpsc::Receiver<ReactorCmd>,
    channels: HashMap<u64, ReactorChannel>,
}

fn default_run_loop_mode() -> &'static objc2_foundation::NSRunLoopMode {
    unsafe { NSDefaultRunLoopMode }
}

fn reactor_tx() -> &'static mpsc::Sender<ReactorCmd> {
    REACTOR_TX.get_or_init(|| {
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("blew-l2cap-reactor".to_string())
            .spawn(move || L2capReactor::new(rx).run())
            .expect("spawn blew-l2cap-reactor");
        tx
    })
}

fn next_channel_id() -> u64 {
    NEXT_CHANNEL_ID.fetch_add(1, Ordering::Relaxed)
}

/// Outcome of servicing one direction of one channel for one tick.
enum Pump {
    /// Made whatever progress was available; keep the channel.
    Continue,
    /// The channel is finished and should be torn down.
    Done(L2capCloseReason),
}

impl ReactorChannel {
    /// Move bytes app -> peer, never writing without reported space.
    fn pump_output(&mut self) -> Pump {
        loop {
            if self.pending.is_empty() {
                match self.outbound_rx.try_recv() {
                    Ok(chunk) => {
                        if !chunk.is_empty() {
                            self.pending.push_back(chunk);
                        }
                    }
                    Err(tokio_mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio_mpsc::error::TryRecvError::Disconnected) => {
                        self.outbound_done = true;
                        break;
                    }
                }
            }
            let Some(front) = self.pending.front() else {
                break;
            };
            // Asking an NSOutputStream to write with no space either blocks the
            // reactor thread -- stalling every other channel -- or reports a
            // failure we would misread as a dead channel. Wait for the next tick
            // instead; the peer's credits will free space.
            if !self.output.hasSpaceAvailable() {
                break;
            }
            let remaining = &front[self.pending_offset..];
            let n = unsafe {
                self.output.write_maxLength(
                    NonNull::new(remaining.as_ptr().cast_mut()).expect("data ptr"),
                    remaining.len(),
                )
            };
            if n < 0 {
                return Pump::Done(stream_error(self.output.streamError().as_deref()));
            }
            if n == 0 {
                break;
            }
            self.pending_offset += n.cast_unsigned();
            if self.pending_offset >= front.len() {
                self.pending.pop_front();
                self.pending_offset = 0;
            }
        }

        Pump::Continue
    }

    /// Move bytes peer -> app, never reading what we cannot deliver.
    fn pump_input(&mut self, id: u64) -> Pump {
        while self.input.hasBytesAvailable() {
            // Reserve delivery capacity *before* reading. Bytes read out of the
            // socket are bytes whose L2CAP credit has been returned to the peer,
            // so reading without somewhere to put them is what turns the peer's
            // flow control into unbounded local buffering.
            let permit = match self.inbound_tx.try_reserve() {
                Ok(permit) => permit,
                Err(tokio_mpsc::error::TrySendError::Full(())) => {
                    trace!(id, "apple L2CAP inbound queue full; pausing reads");
                    break;
                }
                Err(tokio_mpsc::error::TrySendError::Closed(())) => {
                    return Pump::Done(L2capCloseReason::Closed);
                }
            };

            let mut buf = vec![0_u8; self.read_chunk_size];
            let n = unsafe {
                self.input
                    .read_maxLength(NonNull::new(buf.as_mut_ptr()).expect("buf ptr"), buf.len())
            };
            if n < 0 {
                return Pump::Done(stream_error(self.input.streamError().as_deref()));
            }
            if n == 0 {
                trace!(id, "apple L2CAP input reached end of stream");
                return Pump::Done(L2capCloseReason::Closed);
            }
            buf.truncate(n.cast_unsigned());
            permit.send(buf);
        }

        if self.input.streamStatus() == NSStreamStatus::Error {
            return Pump::Done(stream_error(self.input.streamError().as_deref()));
        }
        Pump::Continue
    }
}

fn stream_error(err: Option<&objc2_foundation::NSError>) -> L2capCloseReason {
    match err {
        Some(e) => L2capCloseReason::TransportError(e.localizedDescription().to_string()),
        None => L2capCloseReason::LinkLost,
    }
}

fn close_channel(
    run_loop: &NSRunLoop,
    id: u64,
    channel: ReactorChannel,
    reason: &L2capCloseReason,
) {
    trace!(id, ?reason, "apple L2CAP reactor closing channel");
    channel.close_reason.set(reason.clone());
    unsafe {
        channel
            .input
            .removeFromRunLoop_forMode(run_loop, default_run_loop_mode());
        channel
            .output
            .removeFromRunLoop_forMode(run_loop, default_run_loop_mode());
    }
    channel.input.close();
    channel.output.close();
    drop(channel.channel_ref);
}

impl L2capReactor {
    fn new(cmd_rx: mpsc::Receiver<ReactorCmd>) -> Self {
        Self {
            cmd_rx,
            channels: HashMap::new(),
        }
    }

    fn run(mut self) {
        autoreleasepool(|_| {
            trace!("apple L2CAP reactor started");
            let run_loop = NSRunLoop::currentRunLoop();
            loop {
                autoreleasepool(|_| {
                    self.drain_commands(&run_loop);
                    self.pump_channels(&run_loop);
                    Self::poll_run_loop(&run_loop);
                });
            }
        });
    }

    fn drain_commands(&mut self, run_loop: &NSRunLoop) {
        while let Ok(cmd) = self.cmd_rx.try_recv() {
            match cmd {
                ReactorCmd::Register(channel) => self.register_channel(run_loop, *channel),
                // Closing is a request to stop *after* draining, not an
                // immediate teardown -- see `pump_channels`.
                ReactorCmd::Close { id } => {
                    if let Some(channel) = self.channels.get_mut(&id)
                        && channel.closing_since.is_none()
                    {
                        channel.closing_since = Some(Instant::now());
                        trace!(id, "apple L2CAP channel closing; draining outbound queue");
                    }
                }
            }
        }
    }

    fn register_channel(&mut self, run_loop: &NSRunLoop, channel: RegisterChannel) {
        let RegisterChannel {
            id,
            channel_ref,
            input,
            output,
            inbound_tx,
            outbound_rx,
            read_chunk_size,
            close_reason,
            linger_timeout,
        } = channel;

        trace!(id, "apple L2CAP reactor registering channel");
        unsafe {
            input.scheduleInRunLoop_forMode(run_loop, default_run_loop_mode());
            output.scheduleInRunLoop_forMode(run_loop, default_run_loop_mode());
        }
        input.open();
        output.open();
        self.channels.insert(
            id,
            ReactorChannel {
                channel_ref,
                input,
                output,
                inbound_tx,
                outbound_rx,
                pending: VecDeque::new(),
                pending_offset: 0,
                outbound_done: false,
                closing_since: None,
                linger_timeout,
                read_chunk_size,
                close_reason,
            },
        );
    }

    fn pump_channels(&mut self, run_loop: &NSRunLoop) {
        let now = Instant::now();
        let mut finished = Vec::new();
        for (&id, channel) in &mut self.channels {
            if let Pump::Done(reason) = channel.pump_output() {
                finished.push((id, reason));
                continue;
            }
            // A closing channel has no reader left, and its inbound queue has
            // usually already been dropped -- which would otherwise look like a
            // reason to tear down immediately and defeat the linger.
            if channel.closing_since.is_none()
                && let Pump::Done(reason) = channel.pump_input(id)
            {
                finished.push((id, reason));
                continue;
            }
            if linger_finished(
                channel.closing_since,
                channel.outbound_done,
                channel.pending.is_empty(),
                channel.linger_timeout,
                now,
            ) {
                finished.push((id, L2capCloseReason::Closed));
            }
        }
        for (id, reason) in finished {
            self.remove_channel(run_loop, id, &reason);
        }
    }

    fn remove_channel(&mut self, run_loop: &NSRunLoop, id: u64, reason: &L2capCloseReason) {
        if let Some(channel) = self.channels.remove(&id) {
            close_channel(run_loop, id, channel, reason);
        }
    }

    fn poll_run_loop(run_loop: &NSRunLoop) {
        let deadline = NSDate::dateWithTimeIntervalSinceNow(RUN_LOOP_POLL_SECS);
        run_loop.acceptInputForMode_beforeDate(default_run_loop_mode(), &deadline);
    }
}

/// Wrap an Apple `CBL2CAPChannel` into a `L2capChannel` (AsyncRead + AsyncWrite).
///
/// # Ownership / lifetime
/// The `CBL2CAPChannel` is retained for the lifetime of the bridge: Apple's
/// docs say "you should retain the channel yourself" -- without this, CB may
/// release the channel after the delegate callback returns, closing the streams.
///
/// # Errors
/// Returns an error if CoreBluetooth vended a channel without both streams,
/// rather than handing back a channel that would read EOF immediately.
pub(crate) fn bridge_l2cap_channel(
    channel: &CBL2CAPChannel,
    runtime: &Handle,
    config: &L2capConfig,
) -> Result<L2capChannel, L2capCloseReason> {
    let channel = Arc::new(unsafe { retain_send(channel) });

    let Some(inp) = (unsafe { channel.inputStream() }) else {
        warn!("apple L2CAP channel missing input stream");
        return Err(L2capCloseReason::TransportError(
            "L2CAP channel has no input stream".into(),
        ));
    };
    let Some(out) = (unsafe { channel.outputStream() }) else {
        warn!("apple L2CAP channel missing output stream");
        return Err(L2capCloseReason::TransportError(
            "L2CAP channel has no output stream".into(),
        ));
    };

    let input = Arc::new(unsafe { retain_send(&*inp) });
    let output = Arc::new(unsafe { retain_send(&*out) });

    let capacity = queue_capacity(config);
    let (inbound_tx, mut inbound_rx) = tokio_mpsc::channel::<Vec<u8>>(capacity);
    let (outbound_tx, outbound_rx) = tokio_mpsc::channel::<Vec<u8>>(capacity);
    let close_reason = CloseReasonSlot::default();

    let reactor = reactor_tx().clone();
    let channel_id = next_channel_id();

    reactor
        .send(ReactorCmd::Register(Box::new(RegisterChannel {
            id: channel_id,
            channel_ref: Arc::clone(&channel),
            input,
            output,
            inbound_tx,
            outbound_rx,
            read_chunk_size: config.read_chunk_size.max(1),
            close_reason: close_reason.clone(),
            linger_timeout: config.linger_timeout,
        })))
        .expect("apple L2CAP reactor available");

    let (app_side, io_side) = tokio::io::duplex(config.buffer_size);
    let (mut io_reader, mut io_writer) = tokio::io::split(io_side);

    runtime.spawn(async move {
        trace!(id = channel_id, "apple L2CAP inbound async bridge started");
        while let Some(bytes) = inbound_rx.recv().await {
            if io_writer.write_all(&bytes).await.is_err() {
                trace!(
                    id = channel_id,
                    "apple L2CAP inbound async bridge stopping: app-side writer closed"
                );
                break;
            }
        }
        trace!(id = channel_id, "apple L2CAP inbound async bridge exited");
    });

    let read_chunk = config.read_chunk_size.max(1);
    runtime.spawn(async move {
        trace!(id = channel_id, "apple L2CAP outbound async bridge started");
        let mut buf = vec![0_u8; read_chunk];
        loop {
            let n = match io_reader.read(&mut buf).await {
                Ok(0) => {
                    trace!(
                        id = channel_id,
                        "apple L2CAP outbound async bridge stopping: app-side reader EOF"
                    );
                    break;
                }
                Err(e) => {
                    debug!(
                        id = channel_id,
                        "apple L2CAP outbound async bridge stopping: app-side read failed: {e}"
                    );
                    break;
                }
                Ok(n) => n,
            };
            // Awaiting capacity here is what makes the caller's `write()` block
            // when the peer stops accepting data.
            if outbound_tx.send(buf[..n].to_vec()).await.is_err() {
                trace!(
                    id = channel_id,
                    "apple L2CAP outbound async bridge stopping: reactor dropped channel"
                );
                break;
            }
        }
        // Dropping `outbound_tx` is the reactor's signal that no more bytes are
        // coming, which lets it complete the flush handshake for `close()`.
        drop(outbound_tx);
        trace!(id = channel_id, "apple L2CAP outbound async bridge exited");
    });

    Ok(L2capChannel::from_bridge(DuplexBridge {
        inner: app_side,
        close_hook: Some(Box::new(move || {
            let _ = reactor.send(ReactorCmd::Close { id: channel_id });
        })),
        close_reason,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_capacity_divides_buffer_into_chunks() {
        let config = L2capConfig {
            buffer_size: 64 * 1024,
            read_chunk_size: 4096,
            ..Default::default()
        };
        assert_eq!(queue_capacity(&config), 16);
    }

    #[test]
    fn queue_capacity_rounds_up_a_partial_chunk() {
        let config = L2capConfig {
            buffer_size: 5000,
            read_chunk_size: 4096,
            ..Default::default()
        };
        assert_eq!(queue_capacity(&config), 2);
    }

    #[test]
    fn not_closing_never_finishes() {
        let now = Instant::now();
        assert!(!linger_finished(None, true, true, None, now));
    }

    #[test]
    fn closing_finishes_once_the_queue_is_drained() {
        let now = Instant::now();
        assert!(linger_finished(
            Some(now),
            true,
            true,
            Some(Duration::from_secs(60)),
            now
        ));
    }

    #[test]
    fn closing_waits_while_bytes_remain() {
        let now = Instant::now();
        // Outbound finished but bytes still pending...
        assert!(!linger_finished(
            Some(now),
            true,
            false,
            Some(Duration::from_secs(60)),
            now
        ));
        // ...and the app may still be writing.
        assert!(!linger_finished(
            Some(now),
            false,
            true,
            Some(Duration::from_secs(60)),
            now
        ));
    }

    #[test]
    fn closing_gives_up_once_the_deadline_passes() {
        let start = Instant::now();
        let later = start + Duration::from_secs(2);
        assert!(linger_finished(
            Some(start),
            false,
            false,
            Some(Duration::from_secs(1)),
            later
        ));
    }

    #[test]
    fn a_none_deadline_drains_indefinitely() {
        let start = Instant::now();
        let much_later = start + Duration::from_secs(86_400);
        assert!(!linger_finished(
            Some(start),
            false,
            false,
            None,
            much_later
        ));
    }

    #[test]
    fn queue_capacity_never_reaches_zero() {
        // A zero-capacity tokio channel would deadlock rather than throttle.
        let config = L2capConfig {
            buffer_size: 0,
            read_chunk_size: 0,
            ..Default::default()
        };
        assert!(queue_capacity(&config) >= MIN_QUEUE_CHUNKS);
    }
}
