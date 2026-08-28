use std::time::Duration;

/// L2CAP Protocol Service Multiplexer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Psm(pub u16);

impl Psm {
    #[must_use]
    pub fn value(self) -> u16 {
        self.0
    }
}

impl From<u16> for Psm {
    fn from(v: u16) -> Self {
        Self(v)
    }
}

impl std::fmt::Display for Psm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Default for [`L2capConfig::buffer_size`].
pub const DEFAULT_L2CAP_BUFFER_SIZE: usize = 64 * 1024;
/// Default for [`L2capConfig::read_chunk_size`].
pub const DEFAULT_L2CAP_READ_CHUNK_SIZE: usize = 4096;
/// Default for [`L2capConfig::flush_timeout`].
pub const DEFAULT_L2CAP_FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

/// Tuning for a single L2CAP channel.
///
/// Construct with `..Default::default()` so a new field costs you one
/// recompile rather than an edit at every call site.
///
/// # Platform caveats
///
/// **Linux observes none of these.** `bluer::l2cap::Stream` is already an async
/// byte stream, so the backend hands it to the caller directly and the kernel
/// socket buffers provide flow control. There is no in-process bridge to size
/// and nothing queued locally to flush. Apple and Android both marshal bytes
/// between a platform socket and the async world, so they observe all three.
#[derive(Debug, Clone)]
pub struct L2capConfig {
    /// Bytes buffered in each direction between the application and the
    /// platform socket.
    ///
    /// Once buffering fills, the backend stops reading from the platform
    /// socket, which stops L2CAP credits being returned to the peer, which
    /// stops the peer transmitting — the protocol's own flow control does the
    /// work. Larger values trade memory per channel for tolerance of bursty
    /// readers.
    ///
    /// Each direction holds this much twice: once in the stream buffer the
    /// application reads and writes, and once in the queue handing bytes to the
    /// platform socket. Budget roughly `4 * buffer_size` per open channel.
    pub buffer_size: usize,
    /// Largest read issued against the platform socket at once.
    pub read_chunk_size: usize,
    /// How long [`L2capChannel::close`](crate::L2capChannel::close) waits for
    /// queued outbound bytes to reach the peer before tearing the channel down.
    ///
    /// `None` waits indefinitely. Dropping a channel never waits — only an
    /// explicit `close()` can, since `Drop` cannot await.
    pub flush_timeout: Option<Duration>,
}

impl Default for L2capConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_L2CAP_BUFFER_SIZE,
            read_chunk_size: DEFAULT_L2CAP_READ_CHUNK_SIZE,
            flush_timeout: Some(DEFAULT_L2CAP_FLUSH_TIMEOUT),
        }
    }
}

/// Why an L2CAP channel stopped carrying data.
///
/// Retrieved with [`L2capChannel::close_reason`](crate::L2capChannel::close_reason).
/// Anything other than [`Closed`](Self::Closed) also surfaces from `AsyncRead`
/// as an `io::Error` rather than a clean end-of-stream, so a dropped link is not
/// mistaken for the peer politely hanging up.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum L2capCloseReason {
    /// Either end closed the channel deliberately. Reads report EOF.
    Closed,
    /// The underlying ACL connection went away.
    LinkLost,
    /// The platform transport reported an error.
    TransportError(String),
}

impl L2capCloseReason {
    /// Convert to the `io::Error` that `AsyncRead` should report, or `None` for
    /// a clean close (which stays an end-of-stream).
    pub(crate) fn as_io_error(&self) -> Option<std::io::Error> {
        match self {
            Self::Closed => None,
            Self::LinkLost => Some(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "L2CAP link lost",
            )),
            Self::TransportError(msg) => Some(std::io::Error::other(format!(
                "L2CAP transport error: {msg}"
            ))),
        }
    }
}
