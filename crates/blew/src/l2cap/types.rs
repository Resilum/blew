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
/// Default for [`L2capConfig::linger_timeout`].
pub const DEFAULT_L2CAP_LINGER_TIMEOUT: Duration = Duration::from_secs(1);

/// Floor applied to [`L2capConfig::buffer_size`].
///
/// A zero-capacity buffer is not a tight bound, it is a deadlock:
/// `tokio::io::duplex(0)` never accepts a write, so neither direction could
/// make progress.
pub const MIN_L2CAP_BUFFER_SIZE: usize = 1024;
/// Floor applied to [`L2capConfig::read_chunk_size`]. A zero-length read
/// makes no progress either.
pub const MIN_L2CAP_READ_CHUNK_SIZE: usize = 64;

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
    ///
    /// Raised to [`MIN_L2CAP_BUFFER_SIZE`], and to `read_chunk_size`, if set
    /// lower — a buffer too small to hold one read would stall rather than
    /// throttle.
    pub buffer_size: usize,
    /// Largest read issued against the platform socket at once.
    ///
    /// Raised to [`MIN_L2CAP_READ_CHUNK_SIZE`] if set lower.
    pub read_chunk_size: usize,
    /// How long the backend keeps a closing channel alive to finish writing
    /// whatever is still queued, before tearing it down regardless.
    ///
    /// Modelled on `SO_LINGER`. Closing is asynchronous: both
    /// [`L2capChannel::close`](crate::L2capChannel::close) and dropping the
    /// channel hand it to the backend, which keeps draining until the queue
    /// empties or this deadline passes. Neither blocks the caller, so `Drop`
    /// gets the same delivery guarantee `close()` does — which matters, because
    /// dropping is by far the more common way an `AsyncWrite` goes away.
    ///
    /// `None` drains indefinitely and never forces teardown; use it only when
    /// the peer is trusted to keep accepting data.
    ///
    /// Note this is delivery to the *platform socket*, not acknowledgement by
    /// the peer. Nothing here waits for the far end to read.
    pub linger_timeout: Option<Duration>,
}

impl L2capConfig {
    /// `read_chunk_size` with the floor applied.
    #[must_use]
    pub(crate) fn effective_read_chunk_size(&self) -> usize {
        self.read_chunk_size.max(MIN_L2CAP_READ_CHUNK_SIZE)
    }

    /// `buffer_size` with the floors applied. Never smaller than one read,
    /// so a full chunk always has somewhere to land.
    #[must_use]
    pub(crate) fn effective_buffer_size(&self) -> usize {
        self.buffer_size
            .max(MIN_L2CAP_BUFFER_SIZE)
            .max(self.effective_read_chunk_size())
    }
}

impl Default for L2capConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_L2CAP_BUFFER_SIZE,
            read_chunk_size: DEFAULT_L2CAP_READ_CHUNK_SIZE,
            linger_timeout: Some(DEFAULT_L2CAP_LINGER_TIMEOUT),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_returned_unchanged() {
        let config = L2capConfig::default();
        assert_eq!(config.effective_buffer_size(), DEFAULT_L2CAP_BUFFER_SIZE);
        assert_eq!(
            config.effective_read_chunk_size(),
            DEFAULT_L2CAP_READ_CHUNK_SIZE
        );
    }

    #[test]
    fn zero_sizes_are_raised_to_the_floor() {
        // A zero here would deadlock rather than throttle: duplex(0) never
        // accepts a write and a zero-length read never progresses.
        let config = L2capConfig {
            buffer_size: 0,
            read_chunk_size: 0,
            ..Default::default()
        };
        assert_eq!(config.effective_buffer_size(), MIN_L2CAP_BUFFER_SIZE);
        assert_eq!(
            config.effective_read_chunk_size(),
            MIN_L2CAP_READ_CHUNK_SIZE
        );
    }

    #[test]
    fn buffer_is_never_smaller_than_one_read() {
        let config = L2capConfig {
            buffer_size: 2048,
            read_chunk_size: 16 * 1024,
            ..Default::default()
        };
        assert_eq!(config.effective_buffer_size(), 16 * 1024);
    }
}
