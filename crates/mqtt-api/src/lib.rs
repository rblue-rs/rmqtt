use std::time::Duration;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Error {
    InvalidArgument = 1,
    NotInitialized = 2,
    Closed = 3,
    Busy = 4,
    QueueFull = 5,
    Unsupported = 6,
    Network = 7,
    Refused = 8,
    BufferTooSmall = 9,
    Empty = 10,
    Internal = 11,
    Overflow = 12,
    Timeout = 13,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone)]
pub struct Options {
    pub server: String,
    pub client_id: String,
    pub port: u16,
    pub protocol: u8,
    pub keep_alive: Duration,
}
impl Options {
    pub fn new(server: impl Into<String>, client_id: impl Into<String>, port: u16) -> Self {
        Self {
            server: server.into(),
            client_id: client_id.into(),
            port,
            protocol: 4,
            keep_alive: Duration::ZERO,
        }
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.server.is_empty()
            || self.client_id.is_empty()
            || self.client_id.len() > 65535
            || self.server.contains('\0')
            || self.client_id.contains('\0')
            || self.port == 0
            || self.keep_alive.as_secs() > 65535
            || self.keep_alive.subsec_nanos() != 0
        {
            return Err(Error::InvalidArgument);
        }
        if self.protocol != 4 {
            return Err(Error::Unsupported);
        }
        Ok(())
    }
}
