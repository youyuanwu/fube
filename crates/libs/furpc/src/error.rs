use std::fmt;

/// A native representation, configured-limit, or allocation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidInput(&'static str),
    Protocol(&'static str),
    ResourceLimit(&'static str),
    Internal(&'static str),
    NativeHeader { id: u16, source: fubst::Error },
    UnsupportedSchema { id: u16, reason: &'static str },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(f, "invalid native transport input: {message}"),
            Self::Protocol(message) => write!(f, "native protocol: {message}"),
            Self::ResourceLimit(resource) => write!(f, "native resource limit: {resource}"),
            Self::Internal(message) => write!(f, "native transport invariant: {message}"),
            Self::NativeHeader { id, source } => write!(f, "native header {id:#06x}: {source}"),
            Self::UnsupportedSchema { id, reason } => {
                write!(f, "unsupported native header {id:#06x} schema: {reason}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NativeHeader { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<crate::wire::headers::HeaderError> for Error {
    fn from(error: crate::wire::headers::HeaderError) -> Self {
        use crate::wire::headers::HeaderError;
        match error {
            HeaderError::Protocol(message) => Self::Protocol(message),
            HeaderError::InvalidInput(message) => Self::InvalidInput(message),
            HeaderError::ResourceLimit(resource) => Self::ResourceLimit(resource),
            HeaderError::Decode { id, source } => Self::NativeHeader { id, source },
            HeaderError::UnsupportedSchema { id, reason } => Self::UnsupportedSchema { id, reason },
        }
    }
}

impl From<crate::wire::frame::FrameError> for Error {
    fn from(error: crate::wire::frame::FrameError) -> Self {
        use crate::wire::frame::FrameError;
        match error {
            FrameError::Storage(source) => source,
            FrameError::InvalidState(message) => Self::Internal(message),
            FrameError::SizeLimit { .. } => Self::ResourceLimit("frame bytes"),
            FrameError::HeaderLengthOverflow { .. } | FrameError::FrameLengthOverflow => {
                Self::InvalidInput("native frame length overflow")
            }
            FrameError::UnsupportedProvider { .. } => {
                Self::InvalidInput("unsupported security provider")
            }
            FrameError::InvalidFrameLength { .. } => Self::Protocol("invalid native frame length"),
            FrameError::HeaderChecksumMismatch { .. } => {
                Self::Protocol("native header CRC mismatch")
            }
            FrameError::BodyChecksumMismatch { .. } => Self::Protocol("native body CRC mismatch"),
            FrameError::ProviderMismatch { .. } => {
                Self::Protocol("native security provider mismatch")
            }
            FrameError::Truncated { .. } => Self::Protocol("truncated native frame"),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
