use std::{fmt, sync::Arc};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Shared original error; equality identifies the same retained source.
#[derive(Debug, Clone)]
pub struct ErrorSource(Arc<dyn std::error::Error + Send + Sync>);

impl PartialEq for ErrorSource {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for ErrorSource {}

/// A native construction, transport, execution, or configured-policy failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidInput(&'static str),
    Protocol(&'static str),
    ResourceLimit(&'static str),
    Internal(&'static str),
    NativeHeader {
        id: u16,
        source: fubst::Error,
    },
    UnsupportedSchema {
        id: u16,
        reason: &'static str,
    },
    Unsupported(&'static str),
    Timeout(&'static str),
    Closed,
    Cancelled,
    RemoteFault(Arc<crate::RemoteFault>),
    Transport {
        phase: &'static str,
        source: ErrorSource,
    },
    Handler {
        phase: &'static str,
        source: ErrorSource,
    },
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
            Self::Unsupported(message) => write!(f, "unsupported native transport: {message}"),
            Self::Timeout(phase) => write!(f, "native transport {phase} timed out"),
            Self::Closed => f.write_str("native transport closed"),
            Self::Cancelled => f.write_str("native operation cancelled"),
            Self::RemoteFault(fault) => write!(f, "native remote Fault {}", fault.code()),
            Self::Transport { phase, source } | Self::Handler { phase, source } => {
                write!(f, "native transport {phase}: {}", source.0)
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NativeHeader { source, .. } => Some(source),
            Self::Transport { source, .. } | Self::Handler { source, .. } => {
                Some(source.0.as_ref())
            }
            _ => None,
        }
    }
}

impl Error {
    pub(crate) fn diagnostic(&self) -> (&'static str, &'static str) {
        match self {
            Self::InvalidInput(detail) => ("invalid input", detail),
            Self::Protocol(detail) => ("protocol", detail),
            Self::ResourceLimit(detail) => ("resource limit", detail),
            Self::Internal(detail) => ("internal", detail),
            Self::NativeHeader { .. } => ("native header", "codec"),
            Self::UnsupportedSchema { reason, .. } => ("unsupported schema", reason),
            Self::Unsupported(detail) => ("unsupported", detail),
            Self::Timeout(phase) => ("timeout", phase),
            Self::Closed => ("closed", "retirement"),
            Self::Cancelled => ("cancelled", "operation"),
            Self::RemoteFault(_) => ("remote fault", "RPC completion"),
            Self::Transport { phase, .. } => ("transport", phase),
            Self::Handler { phase, .. } => ("handler", phase),
        }
    }

    pub(crate) fn transport(phase: &'static str, source: impl Into<BoxError>) -> Self {
        Self::sourced(phase, source.into(), false)
    }

    pub(crate) fn handler(phase: &'static str, source: impl Into<BoxError>) -> Self {
        Self::sourced(phase, source.into(), true)
    }

    fn sourced(phase: &'static str, source: BoxError, handler: bool) -> Self {
        match source.downcast::<Self>() {
            Ok(error) => *error,
            Err(error) => {
                let source = ErrorSource(Arc::from(error));
                if handler {
                    Self::Handler { phase, source }
                } else {
                    Self::Transport { phase, source }
                }
            }
        }
    }
}

impl From<Arc<crate::connection::ConnectionError>> for Error {
    fn from(error: Arc<crate::connection::ConnectionError>) -> Self {
        use crate::connection::ConnectionError;
        match error.as_ref() {
            ConnectionError::Native(error) => error.clone(),
            ConnectionError::Frame(error) => error.clone().into(),
            ConnectionError::Control(error) => match error {
                fubst::Error::Protocol(message) => Self::Protocol(message),
                fubst::Error::InvalidInput(message) => Self::InvalidInput(message),
                fubst::Error::ResourceLimit(resource) => Self::ResourceLimit(resource),
            },
            ConnectionError::Timeout(phase) => Self::Timeout(phase),
            ConnectionError::Unsupported(feature) => Self::Unsupported(feature),
            ConnectionError::Cancelled => Self::Cancelled,
            ConnectionError::Closed => Self::Closed,
            ConnectionError::Io { phase, .. } => Self::Transport {
                phase,
                source: ErrorSource(error),
            },
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
