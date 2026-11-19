use std::fmt;

/// A native representation, construction-policy, or resource failure.
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

pub type Result<T> = std::result::Result<T, Error>;
