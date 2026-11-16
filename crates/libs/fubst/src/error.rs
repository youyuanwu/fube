use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Protocol(&'static str),
    InvalidInput(&'static str),
    ResourceLimit(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(message) => write!(f, "native protocol: {message}"),
            Self::InvalidInput(message) => write!(f, "invalid input: {message}"),
            Self::ResourceLimit(resource) => write!(f, "native resource limit: {resource}"),
        }
    }
}

impl std::error::Error for Error {}

impl ::serde::ser::Error for Error {
    fn custom<T: fmt::Display>(_: T) -> Self {
        Self::InvalidInput("invalid native Serde value")
    }
}

impl ::serde::de::Error for Error {
    fn custom<T: fmt::Display>(_: T) -> Self {
        Self::Protocol("invalid native Serde value")
    }
}

pub type Result<T> = std::result::Result<T, Error>;
