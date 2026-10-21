use std::{fmt, io};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Protocol(&'static str),
    InvalidInput(&'static str),
    Timeout,
    Closed,
    Remote { code: i64, message: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "native transport I/O: {error}"),
            Self::Protocol(message) => write!(f, "native protocol: {message}"),
            Self::InvalidInput(message) => write!(f, "invalid input: {message}"),
            Self::Timeout => f.write_str("native operation deadline exceeded"),
            Self::Closed => f.write_str("native connection is closed"),
            Self::Remote { code, message } => {
                write!(f, "native error {code} ({:#x}): {message}", *code as u64)
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
