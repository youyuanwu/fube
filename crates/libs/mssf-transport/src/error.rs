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

impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Self::Io(error) => Self::Io(match error.raw_os_error() {
                Some(code) => io::Error::from_raw_os_error(code),
                None => io::Error::new(error.kind(), error.to_string()),
            }),
            Self::Protocol(message) => Self::Protocol(message),
            Self::InvalidInput(message) => Self::InvalidInput(message),
            Self::Timeout => Self::Timeout,
            Self::Closed => Self::Closed,
            Self::Remote { code, message } => Self::Remote {
                code: *code,
                message: message.clone(),
            },
        }
    }
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

impl serde::ser::Error for Error {
    fn custom<T: fmt::Display>(_: T) -> Self {
        Self::InvalidInput("invalid native Serde value")
    }
}

impl serde::de::Error for Error {
    fn custom<T: fmt::Display>(_: T) -> Self {
        Self::Protocol("invalid native Serde value")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_errors_preserve_io_and_native_failure_details() {
        let error = Error::Io(io::Error::from_raw_os_error(13));
        let Error::Io(copy) = error.clone() else {
            panic!("I/O error category changed");
        };
        assert_eq!(copy.raw_os_error(), Some(13));
        let error = Error::Io(io::Error::new(io::ErrorKind::ConnectionReset, "peer reset"));
        let Error::Io(copy) = error.clone() else {
            panic!("I/O error category changed");
        };
        assert_eq!(copy.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(copy.to_string(), "peer reset");
        let error = Error::Remote {
            code: i64::MIN,
            message: "native diagnostic".into(),
        };
        assert!(
            matches!(error.clone(), Error::Remote { code: i64::MIN, message } if message == "native diagnostic")
        );
    }
}
