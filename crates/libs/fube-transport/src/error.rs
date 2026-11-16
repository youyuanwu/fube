use std::{fmt, io};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Protocol(&'static str),
    InvalidInput(&'static str),
    Timeout,
    Closed,
    /// A shared codec or transport resource was exhausted.
    ResourceLimit(&'static str),
    Remote {
        code: i64,
        message: String,
    },
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
            Self::ResourceLimit(resource) => Self::ResourceLimit(resource),
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
            Self::ResourceLimit(resource) => write!(f, "native resource limit: {resource}"),
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

impl From<fubst::Error> for Error {
    fn from(value: fubst::Error) -> Self {
        match value {
            fubst::Error::Protocol(message) => Self::Protocol(message),
            fubst::Error::InvalidInput(message) => Self::InvalidInput(message),
            fubst::Error::ResourceLimit(resource) => Self::ResourceLimit(resource),
        }
    }
}

impl serde::ser::Error for Error {
    fn custom<T: fmt::Display>(message: T) -> Self {
        <fubst::Error as serde::ser::Error>::custom(message).into()
    }
}

impl serde::de::Error for Error {
    fn custom<T: fmt::Display>(message: T) -> Self {
        <fubst::Error as serde::de::Error>::custom(message).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_failures_preserve_transport_error_categories_and_budget_cause() {
        use fubst::serde::{from_slice, from_slice_with_budget, to_vec};

        assert!(matches!(
            to_vec(&1u8).map_err(Error::from),
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            from_slice::<String>(&[0xff]).map_err(Error::from),
            Err(Error::Protocol(_))
        ));
        let bytes = to_vec("value").unwrap();
        let result = from_slice_with_budget::<String>(&bytes, &mut |_| {
            Err(fubst::Error::ResourceLimit("caller reservation"))
        });
        assert!(matches!(
            result.map_err(Error::from),
            Err(Error::ResourceLimit("caller reservation"))
        ));
    }

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
