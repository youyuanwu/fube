//! Shared runtime/data errors, classified from actual causes rather than text.
//!
//! Runtime-accepted user causes use existing finite dormant object/cleanup
//! reservations; clones preserve the real typed source and native code.
//! Plain SDK/native/OS metadata needs no capture reservation. An error carrying
//! an arbitrary user-defined cause is a managed capture: release all such
//! observed errors before shutting down their execution owner, just as for
//! other retained managed resources. Retention does not keep its constructor
//! deadline armed or claim that arbitrary application-owned aliases are joined.
//! Unaccepted direct registration input remains caller-owned. If an accepted
//! callback result cannot reserve capture capacity, it is explicitly rejected
//! ResourceExhausted and destroyed under that callback's active worker guard,
//! not silently handed to an owner/core destructor.
use std::{
    fmt, io,
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidArgument,
    Unsupported,
    ObjectClosed,
    InvalidState,
    InvalidExecutionContext,
    NotPrimary,
    ReconfigurationPending,
    NoWriteQuorum,
    Canceled,
    Timeout,
    ResourceExhausted,
    /// Temporary replication admission pressure; retry only after capacity recovery.
    ReplicationQueueFull,
    /// Permanent rejection by replication operation sizing limits.
    ReplicationOperationTooLarge,
    Transport,
    Native,
    Provider,
}

/// Caller-owned input that was never accepted by the runtime. Recover it and
/// reserve an execution entry before resubmitting. Dropping this value (or its
/// enclosing error) is caller destruction, not runtime-supervised destruction.
pub enum RejectedRegistrationInput {
    Stateless(Box<dyn crate::IStatelessServiceFactory>),
    Stateful(Box<dyn crate::IStatefulServiceFactory>),
}
impl fmt::Debug for RejectedRegistrationInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stateless(_) => "Stateless(factory)",
            Self::Stateful(_) => "Stateful(factory)",
        })
    }
}
#[derive(Debug)]
pub enum Error {
    Io(Arc<io::Error>),
    Protocol(&'static str),
    InvalidInput(&'static str),
    Timeout,
    Closed,
    ResourceLimit(&'static str),
    Remote {
        code: i64,
        message: String,
    },
    Classified {
        kind: ErrorKind,
        message: String,
        cause: Option<Arc<dyn std::error::Error + Send + Sync>>,
    },
    /// Admission/write may have happened. This is not a rollback guarantee.
    Uncertain(Arc<Error>),
    RejectedRegistration(Arc<Mutex<Option<RejectedRegistrationInput>>>),
    /// Runtime-accepted error captures retain a dormant, supervised release
    /// route. Observation/drop on an owner never runs a user cause destructor.
    Supervised(Arc<SupervisedError>),
}
pub struct SupervisedError {
    original: crate::extensions::ManagedObject<Box<Error>>,
    kind: ErrorKind,
    code: Option<i64>,
    native_result: i64,
    uncertain: bool,
    message: String,
    rejected_owner: Option<crate::extensions::ExecutionRuntime>,
}
impl fmt::Debug for SupervisedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupervisedError")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .field("message", &self.message)
            .finish()
    }
}
impl Error {
    fn has_rejected_input(&self) -> bool {
        match self {
            Self::RejectedRegistration(_) => true,
            Self::Uncertain(cause) => cause.has_rejected_input(),
            Self::Classified {
                cause: Some(cause), ..
            } => cause
                .downcast_ref::<Error>()
                .is_some_and(Self::has_rejected_input),
            Self::Supervised(error) => error.rejected_owner.is_some(),
            _ => false,
        }
    }
    fn needs_supervision(&self) -> bool {
        match self {
            Self::Io(error) => error.get_ref().is_some(),
            Self::Classified {
                cause: Some(cause), ..
            } => {
                if let Some(error) = cause.downcast_ref::<Error>() {
                    error.needs_supervision()
                } else if let Some(error) = cause.downcast_ref::<fube_transport::Error>() {
                    matches!(error, fube_transport::Error::Io(error) if error.get_ref().is_some())
                } else {
                    true
                }
            }
            Self::RejectedRegistration(_) => true,
            Self::Uncertain(cause) => cause.needs_supervision(),
            _ => false,
        }
    }
    pub(crate) fn supervised(self, execution: &crate::extensions::ExecutionRuntime) -> Self {
        if !self.needs_supervision() {
            return self;
        }
        let kind = self.kind();
        let code = self.native_code();
        let native_result = self.native_code_value();
        let rejected_owner = self.has_rejected_input().then(|| execution.clone());
        let uncertain = self.is_uncertain();
        let message = self.to_string();
        let ticket = execution.reserve_object(
            &crate::extensions::ObjectQuota::new(1)
                .expect("one error capture is a valid object quota"),
            &crate::extensions::DisposalTracker::default(),
        );
        match ticket {
            Ok(ticket) => Self::Supervised(Arc::new(SupervisedError {
                original: ticket.publish(Box::new(self)),
                kind,
                code,
                native_result,
                uncertain,
                message,
                rejected_owner,
            })),
            Err(error) => {
                // This result was not accepted. Its real captures are destroyed
                // HERE on the active worker, not handed to an unaccounted owner.
                if let Some(owner) = rejected_owner {
                    owner.retire();
                }
                drop(self);
                Self::Classified {
                    kind: error.kind(),
                    message: format!("{error} while accepting callback error: {message}"),
                    cause: code.map(|code| {
                        Arc::new(Self::Remote { code, message })
                            as Arc<dyn std::error::Error + Send + Sync>
                    }),
                }
            }
        }
    }
    pub(crate) fn native_result_code(&self) -> i64 {
        self.retire_rejected_owner();
        self.native_code_value()
    }
    fn retire_rejected_owner(&self) {
        match self {
            Self::Supervised(error) => {
                if let Some(owner) = &error.rejected_owner {
                    owner.retire();
                }
            }
            Self::Uncertain(cause) => cause.retire_rejected_owner(),
            Self::Classified {
                cause: Some(cause), ..
            } => {
                if let Some(cause) = cause.downcast_ref::<Error>() {
                    cause.retire_rejected_owner();
                }
            }
            _ => {}
        }
    }
    fn native_code_value(&self) -> i64 {
        if let Self::Supervised(error) = self {
            return error.native_result;
        }
        if let Some(code) = self.native_code() {
            return code;
        }
        if let Self::Uncertain(cause) = self {
            return cause.native_code_value();
        }
        if let Self::Classified {
            kind: ErrorKind::Provider,
            cause: Some(cause),
            ..
        } = self
        {
            if let Some(cause) = cause.downcast_ref::<Error>() {
                return cause.native_code_value();
            }
        }
        // FabricTypes.idl FABRIC_ERROR_CODE and Common/ErrorCodeValue.h.
        let code: u32 = match self.kind() {
            ErrorKind::InvalidArgument => 0x80070057,
            ErrorKind::Unsupported => 0x80004001,
            ErrorKind::ObjectClosed => 0x80071bfe,
            ErrorKind::InvalidState | ErrorKind::InvalidExecutionContext => 0x80071bfd,
            ErrorKind::NotPrimary => 0x80071bc6,
            ErrorKind::ReconfigurationPending => 0x80071bca,
            ErrorKind::NoWriteQuorum => 0x80071bc5,
            ErrorKind::Canceled => 0x80004004,
            ErrorKind::Timeout => 0x80071bff,
            ErrorKind::ResourceExhausted => 0x8007000e,
            ErrorKind::ReplicationQueueFull => 0x80071bcb,
            ErrorKind::ReplicationOperationTooLarge => 0x80071c2f,
            _ => 0x80004005,
        };
        code as i32 as i64
    }
    pub(crate) fn transport_fault(&self) -> fube_transport::Error {
        match self {
            Self::Io(error) => {
                return fube_transport::Error::Io(match error.raw_os_error() {
                    Some(code) => io::Error::from_raw_os_error(code),
                    None => io::Error::new(error.kind(), error.to_string()),
                })
            }
            Self::Protocol(message) => return fube_transport::Error::Protocol(message),
            Self::InvalidInput(message) => return fube_transport::Error::InvalidInput(message),
            Self::Timeout => return fube_transport::Error::Timeout,
            Self::Closed => return fube_transport::Error::Closed,
            Self::ResourceLimit(resource) => return fube_transport::Error::ResourceLimit(resource),
            Self::Remote { code, message } => {
                return fube_transport::Error::Remote {
                    code: *code,
                    message: message.clone(),
                }
            }
            Self::Uncertain(cause) => return cause.transport_fault(),
            _ => {}
        }
        fube_transport::Error::Remote {
            code: self.native_result_code(),
            message: self.to_string(),
        }
    }
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self::Classified {
            kind,
            message: message.into(),
            cause: None,
        }
    }
    pub fn with_cause(
        kind: ErrorKind,
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Classified {
            kind,
            message: message.into(),
            cause: Some(Arc::new(cause)),
        }
    }
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Io(_) | Self::Protocol(_) => ErrorKind::Transport,
            Self::InvalidInput(_) => ErrorKind::InvalidArgument,
            Self::Timeout => ErrorKind::Timeout,
            Self::Closed => ErrorKind::ObjectClosed,
            Self::ResourceLimit(_) => ErrorKind::ResourceExhausted,
            Self::Remote { code, .. } => match *code {
                -2147017781 => ErrorKind::ReplicationQueueFull,
                -2147017681 => ErrorKind::ReplicationOperationTooLarge,
                _ => ErrorKind::Native,
            },
            Self::Classified { kind, .. } => *kind,
            Self::Uncertain(cause) => cause.kind(),
            Self::RejectedRegistration(_) => ErrorKind::InvalidExecutionContext,
            Self::Supervised(error) => error.kind,
        }
    }
    pub fn native_code(&self) -> Option<i64> {
        let mut next = Some(self as &(dyn std::error::Error + 'static));
        for _ in 0..64 {
            let Some(cause) = next else {
                break;
            };
            if let Some(error) = cause.downcast_ref::<Error>() {
                match error {
                    Self::Remote { code, .. } => return Some(*code),
                    Self::Classified {
                        kind: ErrorKind::ReplicationQueueFull,
                        ..
                    } => return Some(0x80071bcbu32 as i32 as i64),
                    Self::Classified {
                        kind: ErrorKind::ReplicationOperationTooLarge,
                        ..
                    } => return Some(0x80071c2fu32 as i32 as i64),
                    Self::Supervised(error) => return error.code,
                    Self::Uncertain(error) => {
                        next = Some(&**error);
                        continue;
                    }
                    Self::Classified {
                        cause: Some(cause), ..
                    } => {
                        next = Some(&**cause);
                        continue;
                    }
                    _ => {}
                }
            }
            if let Some(fube_transport::Error::Remote { code, .. }) =
                cause.downcast_ref::<fube_transport::Error>()
            {
                return Some(*code);
            }
            next = cause.source();
        }
        None
    }
    pub fn is_uncertain(&self) -> bool {
        matches!(self, Self::Uncertain(_))
            || matches!(self, Self::Supervised(error) if error.uncertain)
    }
    pub fn uncertain(self) -> Self {
        Self::Uncertain(Arc::new(self))
    }
    pub(crate) fn rejected(input: RejectedRegistrationInput) -> Self {
        Self::RejectedRegistration(Arc::new(Mutex::new(Some(input))))
    }
    /// Takes the exact rejected box once, including across error clones.
    pub fn take_rejected_registration_input(&mut self) -> Option<RejectedRegistrationInput> {
        self.take_rejected()
    }
    fn take_rejected(&self) -> Option<RejectedRegistrationInput> {
        match self {
            Self::RejectedRegistration(input) => input.lock().unwrap().take(),
            Self::Supervised(error) => error.original.get().take_rejected(),
            _ => None,
        }
    }
}
impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Self::Io(e) => Self::Io(e.clone()),
            Self::Protocol(s) => Self::Protocol(s),
            Self::InvalidInput(s) => Self::InvalidInput(s),
            Self::Timeout => Self::Timeout,
            Self::Closed => Self::Closed,
            Self::ResourceLimit(s) => Self::ResourceLimit(s),
            Self::Remote { code, message } => Self::Remote {
                code: *code,
                message: message.clone(),
            },
            Self::Classified {
                kind,
                message,
                cause,
            } => Self::Classified {
                kind: *kind,
                message: message.clone(),
                cause: cause.clone(),
            },
            Self::Uncertain(e) => Self::Uncertain(e.clone()),
            Self::RejectedRegistration(input) => Self::RejectedRegistration(input.clone()),
            Self::Supervised(error) => Self::Supervised(error.clone()),
        }
    }
}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(Arc::new(e))
    }
}
impl From<fube_transport::Error> for Error {
    fn from(e: fube_transport::Error) -> Self {
        use fube_transport::Error as T;
        match e {
            T::Io(e) => Self::Io(Arc::new(e)),
            T::Protocol(s) => Self::Protocol(s),
            T::InvalidInput(s) => Self::InvalidInput(s),
            T::Timeout => Self::Timeout,
            T::Closed => Self::Closed,
            T::ResourceLimit(s) => Self::ResourceLimit(s),
            T::Remote { code, message } => Self::Remote { code, message },
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "transport I/O: {e}"),
            Self::Protocol(s) => write!(f, "protocol: {s}"),
            Self::InvalidInput(s) => write!(f, "invalid argument: {s}"),
            Self::Timeout => f.write_str("operation deadline exceeded"),
            Self::Closed => f.write_str("object closed"),
            Self::ResourceLimit(s) => write!(f, "resource exhausted: {s}"),
            Self::Remote { code, message } => write!(f, "native error {code:#x}: {message}"),
            Self::Classified { message, .. } => message.fmt(f),
            Self::Uncertain(cause) => write!(f, "uncertain completion: {cause}"),
            Self::RejectedRegistration(_) => f.write_str("registration requires a reserved runtime execution entry; input remains caller-owned"),
            Self::Supervised(error) => error.message.fmt(f),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(&**e),
            Self::Uncertain(e) => Some(&**e),
            Self::Classified {
                cause: Some(cause), ..
            } => Some(&**cause),
            Self::Supervised(error) => std::error::Error::source(&**error.original.get()),
            _ => None,
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;
#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
