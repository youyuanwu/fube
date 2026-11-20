use super::*;

#[test]
fn scoped_errors_preserve_common_sources_cloning_and_domain_diagnostics() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<NotificationError>();
    for error in [
        Error::Timeout,
        Error::Remote {
            code: -7,
            message: "native failure".into(),
        },
        Error::ResourceLimit("shared decoder"),
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "peer reset",
        )),
    ] {
        let display = error.to_string();
        let scoped = NotificationError::from(error).clone();
        assert_eq!(scoped.to_string(), display);
        let source = std::error::Error::source(&scoped)
            .unwrap()
            .downcast_ref::<Error>()
            .unwrap();
        match source {
            Error::Remote { code, message } => {
                assert_eq!(*code, -7);
                assert_eq!(message, "native failure");
            }
            Error::Io(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
            Error::ResourceLimit(resource) => assert_eq!(*resource, "shared decoder"),
            Error::Timeout => {}
            _ => panic!("unexpected common error"),
        }
    }
    for error in [
        NotificationError::Lagged,
        NotificationError::ResourceLimit("notification filters"),
    ] {
        let cloned = error.clone();
        assert!(std::error::Error::source(&cloned).is_none());
        assert!(cloned.to_string().starts_with("notification "));
    }
}
