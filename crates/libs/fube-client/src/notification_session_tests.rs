use super::*;

#[test]
fn scoped_errors_preserve_common_sources_cloning_and_domain_diagnostics() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<NotificationError>();
    for error in [
        FurpcClientError::Native(furpc::Error::Timeout("RPC")),
        FurpcClientError::Domain(crate::DomainFailure {
            code: -7,
            message: "native failure".into(),
        }),
        FurpcClientError::Native(furpc::Error::ResourceLimit("shared decoder")),
        FurpcClientError::Decode(fubst::Error::Protocol("invalid notification body")),
    ] {
        let display = error.to_string();
        let scoped = NotificationError::from(error).clone();
        assert_eq!(scoped.to_string(), display);
        let source = std::error::Error::source(&scoped)
            .unwrap()
            .downcast_ref::<FurpcClientError>()
            .unwrap();
        match source {
            FurpcClientError::Domain(failure) => {
                assert_eq!(failure.code, -7);
                assert_eq!(failure.message, "native failure");
            }
            FurpcClientError::Native(furpc::Error::ResourceLimit(resource)) => {
                assert_eq!(*resource, "shared decoder")
            }
            FurpcClientError::Native(furpc::Error::Timeout("RPC")) => {}
            FurpcClientError::Decode(fubst::Error::Protocol("invalid notification body")) => {}
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
