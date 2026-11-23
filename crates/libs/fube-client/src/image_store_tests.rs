use super::*;

#[test]
fn destinations_normalize_native_aliases_without_allowing_system_or_root_paths() {
    assert_eq!(
        validate_destination("  ./My Package\\nested-\u{7a7a}/.  ").unwrap(),
        "My Package/nested-\u{7a7a}"
    );
    for path in [
        "",
        ".",
        "..",
        "/",
        "\\root",
        "C:\\package",
        "a/../b",
        "./ Store/.",
        "WindowsFabricStore",
        "./ /./",
        "package\0tail",
    ] {
        assert!(
            matches!(
                validate_destination(path),
                Err(ImageStoreError::Common(FurpcClientError::Native(
                    furpc::Error::InvalidInput(_)
                )))
            ),
            "{path:?}"
        );
    }
}

#[test]
fn cloned_io_errors_preserve_the_original_source() {
    let error = ImageStoreError::from(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "owned package cannot be read",
    ));
    let cloned = error.clone();
    let original = std::error::Error::source(&error).unwrap();
    let source = std::error::Error::source(&cloned).unwrap();
    assert!(std::ptr::eq(original, source));
    assert_eq!(
        source.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn ingress_failure_survives_secondary_closure_and_ends_the_ack_wait() {
    let state = UploadState {
        connection: Mutex::new(Connection {
            channel: None,
            error: None,
        }),
        failed: Notify::new(),
    };
    state.fail(FurpcClientError::Protocol("invalid native ACK").into());
    state.fail(furpc::Error::Closed.into());
    for error in [
        state.failure().await,
        state.cause(furpc::Error::Closed.into()),
        state.check().unwrap_err(),
    ] {
        assert!(matches!(
            error,
            ImageStoreError::Common(FurpcClientError::Protocol("invalid native ACK"))
        ));
    }
}

#[test]
fn cancelling_an_unpolled_typed_ack_handoff_retires_its_upload_owner() {
    let state = Arc::new(UploadState {
        connection: Mutex::new(Connection {
            channel: None,
            error: None,
        }),
        failed: Notify::new(),
    });
    let (replies, _receiver) = mpsc::channel(MAX_REPLIES);
    let mut ingress = UploadIngress {
        state: Arc::downgrade(&state),
        generation: 1,
        replies,
    };
    let pending = ingress.call(Request::new(Body::empty()));
    assert!(state.check().is_ok());
    drop(pending);
    assert!(matches!(
        state.check(),
        Err(ImageStoreError::Common(FurpcClientError::Native(
            furpc::Error::Cancelled
        )))
    ));
}
