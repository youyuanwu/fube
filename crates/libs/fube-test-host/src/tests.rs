use super::*;
use fube_hosting::wire::NativeApplicationHostType;
use furpc::{ActorId, HeaderId, HeaderRecord, MessageId};

fn message(headers: &[HeaderRecord], body: Vec<u8>) -> Message {
    let mut builder = Message::builder().body(Body::from_bytes(body).unwrap());
    for header in headers {
        builder = builder.append_header(header.clone());
    }
    builder.build().unwrap()
}

fn dispatch(
    state: &mut Registration,
    message: Message,
    bundle: &deployment::Deployment,
) -> Result<()> {
    let (response, _receive) = tokio::sync::oneshot::channel();
    state.dispatch(
        ipc::Incoming {
            message,
            response: Some(response),
        },
        "owned",
        42,
        bundle,
    )
}

#[derive(serde::Serialize)]
struct Flag {
    value: bool,
}

// These are state/decoder regression tests, not native compatibility proof.
#[test]
fn reject_unowned_identity_unsupported_actions_and_bad_state() {
    let bundle =
        deployment::Deployment::create(&std::env::temp_dir(), &std::env::current_exe().unwrap())
            .unwrap();
    let mut state = Registration::default();
    let mut headers = vec![
        header::actor(ActorId::new(7).unwrap()).unwrap(),
        header::action("FinishRegisterApplicationHostRequest").unwrap(),
        HeaderRecord::native(
            header::MESSAGE_ID,
            &MessageId {
                guid: [1; 16],
                index: 1,
            },
        )
        .unwrap(),
        HeaderRecord::native(header::EXPECTS_REPLY, &Flag { value: true }).unwrap(),
    ];
    let body = to_vec(&wire::HostId {
        host: "owned".into(),
    })
    .unwrap();
    assert!(
        dispatch(&mut state, message(&headers, body.clone()), &bundle)
            .unwrap_err()
            .0
            .contains("identity")
    );
    headers.push(
        HeaderRecord::native(
            HeaderId::new(wire::IPC_HEADER).unwrap(),
            &wire::IpcIdentity {
                host: "owned".into(),
                pid: 43,
            },
        )
        .unwrap(),
    );
    assert!(
        dispatch(&mut state, message(&headers, body.clone()), &bundle)
            .unwrap_err()
            .0
            .contains("unowned")
    );
    *headers.last_mut().unwrap() = HeaderRecord::native(
        HeaderId::new(wire::IPC_HEADER).unwrap(),
        &wire::IpcIdentity {
            host: "owned".into(),
            pid: 42,
        },
    )
    .unwrap();
    assert!(
        dispatch(&mut state, message(&headers, body.clone()), &bundle)
            .unwrap_err()
            .0
            .contains("finish")
    );
    headers[1] = header::action("UnsupportedAction").unwrap();
    assert!(dispatch(&mut state, message(&headers, body), &bundle)
        .unwrap_err()
        .0
        .contains("unsupported"));
    std::fs::remove_dir_all(bundle.root).unwrap();
}

#[test]
fn truncated_start_requests_never_succeed() {
    let bytes = to_vec(&wire::HostStart {
        host: "owned".into(),
        kind: NativeApplicationHostType::ActivatedSingleCodePackage,
        pid: 42,
        timeout: 1,
        container: false,
        activator: false,
    })
    .unwrap();
    for end in 0..bytes.len() {
        assert!(from_slice::<wire::HostStart>(&bytes[..end]).is_err());
    }
}

#[test]
fn observers_preserve_order_failure_and_deadline() {
    let mut observations = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut observer = |event: LifecycleEvent<'_>| {
        observations.push((event.stage, event.endpoint.to_owned(), event.pid));
        Ok(())
    };
    observe(
        &mut observer,
        LifecycleStage::Opened,
        "opaque",
        42,
        deadline,
    )
    .unwrap();
    observe(
        &mut observer,
        LifecycleStage::Closed,
        "opaque",
        42,
        deadline,
    )
    .unwrap();
    assert_eq!(
        observations,
        [
            (LifecycleStage::Opened, "opaque".into(), 42),
            (LifecycleStage::Closed, "opaque".into(), 42)
        ]
    );
    let mut called = false;
    assert!(observe(
        &mut |_| {
            called = true;
            Ok(())
        },
        LifecycleStage::Opened,
        "",
        42,
        Instant::now() - Duration::from_secs(1)
    )
    .is_err());
    assert!(!called);
    let error = observe(
        &mut |_| Err(HostError("caller probe failed".into())),
        LifecycleStage::Opened,
        "",
        42,
        deadline,
    )
    .unwrap_err();
    assert_eq!(error.0, "caller probe failed");
}

#[test]
fn cleanup_never_masks_native_or_caller_failure() {
    let cleanup = json!({"forced":true,"reaped":true});
    assert_eq!(
        complete(
            Err(HostError("semantic Open failed".into())),
            Ok(cleanup.clone())
        )
        .unwrap_err()
        .0,
        "semantic Open failed"
    );
    let error = complete::<serde_json::Value>(
        Err(HostError("caller probe failed".into())),
        Err(HostError("owned reap failed".into())),
    )
    .unwrap_err();
    assert!(error.0.contains("caller probe failed") && error.0.contains("owned reap failed"));
    assert_eq!(complete(Ok(()), Ok(cleanup.clone())).unwrap(), cleanup);
}

#[test]
fn manifest_validation_precedes_native_preflight_or_process_creation() {
    let root = std::env::temp_dir().join(format!("no-launch-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("ApplicationManifest.xml"), "<NotSupported/>").unwrap();
    let error = run_manifest(
        ManifestOptions {
            native_directory: "/missing-native".into(),
            dependency_directory: "/missing-deps".into(),
            package_directory: root.clone(),
            application_name: "fabric:/Example".into(),
            parameters: Vec::new(),
            run_parent: root.join("must-not-be-created"),
        },
        Scenario::Close,
        |_| panic!("observer before validated launch"),
    )
    .unwrap_err();
    assert!(error.0.contains("expected Fabric ApplicationManifest"));
    assert!(!root.join("must-not-be-created").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn registration_admits_only_manifest_code_and_type_context() {
    let mut bundle =
        deployment::Deployment::create(&std::env::temp_dir(), &std::env::current_exe().unwrap())
            .unwrap();
    bundle.identity = wire::code_context("CustomApp", "fabric:/Custom", "Package", "Executable");
    bundle.service_type = "CustomType".into();
    let mut state = Registration {
        started: true,
        finished: true,
        ..Default::default()
    };
    let mut context = wire::RuntimeContext {
        id: "actual-runtime".into(),
        host: wire::HostContext {
            host: "owned".into(),
            kind: NativeApplicationHostType::ActivatedSingleCodePackage,
            pid: 42,
            container: false,
            activator: false,
        },
        code: wire::expected_code(),
    };
    let request = |action: &str, body| {
        message(
            &[
                header::actor(ActorId::new(9).unwrap()).unwrap(),
                header::action(action).unwrap(),
                HeaderRecord::native(
                    header::MESSAGE_ID,
                    &MessageId {
                        guid: [1; 16],
                        index: 1,
                    },
                )
                .unwrap(),
                HeaderRecord::native(header::EXPECTS_REPLY, &Flag { value: true }).unwrap(),
                HeaderRecord::native(
                    HeaderId::new(wire::IPC_HEADER).unwrap(),
                    &wire::IpcIdentity {
                        host: "owned".into(),
                        pid: 42,
                    },
                )
                .unwrap(),
            ],
            body,
        )
    };
    assert!(dispatch(
        &mut state,
        request(
            "RegisterFabricRuntimeRequest",
            to_vec(&wire::RuntimeRegistration {
                context: context.clone()
            })
            .unwrap()
        ),
        &bundle
    )
    .is_err());
    assert!(state.runtime.is_none());
    context.code = bundle.identity.clone();
    dispatch(
        &mut state,
        request(
            "RegisterFabricRuntimeRequest",
            to_vec(&wire::RuntimeRegistration {
                context: context.clone(),
            })
            .unwrap(),
        ),
        &bundle,
    )
    .unwrap();
    for (name, success) in [("WrongType", false), ("CustomType", true)] {
        let value = wire::TypeRegistration {
            context: context.clone(),
            service_type: wire::TypeId {
                package: context.code.code.package.clone(),
                name: name.into(),
            },
        };
        assert_eq!(
            dispatch(
                &mut state,
                request("RegisterServiceTypeRequest", to_vec(&value).unwrap()),
                &bundle
            )
            .is_ok(),
            success
        );
        assert_eq!(state.registered_type, success);
    }
    std::fs::remove_dir_all(bundle.root).unwrap();
}
