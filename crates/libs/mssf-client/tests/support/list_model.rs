//! Independent source-shaped models, never live interoperability evidence.
use super::*;
use mssf_client::{
    ApplicationDefinitionKind, ApplicationDefinitionKindFilter as Definitions,
    ApplicationListFilter, ApplicationQueryDescription, ApplicationQueryParameters,
    ApplicationStatus, HealthState, Paging, ServiceKind, ServiceListFilter,
    ServiceQueryDescription, ServiceQueryParameters, ServiceStatus,
};
use std::{collections::BTreeMap, num::NonZeroU64};

#[derive(Clone)]
struct Shape {
    service: bool,
    wrapper: i32,
    result_kind: i64,
    null: bool,
    item: bool,
    suffix: usize,
    status: i64,
    health: i64,
    definition: i64,
    kind: i64,
    persisted: bool,
    uri_type: i64,
    segments: u32,
    path: &'static str,
    duplicate: bool,
    upgrade: bool,
    upgrade_parameters: bool,
    paging_tag: bool,
    wrong_metadata: bool,
    wrong_suffix: Option<usize>,
    error: i64,
}

impl Shape {
    fn application() -> Self {
        Self {
            service: false,
            wrapper: 4,
            result_kind: 2,
            null: false,
            item: false,
            suffix: 3,
            status: 1,
            health: 2,
            definition: 0,
            kind: 1,
            persisted: false,
            uri_type: 1,
            segments: 2,
            path: "/Model/App",
            duplicate: false,
            upgrade: false,
            upgrade_parameters: false,
            paging_tag: false,
            wrong_metadata: false,
            wrong_suffix: None,
            error: 0,
        }
    }
    fn service() -> Self {
        Self {
            service: true,
            wrapper: 2,
            suffix: 2,
            ..Self::application()
        }
    }
}

fn uri(e: &mut Encoder, s: &Shape) -> mssf_transport::Result<()> {
    e.object(|e| {
        e.i64(s.uri_type);
        e.string("fabric")?;
        e.string("")?;
        e.i64(0);
        e.string("")?;
        e.i32(-1);
        e.string(s.path)?;
        e.string("")?;
        e.string("")?;
        e.u32(s.segments);
        e.string("Model")?;
        e.string("App")
    })
}

fn record(e: &mut Encoder, s: &Shape) -> mssf_transport::Result<()> {
    if s.service {
        e.i64(s.kind);
    }
    uri(e, s)?;
    e.string("ModelType")?;
    e.string("1.2")?;
    if s.service {
        e.boolean(s.persisted);
    } else {
        e.i64(s.status);
    }
    if s.wrong_metadata {
        e.string("not an enum")?;
    } else {
        e.i64(s.health);
    }
    if s.service {
        if s.suffix >= 1 {
            if s.wrong_suffix == Some(0) {
                e.string("not a status")?;
            } else {
                e.i64(s.status);
            }
        }
        if s.suffix >= 2 {
            if s.wrong_suffix == Some(1) {
                e.string("not a group flag")?;
            } else {
                e.boolean(true);
            }
        }
    } else {
        let pairs = if s.duplicate {
            vec![("p", "v"), ("p", "other")]
        } else {
            vec![("p", "v")]
        };
        e.object_array(&pairs, |e, (k, v)| {
            e.string(k)?;
            e.string(v)
        })?;
        if s.suffix >= 1 {
            e.string(if s.upgrade { "unsupported" } else { "" })?;
        }
        if s.suffix >= 2 {
            let pairs: &[(&str, &str)] = if s.upgrade_parameters {
                &[("upgrade", "value")]
            } else {
                &[]
            };
            e.object_array(pairs, |e, (key, value)| {
                e.string(key)?;
                e.string(value)
            })?;
        }
        if s.suffix >= 3 {
            e.i64(s.definition);
        }
    }
    Ok(())
}

fn list_body(s: &Shape, token: Option<&str>) -> Vec<u8> {
    body(|e| {
        e.i64(s.result_kind);
        e.pointer(
            (!s.null).then_some(s.wrapper.to_le_bytes().as_slice()),
            |e| {
                e.object_array(&[s], |e, s| record(e, s))?;
                e.pointer(s.item.then_some(&[][..]), |_| Ok(()))
            },
        )?;
        e.i64(s.error);
        e.pointer(
            token.map(|_| if s.paging_tag { &b"bad"[..] } else { &[][..] }),
            |e| e.string(token.unwrap()),
        )
    })
}

fn aggregate(request: &Message, body: Vec<u8>) -> Message {
    Message {
        headers: vec![Header::relates_to(request.properties().unwrap().id).unwrap()],
        body,
    }
}

fn ping_response(request: &Message) -> Message {
    let mut response = reply(request, "PingRequest", ping_body());
    response.headers.push(Header::version(1, 1).unwrap());
    response
}

fn arguments(request: &Message) -> (String, BTreeMap<String, String>) {
    assert_eq!(request.properties().unwrap().action, "QueryRequest");
    let mut d = Decoder::new(&request.body);
    let result = d
        .object(|d| {
            let args =
                d.object(|d| d.object_array(|d| Ok((d.object(|d| d.string())?, d.string()?))))?;
            Ok((d.string()?, args.into_iter().collect()))
        })
        .unwrap();
    d.finish().unwrap();
    result
}

#[tokio::test]
async fn application_selectors_exclusion_and_native_paging_are_explicit() {
    let filters = [
        (ApplicationListFilter::All, None),
        (
            ApplicationListFilter::Name("fabric:/Model/App".parse().unwrap()),
            Some(("ApplicationName", "fabric:/Model/App")),
        ),
        (
            ApplicationListFilter::TypeName("CaseSensitive".into()),
            Some(("ApplicationTypeName", "CaseSensitive")),
        ),
        (
            ApplicationListFilter::DefinitionKind(
                Definitions::COMPOSE | Definitions::MESH_APPLICATION_DESCRIPTION,
            ),
            Some(("ApplicationDefinitionKindFilter", "6")),
        ),
        (
            ApplicationListFilter::DefinitionKind(Definitions::ALL),
            Some(("ApplicationDefinitionKindFilter", "65535")),
        ),
        (
            ApplicationListFilter::DefinitionKind(Definitions::DEFAULT),
            None,
        ),
    ];
    for (filter, expected) in filters {
        let (client, server) = single_reply(move |request| {
            let (name, args) = arguments(request);
            assert_eq!(name, "GetApplicationList");
            let mut expected_args = BTreeMap::from([
                ("ExcludeApplicationParameters".into(), "true".into()),
                ("ContinuationToken".into(), "opaque:not-a-node-id".into()),
                ("MaxResults".into(), i64::MAX.to_string()),
            ]);
            if let Some((k, v)) = expected {
                expected_args.insert(k.into(), v.into());
            }
            assert_eq!(args, expected_args);
            aggregate(
                request,
                list_body(&Shape::application(), Some("fabric:/Model/App")),
            )
        })
        .await;
        let page = client
            .get_application_list_with_timeout(
                &ApplicationQueryDescription {
                    parameters: ApplicationQueryParameters {
                        filter,
                        exclude_application_parameters: true,
                    },
                    paging: Paging {
                        continuation_token: Some("opaque:not-a-node-id".into()),
                        max_results: NonZeroU64::new(i64::MAX as u64),
                    },
                },
                TIMEOUT_DURATION,
            )
            .await
            .unwrap();
        assert_eq!(
            page.continuation_token.as_deref(),
            Some("fabric:/Model/App")
        );
        let app = &page.items[0];
        assert_eq!(app.name.as_str(), "fabric:/Model/App");
        assert_eq!(app.type_name, "ModelType");
        assert_eq!(app.type_version, "1.2");
        assert_eq!(app.status, ApplicationStatus::Ready);
        assert_eq!(app.health_state, HealthState::Warning);
        assert_eq!(app.parameters.get("p").map(String::as_str), Some("v"));
        assert!(app.upgrade_type_version.is_empty());
        assert!(app.upgrade_parameters.is_empty());
        assert_eq!(
            app.definition_kind,
            ApplicationDefinitionKind::ServiceFabricApplicationDescription
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn service_selectors_route_to_application_service_list_and_both_kinds_decode() {
    for (filter, key, value, kind, persisted) in [
        (ServiceListFilter::All, None, "", 1, false),
        (
            ServiceListFilter::Name("fabric:/Model/App".parse().unwrap()),
            Some("ServiceName"),
            "fabric:/Model/App",
            2,
            true,
        ),
        (
            ServiceListFilter::TypeName("ExactType".into()),
            Some("ServiceTypeName"),
            "ExactType",
            2,
            false,
        ),
    ] {
        let (client, server) = single_reply(move |request| {
            let (name, args) = arguments(request);
            assert_eq!(name, "GetApplicationServiceList");
            let mut expected = BTreeMap::from([("ApplicationName".into(), "fabric:/Model".into())]);
            if let Some(key) = key {
                expected.insert(key.into(), value.into());
            }
            assert_eq!(args, expected);
            aggregate(
                request,
                list_body(
                    &Shape {
                        kind,
                        persisted,
                        ..Shape::service()
                    },
                    None,
                ),
            )
        })
        .await;
        let mut query = ServiceQueryDescription::new(ServiceQueryParameters::new(
            "fabric:/Model".parse().unwrap(),
        ));
        query.parameters.filter = filter;
        query.paging.continuation_token = Some(String::new());
        let page = client.get_service_list(&query).await.unwrap();
        let service = &page.items[0];
        assert_eq!(
            service.kind,
            if kind == 1 {
                ServiceKind::Stateless
            } else {
                ServiceKind::Stateful
            }
        );
        assert_eq!(service.has_persisted_state, persisted);
        assert_eq!(service.status, ServiceStatus::Active);
        assert_eq!(service.health_state, HealthState::Warning);
        assert_eq!(service.manifest_version, "1.2");
        assert!(service.is_service_group);
        assert!(page.continuation_token.is_none());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn optional_record_suffixes_use_only_native_defaults() {
    for service in [false, true] {
        for suffix in 0..=if service { 2 } else { 3 } {
            let s = Shape {
                suffix,
                ..if service {
                    Shape::service()
                } else {
                    Shape::application()
                }
            };
            let (client, server) =
                single_reply(move |r| aggregate(r, list_body(&s, Some("")))).await;
            if service {
                let page = client
                    .get_service_list(&ServiceQueryDescription::new(ServiceQueryParameters::new(
                        "fabric:/Model".parse().unwrap(),
                    )))
                    .await
                    .unwrap();
                assert_eq!(
                    page.items[0].status,
                    if suffix == 0 {
                        ServiceStatus::Unknown
                    } else {
                        ServiceStatus::Active
                    }
                );
                assert_eq!(page.items[0].is_service_group, suffix == 2);
                assert!(page.continuation_token.is_none());
            } else {
                let page = client
                    .get_application_list(&ApplicationQueryDescription::default())
                    .await
                    .unwrap();
                assert_eq!(
                    page.items[0].definition_kind,
                    if suffix == 3 {
                        ApplicationDefinitionKind::ServiceFabricApplicationDescription
                    } else {
                        ApplicationDefinitionKind::Invalid
                    }
                );
                assert!(page.items[0].upgrade_type_version.is_empty());
                assert!(page.items[0].upgrade_parameters.is_empty());
            }
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn all_named_status_health_definition_values_are_not_normalized() {
    for status in 0..=5 {
        for service in [false, true] {
            for health in [0, 1, 2, 3, 65535] {
                let s = Shape {
                    status,
                    health,
                    ..if service {
                        Shape::service()
                    } else {
                        Shape::application()
                    }
                };
                let (client, server) =
                    single_reply(move |r| aggregate(r, list_body(&s, None))).await;
                if service {
                    let page = client
                        .get_service_list(&ServiceQueryDescription::new(
                            ServiceQueryParameters::new("fabric:/Model".parse().unwrap()),
                        ))
                        .await
                        .unwrap();
                    let expected = [
                        ServiceStatus::Unknown,
                        ServiceStatus::Active,
                        ServiceStatus::Upgrading,
                        ServiceStatus::Deleting,
                        ServiceStatus::Creating,
                        ServiceStatus::Failed,
                    ];
                    assert_eq!(page.items[0].status, expected[status as usize]);
                    assert_eq!(page.items[0].health_state.as_raw(), health);
                } else {
                    let page = client
                        .get_application_list(&ApplicationQueryDescription::default())
                        .await
                        .unwrap();
                    let expected = [
                        ApplicationStatus::Invalid,
                        ApplicationStatus::Ready,
                        ApplicationStatus::Upgrading,
                        ApplicationStatus::Creating,
                        ApplicationStatus::Deleting,
                        ApplicationStatus::Failed,
                    ];
                    assert_eq!(page.items[0].status, expected[status as usize]);
                    assert_eq!(page.items[0].health_state.as_raw(), health);
                }
                server.await.unwrap();
            }
        }
    }
    for (definition, expected) in [
        (
            0,
            ApplicationDefinitionKind::ServiceFabricApplicationDescription,
        ),
        (1, ApplicationDefinitionKind::Compose),
        (2, ApplicationDefinitionKind::MeshApplicationDescription),
        (65535, ApplicationDefinitionKind::Invalid),
    ] {
        let (client, server) = single_reply(move |r| {
            aggregate(
                r,
                list_body(
                    &Shape {
                        definition,
                        ..Shape::application()
                    },
                    None,
                ),
            )
        })
        .await;
        assert_eq!(
            client
                .get_application_list(&ApplicationQueryDescription::default())
                .await
                .unwrap()
                .items[0]
                .definition_kind,
            expected
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn malformed_list_successes_retire_shared_stream() {
    for service in [false, true] {
        let base = if service {
            Shape::service()
        } else {
            Shape::application()
        };
        let mut shapes = vec![
            Shape {
                wrong_metadata: true,
                ..base.clone()
            },
            Shape {
                wrapper: 15,
                ..base.clone()
            },
            Shape {
                result_kind: 1,
                ..base.clone()
            },
            Shape {
                null: true,
                ..base.clone()
            },
            Shape {
                item: true,
                ..base.clone()
            },
            Shape {
                status: 99,
                ..base.clone()
            },
            Shape {
                health: -1,
                ..base.clone()
            },
            Shape {
                uri_type: 0,
                ..base.clone()
            },
            Shape {
                segments: 4097,
                ..base.clone()
            },
            Shape {
                segments: 1,
                ..base.clone()
            },
            Shape {
                path: "/Model/Other",
                ..base.clone()
            },
            Shape {
                path: "/Model/Unicodeé",
                ..base.clone()
            },
            Shape {
                paging_tag: true,
                ..base.clone()
            },
        ];
        if service {
            shapes.push(Shape {
                kind: 0,
                ..base.clone()
            });
            shapes.push(Shape {
                kind: 7,
                ..base.clone()
            });
            shapes.push(Shape {
                persisted: true,
                ..base.clone()
            });
            for position in 0..=1 {
                shapes.push(Shape {
                    wrong_suffix: Some(position),
                    ..base.clone()
                });
            }
        } else {
            shapes.push(Shape {
                definition: 3,
                ..base.clone()
            });
            shapes.push(Shape {
                duplicate: true,
                ..base.clone()
            });
            shapes.push(Shape {
                upgrade: true,
                ..base.clone()
            });
            shapes.push(Shape {
                upgrade_parameters: true,
                ..base.clone()
            });
        }
        for s in shapes {
            let (client, server) =
                single_reply(move |r| aggregate(r, list_body(&s, Some("token")))).await;
            let outcome = if service {
                client
                    .get_service_list(&ServiceQueryDescription::new(ServiceQueryParameters::new(
                        "fabric:/Model".parse().unwrap(),
                    )))
                    .await
                    .map(|_| ())
            } else {
                client
                    .get_application_list(&ApplicationQueryDescription::default())
                    .await
                    .map(|_| ())
            };
            assert!(matches!(outcome, Err(Error::Protocol(_))), "{outcome:?}");
            assert!(client.is_closed());
            assert!(client.ping().await.is_err());
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn truncated_and_wrong_metadata_are_not_missing_optional_suffixes() {
    for service in [false, true] {
        let s = if service {
            Shape::service()
        } else {
            Shape::application()
        };
        let valid = list_body(&s, Some("next"));
        for removed in [1, 2, 5, valid.len() / 2] {
            let bytes = valid[..valid.len() - removed].to_vec();
            let (client, server) = single_reply(move |r| aggregate(r, bytes)).await;
            let outcome = if service {
                client
                    .get_service_list(&ServiceQueryDescription::new(ServiceQueryParameters::new(
                        "fabric:/Model".parse().unwrap(),
                    )))
                    .await
                    .map(|_| ())
            } else {
                client
                    .get_application_list(&ApplicationQueryDescription::default())
                    .await
                    .map(|_| ())
            };
            assert!(matches!(outcome, Err(Error::Protocol(_))));
            assert!(client.is_closed());
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn invalid_list_inputs_and_timeouts_send_no_request() {
    let (client, server) = single_reply(|r| {
        assert_eq!(r.properties().unwrap().action, "PingRequest");
        ping_response(r)
    })
    .await;
    assert!(Definitions::try_from_bits(8).is_err());
    for filter in [
        ApplicationListFilter::TypeName(String::new()),
        ApplicationListFilter::TypeName("x".repeat(32769)),
        ApplicationListFilter::DefinitionKind(Definitions::from_bits(8).unwrap()),
        ApplicationListFilter::DefinitionKind(Definitions::from_bits_retain(u32::MAX)),
        ApplicationListFilter::DefinitionKind(Definitions::ALL & !Definitions::COMPOSE),
        ApplicationListFilter::DefinitionKind({
            let mut encoder = Encoder::new();
            encoder.u32(8);
            mssf_transport::serialization::serde::from_slice(&encoder.finish()).unwrap()
        }),
    ] {
        assert!(matches!(
            client
                .get_application_list(&ApplicationQueryDescription::new(
                    ApplicationQueryParameters {
                        filter,
                        ..Default::default()
                    }
                ))
                .await,
            Err(Error::InvalidInput(_))
        ));
    }
    for application in ["", "fabric:", "fabric:/", "FABRIC:/a", "fabric:/é"] {
        assert!(matches!(
            application.parse::<mssf_client::NamingUri>(),
            Err(Error::InvalidInput(_))
        ));
    }
    let mut service = ServiceQueryDescription::new(ServiceQueryParameters::new(
        "fabric:/System".parse().unwrap(),
    ));
    assert!(matches!(
        client.get_service_list(&service).await,
        Err(Error::InvalidInput(_))
    ));
    service.parameters.application_name = "fabric:/Model".parse().unwrap();
    service.parameters.filter = ServiceListFilter::TypeName(String::new());
    assert!(matches!(
        client.get_service_list(&service).await,
        Err(Error::InvalidInput(_))
    ));
    for name in ["fabric:", "fabric:/bad/", "fabric:/bad//name"] {
        assert!(matches!(
            name.parse::<mssf_client::NamingUri>(),
            Err(Error::InvalidInput(_))
        ));
    }
    service.parameters.filter = ServiceListFilter::All;
    service.paging.max_results = NonZeroU64::new(u64::MAX);
    assert!(matches!(
        client.get_service_list(&service).await,
        Err(Error::InvalidInput(_))
    ));
    for q in [
        ApplicationQueryDescription {
            paging: Paging {
                max_results: NonZeroU64::new(u64::MAX),
                ..Default::default()
            },
            ..Default::default()
        },
        ApplicationQueryDescription {
            paging: Paging {
                continuation_token: Some("x".repeat(32769)),
                ..Default::default()
            },
            ..Default::default()
        },
    ] {
        assert!(matches!(
            client.get_application_list(&q).await,
            Err(Error::InvalidInput(_))
        ));
    }
    for timeout in [Duration::ZERO, Duration::from_nanos(99), Duration::MAX] {
        assert!(matches!(
            client
                .get_application_list_with_timeout(&Default::default(), timeout)
                .await,
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            client
                .get_service_list_with_timeout(
                    &ServiceQueryDescription::new(ServiceQueryParameters::new(
                        "fabric:/Model".parse().unwrap()
                    )),
                    timeout
                )
                .await,
            Err(Error::InvalidInput(_))
        ));
    }
    client.ping().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn semantic_list_validation_precedes_embedded_query_errors() {
    for shape in [
        Shape {
            health: -99,
            ..Shape::application()
        },
        Shape {
            duplicate: true,
            ..Shape::application()
        },
        Shape {
            upgrade: true,
            ..Shape::application()
        },
        Shape {
            upgrade_parameters: true,
            ..Shape::application()
        },
        Shape {
            uri_type: 0,
            ..Shape::application()
        },
        Shape {
            health: -99,
            ..Shape::service()
        },
        Shape {
            kind: 0,
            ..Shape::service()
        },
        Shape {
            kind: 7,
            ..Shape::service()
        },
        Shape {
            status: 99,
            ..Shape::service()
        },
        Shape {
            wrong_suffix: Some(0),
            ..Shape::service()
        },
        Shape {
            wrong_suffix: Some(1),
            ..Shape::service()
        },
        Shape {
            persisted: true,
            ..Shape::service()
        },
        Shape {
            path: "/Model/Other",
            ..Shape::service()
        },
    ] {
        let service = shape.service;
        let (client, server) = single_reply(move |request| {
            aggregate(
                request,
                list_body(
                    &Shape {
                        error: REMOTE_CODE,
                        ..shape
                    },
                    None,
                ),
            )
        })
        .await;
        let result = if service {
            client
                .get_service_list(&ServiceQueryDescription::new(ServiceQueryParameters::new(
                    "fabric:/Model".parse().unwrap(),
                )))
                .await
                .map(|_| ())
        } else {
            client
                .get_application_list(&Default::default())
                .await
                .map(|_| ())
        };
        assert!(matches!(result, Err(Error::Protocol(_))), "{result:?}");
        assert!(client.is_closed());
        assert!(matches!(client.terminal_error(), Some(Error::Protocol(_))));
        client.close().await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn native_list_errors_are_local_and_allow_mixed_reuse() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        for index in 0..5 {
            let request = read_message(&mut stream).await.unwrap();
            let response = match index {
                0 => aggregate(
                    &request,
                    list_body(
                        &Shape {
                            null: true,
                            result_kind: 0,
                            error: REMOTE_CODE,
                            ..Shape::application()
                        },
                        None,
                    ),
                ),
                1 => reply(
                    &request,
                    "ClientOperationFailure",
                    body(|e| {
                        e.i64(REMOTE_CODE);
                        e.string("missing application")
                    }),
                ),
                2 => aggregate(&request, list_body(&Shape::application(), None)),
                3 => aggregate(&request, list_body(&Shape::service(), None)),
                _ => ping_response(&request),
            };
            write_message(&mut stream, &response).await.unwrap();
        }
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    assert!(matches!(
        client.get_application_list(&Default::default()).await,
        Err(Error::Remote {
            code: REMOTE_CODE,
            ..
        })
    ));
    assert!(!client.is_closed());
    assert!(matches!(
        client
            .get_service_list(&ServiceQueryDescription::new(ServiceQueryParameters::new(
                "fabric:/Model".parse().unwrap()
            )))
            .await,
        Err(Error::Remote {
            code: REMOTE_CODE,
            ..
        })
    ));
    client
        .get_application_list(&Default::default())
        .await
        .unwrap();
    client
        .get_service_list(&ServiceQueryDescription::new(ServiceQueryParameters::new(
            "fabric:/Model".parse().unwrap(),
        )))
        .await
        .unwrap();
    client.ping().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn mixed_lists_ping_and_name_complete_out_of_order_on_one_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let mut requests = Vec::new();
        for _ in 0..4 {
            requests.push(read_message(&mut stream).await.unwrap());
        }
        for request in requests.into_iter().rev() {
            let response = match request.properties().unwrap().action.as_str() {
                "PingRequest" => ping_response(&request),
                "NameExistsRequest" => reply(&request, "NameOperationReply", naming_body(true, 1)),
                "QueryRequest" => {
                    let (name, _) = arguments(&request);
                    aggregate(
                        &request,
                        list_body(
                            &if name == "GetApplicationList" {
                                Shape::application()
                            } else {
                                Shape::service()
                            },
                            Some("native"),
                        ),
                    )
                }
                other => panic!("unexpected action {other}"),
            };
            write_message(&mut stream, &response).await.unwrap();
        }
        let request = read_message(&mut stream).await.unwrap();
        write_message(&mut stream, &ping_response(&request))
            .await
            .unwrap();
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    let apps = ApplicationQueryDescription::default();
    let services = ServiceQueryDescription::new(ServiceQueryParameters::new(
        "fabric:/Model".parse().unwrap(),
    ));
    let (a, s, p, n) = tokio::join!(
        client.get_application_list(&apps),
        client.get_service_list(&services),
        client.ping(),
        client.name_exists("fabric:/Model")
    );
    assert_eq!(a.unwrap().items.len(), 1);
    assert_eq!(s.unwrap().items.len(), 1);
    p.unwrap();
    assert!(n.unwrap());
    client.ping().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_list_is_terminal_before_pending_waiter_repoll() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let a = read_message(&mut stream).await.unwrap();
        let _neighbor = read_message(&mut stream).await.unwrap();
        write_message(
            &mut stream,
            &aggregate(
                &a,
                list_body(
                    &Shape {
                        wrapper: 15,
                        ..Shape::application()
                    },
                    None,
                ),
            ),
        )
        .await
        .unwrap();
        sent.send(()).unwrap();
        assert_eq!(
            stream.read_u8().await.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    let c = client.clone();
    let first = tokio::spawn(async move { c.get_application_list(&Default::default()).await });
    tokio::task::yield_now().await;
    let c = client.clone();
    let second = tokio::spawn(async move {
        c.get_service_list(&ServiceQueryDescription::new(ServiceQueryParameters::new(
            "fabric:/Model".parse().unwrap(),
        )))
        .await
    });
    received.await.unwrap();
    tokio::time::timeout(TIMEOUT_DURATION, async {
        while !client.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(first.await.unwrap(), Err(Error::Protocol(_))));
    assert!(matches!(second.await.unwrap(), Err(Error::Protocol(_))));
    assert!(client.ping().await.is_err());
    server.await.unwrap();
}

#[tokio::test]
async fn explicit_service_paging_stops_at_empty_terminal_page_without_hidden_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        for next in [false, true] {
            let request = read_message(&mut stream).await.unwrap();
            let (name, args) = arguments(&request);
            assert_eq!(name, "GetApplicationServiceList");
            assert_eq!(args.get("MaxResults").unwrap(), "1");
            assert_eq!(
                args.get("ContinuationToken").map(String::as_str),
                next.then_some("opaque")
            );
            let bytes = if next {
                body(|e| {
                    e.i64(2);
                    e.pointer(Some(&2i32.to_le_bytes()), |e| {
                        e.object_array::<()>(&[], |_, _| Ok(()))?;
                        e.pointer(None, |_| Ok(()))
                    })?;
                    e.i64(0);
                    e.pointer(None, |_| Ok(()))
                })
            } else {
                list_body(&Shape::service(), Some("opaque"))
            };
            write_message(&mut stream, &aggregate(&request, bytes))
                .await
                .unwrap();
        }
        let request = read_message(&mut stream).await.unwrap();
        assert_eq!(request.properties().unwrap().action, "PingRequest");
        write_message(&mut stream, &ping_response(&request))
            .await
            .unwrap();
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    let mut query = ServiceQueryDescription::new(ServiceQueryParameters::new(
        "fabric:/Model".parse().unwrap(),
    ));
    query.paging.max_results = NonZeroU64::new(1);
    let first = client.get_service_list(&query).await.unwrap();
    assert_eq!(first.continuation_token.as_deref(), Some("opaque"));
    query.paging.continuation_token = first.continuation_token;
    let terminal = client.get_service_list(&query).await.unwrap();
    assert!(terminal.items.is_empty());
    assert!(terminal.continuation_token.is_none());
    client.ping().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn present_aggregate_headers_are_validated_for_each_list() {
    for service in [false, true] {
        for (actor, action, valid) in [
            (5, "QueryReply", true),
            (16, "QueryReply", false),
            (5, "OperationSuccessAction", false),
        ] {
            let (client, server) = single_reply(move |r| {
                let s = if service {
                    Shape::service()
                } else {
                    Shape::application()
                };
                let mut response = reply(r, action, list_body(&s, None));
                response.headers[0] = Header::actor(actor).unwrap();
                response
            })
            .await;
            let result = if service {
                client
                    .get_service_list(&ServiceQueryDescription::new(ServiceQueryParameters::new(
                        "fabric:/Model".parse().unwrap(),
                    )))
                    .await
                    .map(|_| ())
            } else {
                client
                    .get_application_list(&Default::default())
                    .await
                    .map(|_| ())
            };
            assert_eq!(result.is_ok(), valid);
            if !valid {
                assert!(client.is_closed());
            }
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn timed_out_list_ignores_late_malformed_reply_and_preserves_reuse() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let request = read_message(&mut stream).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        write_message(&mut stream, &aggregate(&request, vec![0xff]))
            .await
            .unwrap();
        let request = read_message(&mut stream).await.unwrap();
        write_message(
            &mut stream,
            &aggregate(&request, list_body(&Shape::service(), None)),
        )
        .await
        .unwrap();
        let request = read_message(&mut stream).await.unwrap();
        write_message(&mut stream, &ping_response(&request))
            .await
            .unwrap();
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    assert!(matches!(
        client
            .get_application_list_with_timeout(&Default::default(), Duration::from_millis(75))
            .await,
        Err(Error::Timeout)
    ));
    client
        .get_service_list(&ServiceQueryDescription::new(ServiceQueryParameters::new(
            "fabric:/Model".parse().unwrap(),
        )))
        .await
        .unwrap();
    client.ping().await.unwrap();
    server.await.unwrap();
}
