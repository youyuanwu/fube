use super::{
    methods::{envelope, query_request},
    *,
};
use crate::*;

#[derive(Serialize)]
struct ApplicationWire(
    NamingUri,
    &'static str,
    &'static str,
    i64,
    i64,
    fubst::serde::ObjectArray<Vec<()>>,
    &'static str,
    fubst::serde::ObjectArray<Vec<()>>,
    i64,
);

fn application() -> ApplicationWire {
    ApplicationWire(
        "fabric:/App".parse().unwrap(),
        "AppType",
        "1",
        1,
        1,
        fubst::serde::ObjectArray(vec![]),
        "",
        fubst::serde::ObjectArray(vec![]),
        0,
    )
}

#[derive(Serialize)]
struct ServiceWire(
    i64,
    NamingUri,
    &'static str,
    &'static str,
    bool,
    i64,
    i64,
    bool,
);

fn native_reply<T: Serialize>(
    kind: i64,
    tag: i32,
    records: &[T],
    item: Option<&T>,
    error: i64,
    paging: Option<(&[u8], &str)>,
) -> Body {
    let mut encoder = fubst::Encoder::new();
    encoder
        .object(|encoder| {
            encoder.i64(kind)?;
            encoder.pointer(Some(&tag.to_le_bytes()), |encoder| {
                encoder.object_array(records, |encoder, record| {
                    fubst::serde::to_fields(encoder, record)
                })?;
                encoder.pointer(item.map(|_| &[][..]), |encoder| {
                    fubst::serde::to_fields(encoder, item.unwrap())
                })
            })?;
            encoder.i64(error)?;
            encoder.pointer(paging.map(|(tag, _)| tag), |encoder| {
                encoder.string(paging.unwrap().1)
            })
        })
        .unwrap();
    Body::from_bytes(encoder.finish()).unwrap()
}

fn health_query() -> NodeHealthQueryDescription {
    NodeHealthQueryDescription::new(NodeHealthQueryParameters {
        node_name: "N0020".into(),
        health_policy: Some(NodeHealthPolicy {
            consider_warning_as_error: true,
        }),
        events_filter: Some(HealthEventsFilter::from_bits_retain(u32::MAX).into()),
    })
}

#[tokio::test]
async fn public_read_queries_encode_selectors_and_decode_typed_profiles() {
    let mut client = FurpcClient::new(service(|request: Request<Body>| {
        let (name, arguments) = query_request(request.body());
        let response = match name.as_str() {
            "GetApplicationList" => {
                assert_eq!(
                    arguments,
                    [
                        ("ApplicationTypeName", "AppType"),
                        ("ExcludeApplicationParameters", "true"),
                        ("ContinuationToken", "opaque"),
                        ("MaxResults", "2"),
                    ]
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                );
                envelope(
                    None,
                    None,
                    native_reply(2, 4, &[application()], None, 0, Some((&[], "next"))),
                )
            }
            "GetApplicationServiceList" => {
                assert_eq!(
                    arguments,
                    [
                        ("ApplicationName", "fabric:/App"),
                        ("ServiceTypeName", "ServiceType"),
                    ]
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                );
                let service = ServiceWire(
                    2,
                    "fabric:/App/Service".parse().unwrap(),
                    "ServiceType",
                    "2",
                    true,
                    1,
                    1,
                    false,
                );
                envelope(
                    Some(5),
                    Some("QueryReply"),
                    native_reply(2, 2, &[service], None, 0, None),
                )
            }
            "GetNodeHealth" => {
                assert_eq!(
                    arguments,
                    [
                        ("ClusterHealthPolicy", r#"{"ConsiderWarningAsError":true}"#),
                        ("EventsFilter", r#"{"HealthStateFilter":4294967295}"#),
                        ("NodeName", "N0020"),
                    ]
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                );
                let health = NodeHealth {
                    node_name: "N0020".into(),
                    health_events: vec![],
                    aggregated_health_state: HealthState::Error,
                    unhealthy_evaluations: vec![],
                };
                envelope(
                    Some(16),
                    Some("OperationSuccessAction"),
                    native_reply(1, 16, &[], Some(&health), 0, None),
                )
            }
            other => panic!("unexpected query {other}"),
        };
        ready(Ok::<_, furpc::Error>(response))
    }));
    let query = ApplicationQueryDescription {
        parameters: ApplicationQueryParameters {
            filter: ApplicationListFilter::TypeName("AppType".into()),
            exclude_application_parameters: true,
        },
        paging: Paging {
            continuation_token: Some("opaque".into()),
            max_results: std::num::NonZeroU64::new(2),
        },
    };
    let page = client
        .get_application_list(Request::new(query), deadline())
        .await
        .unwrap()
        .into_body();
    assert_eq!(page.items[0].name.as_str(), "fabric:/App");
    assert_eq!(
        page.items[0].definition_kind,
        ApplicationDefinitionKind::ServiceFabricApplicationDescription
    );
    assert_eq!(page.continuation_token.as_deref(), Some("next"));

    let query = ServiceQueryDescription::new(ServiceQueryParameters {
        application_name: "fabric:/App".parse().unwrap(),
        filter: ServiceListFilter::TypeName("ServiceType".into()),
    });
    let service = client
        .get_service_list(Request::new(query), deadline())
        .await
        .unwrap()
        .into_body()
        .items
        .remove(0);
    assert_eq!(service.kind, ServiceKind::Stateful);
    assert!(service.has_persisted_state);
    let health = client
        .get_node_health(Request::new(health_query()), deadline())
        .await
        .unwrap();
    assert_eq!(health.headers().effective().actor.get(), 16);
    assert_eq!(health.body().aggregated_health_state, HealthState::Error);
}

#[tokio::test]
async fn public_read_queries_preserve_strict_paging_and_validate_before_domain_errors() {
    let mut responses = [
        envelope(
            None,
            None,
            native_reply(2, 4, &[application()], None, -123, Some((b"typed", ""))),
        ),
        envelope(
            None,
            None,
            native_reply(
                2,
                2,
                &[ServiceWire(
                    1,
                    "fabric:/App/Service".parse().unwrap(),
                    "ServiceType",
                    "1",
                    true,
                    1,
                    1,
                    false,
                )],
                None,
                -123,
                None,
            ),
        ),
        envelope(
            Some(5),
            Some("OperationSuccessAction"),
            Body::from_bytes(vec![0xff]).unwrap(),
        ),
        envelope(
            Some(16),
            Some("QueryReply"),
            Body::from_bytes(vec![0xff]).unwrap(),
        ),
    ]
    .into_iter();
    let mut client = FurpcClient::new(service(move |_| {
        ready(Ok::<_, furpc::Error>(responses.next().unwrap()))
    }));
    assert!(matches!(
        client
            .get_application_list(
                Request::new(ApplicationQueryDescription::default()),
                deadline()
            )
            .await,
        Err(FurpcClientError::Protocol(_))
    ));
    assert!(matches!(
        client
            .get_service_list(
                Request::new(ServiceQueryDescription::new(ServiceQueryParameters::new(
                    "fabric:/App".parse().unwrap()
                ))),
                deadline()
            )
            .await,
        Err(FurpcClientError::Protocol(_))
    ));
    for _ in 0..2 {
        assert!(matches!(
            client
                .get_node_health(Request::new(health_query()), deadline())
                .await,
            Err(FurpcClientError::Protocol(_))
        ));
    }
}

#[tokio::test]
async fn public_read_queries_reject_invalid_parameters_without_submitting() {
    let calls = AtomicUsize::new(0);
    let mut client = FurpcClient::new(service(|_: Request<Body>| {
        calls.fetch_add(1, Ordering::SeqCst);
        ready(Ok::<_, furpc::Error>(Response::new(Body::empty())))
    }));
    let app = ApplicationQueryDescription::new(ApplicationQueryParameters {
        filter: ApplicationListFilter::DefinitionKind(
            ApplicationDefinitionKindFilter::from_bits_retain(u32::MAX),
        ),
        ..Default::default()
    });
    let service = ServiceQueryDescription::new(ServiceQueryParameters::new(
        "fabric:/System".parse().unwrap(),
    ));
    let health = NodeHealthQueryDescription::new(NodeHealthQueryParameters::new(""));
    for error in [
        client
            .get_application_list(Request::new(app), deadline())
            .await
            .unwrap_err(),
        client
            .get_service_list(Request::new(service), deadline())
            .await
            .unwrap_err(),
        client
            .get_node_health(Request::new(health), deadline())
            .await
            .unwrap_err(),
    ] {
        assert!(matches!(
            error,
            FurpcClientError::Native(furpc::Error::Codec {
                phase: "encode",
                ..
            })
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
