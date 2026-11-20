//! Real nonempty native queries against an independently prepared owned workload.
use fube_client::*;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    time::Duration,
};

#[path = "support/list_fixture.rs"]
mod oracle;
use oracle::{output, owner, Http, Journal, Result};
#[path = "support/deployment_oracle.rs"]
mod deployment;
#[path = "support/fixture.rs"]
mod fixture;
#[path = "support/furpc.rs"]
mod furpc_support;
#[path = "support/rest.rs"]
mod rest;
#[path = "support/topology_oracle.rs"]
mod topology;

fn compare_application(app: &Application, observed: &Value) {
    assert_eq!(app.name.as_str(), observed["Name"]);
    assert_eq!(app.type_name, observed["TypeName"]);
    assert_eq!(app.type_version, observed["TypeVersion"]);
    assert_eq!(format!("{:?}", app.status), observed["Status"]);
    assert_eq!(format!("{:?}", app.health_state), observed["HealthState"]);
    assert_eq!(
        app.definition_kind,
        ApplicationDefinitionKind::ServiceFabricApplicationDescription
    );
    assert_eq!(
        app.parameters.get("Evidence").map(String::as_str),
        Some("owned-list-value")
    );
    let parameters: BTreeMap<_, _> = observed["Parameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|pair| {
            (
                pair["Key"].as_str().unwrap(),
                pair["Value"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        app.parameters
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect::<BTreeMap<_, _>>(),
        parameters
    );
}

fn compare_service(service: &Service, observed: &Value) {
    assert_eq!(service.name.as_str(), observed["Name"]);
    assert_eq!(service.type_name, observed["TypeName"]);
    assert_eq!(service.manifest_version, observed["ManifestVersion"]);
    assert_eq!(format!("{:?}", service.status), observed["ServiceStatus"]);
    assert_eq!(
        format!("{:?}", service.health_state),
        observed["HealthState"]
    );
    assert_eq!(service.is_service_group, observed["IsServiceGroup"]);
    match service.kind {
        ServiceKind::Invalid => panic!("invalid service kind in a validated query result"),
        ServiceKind::Stateless => {
            assert_eq!(observed["ServiceKind"], "Stateless");
            assert!(!service.has_persisted_state);
        }
        ServiceKind::Stateful => {
            assert_eq!(observed["ServiceKind"], "Stateful");
            assert_eq!(service.has_persisted_state, observed["HasPersistedState"]);
        }
    }
}

fn application_evidence(app: &Application) -> Value {
    json!({
        "name": app.name, "type_name": app.type_name, "type_version": app.type_version,
        "status": format!("{:?}", app.status), "health_state": format!("{:?}", app.health_state),
        "parameters": app.parameters, "definition_kind": format!("{:?}", app.definition_kind),
    })
}

fn service_evidence(service: &Service) -> Value {
    json!({
        "name": service.name, "type_name": service.type_name,
        "manifest_version": service.manifest_version, "kind": format!("{:?}", service.kind),
        "has_persisted_state": service.has_persisted_state,
        "status": format!("{:?}", service.status), "health_state": format!("{:?}", service.health_state),
        "is_service_group": service.is_service_group,
    })
}

async fn observe_furpc(
    client: &mut FurpcClient,
    generation: u64,
    journal: &Journal,
    observed_apps: &[Value],
    observed_services: &[Value],
) -> Result<Value> {
    use furpc::Request;
    use furpc_support::{body, deadline};

    let start_ms = furpc_support::now_ms();
    let mut query = ApplicationQueryDescription::new(ApplicationQueryParameters {
        filter: ApplicationListFilter::TypeName(journal.type_name.clone()),
        ..Default::default()
    });
    query.paging.max_results = NonZeroU64::new(1);
    let first = body(
        client
            .get_application_list(Request::new(query.clone()), deadline())
            .await?,
        generation,
    );
    assert_eq!(first.items.len(), 1);
    assert!(first
        .continuation_token
        .as_ref()
        .is_some_and(|s| !s.is_empty()));
    query.paging.continuation_token = first.continuation_token;
    let second = body(
        client
            .get_application_list(Request::new(query.clone()), deadline())
            .await?,
        generation,
    );
    assert_eq!(second.items.len(), 1);
    assert!(second.continuation_token.is_none());
    let app_page_lengths = [first.items.len(), second.items.len()];
    let applications: Vec<_> = first.items.into_iter().chain(second.items).collect();
    query.paging = Paging::default();
    assert_eq!(
        applications
            .iter()
            .map(|app| app.name.as_str())
            .collect::<BTreeSet<_>>(),
        journal
            .applications
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
    );
    for application in &applications {
        compare_application(
            application,
            observed_apps
                .iter()
                .find(|app| app["Name"] == application.name.as_str())
                .unwrap(),
        );
    }
    query.parameters.filter = ApplicationListFilter::Name(journal.applications[0].parse()?);
    query.parameters.exclude_application_parameters = true;
    let excluded = body(
        client
            .get_application_list(Request::new(query.clone()), deadline())
            .await?,
        generation,
    );
    assert_eq!(excluded.items.len(), 1);
    assert_eq!(excluded.items[0].name.as_str(), journal.applications[0]);
    assert!(excluded.items[0].parameters.is_empty());
    query.parameters.filter =
        ApplicationListFilter::Name(format!("{}-absent", journal.applications[0]).parse()?);
    assert!(body(
        client
            .get_application_list(Request::new(query), deadline())
            .await?,
        generation
    )
    .items
    .is_empty());

    let mut services = Vec::new();
    let mut service_page_lengths = Vec::new();
    for name in &journal.applications {
        let mut query = ServiceQueryDescription::new(ServiceQueryParameters::new(name.parse()?));
        query.paging.max_results = NonZeroU64::new(1);
        let first = body(
            client
                .get_service_list(Request::new(query.clone()), deadline())
                .await?,
            generation,
        );
        assert_eq!(first.items.len(), 1);
        assert!(first
            .continuation_token
            .as_ref()
            .is_some_and(|s| !s.is_empty()));
        query.paging.continuation_token = first.continuation_token;
        let second = body(
            client
                .get_service_list(Request::new(query.clone()), deadline())
                .await?,
            generation,
        );
        assert_eq!(second.items.len(), 1);
        assert!(second.continuation_token.is_none());
        service_page_lengths.push([first.items.len(), second.items.len()]);
        let actual: Vec<_> = first.items.into_iter().chain(second.items).collect();
        assert_eq!(
            actual
                .iter()
                .map(|service| service.name.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
        query.paging = Paging::default();
        for service in &actual {
            assert!(service.name.as_str().starts_with(&format!("{name}/")));
            compare_service(
                service,
                observed_services
                    .iter()
                    .find(|s| s["Name"] == service.name.as_str())
                    .unwrap(),
            );
        }
        services.extend(actual);
        query.parameters.filter = ServiceListFilter::TypeName("ListStateful".into());
        let selected = body(
            client
                .get_service_list(Request::new(query.clone()), deadline())
                .await?,
            generation,
        );
        assert_eq!(selected.items.len(), 1);
        assert_eq!(selected.items[0].kind, ServiceKind::Stateful);
        query.parameters.filter = ServiceListFilter::Name(format!("{name}/Stateless").parse()?);
        let selected = body(
            client
                .get_service_list(Request::new(query.clone()), deadline())
                .await?,
            generation,
        );
        assert_eq!(selected.items.len(), 1);
        assert_eq!(selected.items[0].kind, ServiceKind::Stateless);
        query.parameters.filter = ServiceListFilter::Name(format!("{name}/Absent").parse()?);
        assert!(body(
            client
                .get_service_list(Request::new(query), deadline())
                .await?,
            generation
        )
        .items
        .is_empty());
    }
    let missing = ServiceQueryDescription::new(ServiceQueryParameters::new(
        format!("{}-absent", journal.applications[0]).parse()?,
    ));
    let (error_code, error_origin) = furpc_support::failure(
        client
            .get_service_list(Request::new(missing), deadline())
            .await
            .unwrap_err(),
    );
    assert_eq!(error_code, -2147017763);
    let app_query = ApplicationQueryDescription::new(ApplicationQueryParameters {
        filter: ApplicationListFilter::TypeName(journal.type_name.clone()),
        ..Default::default()
    });
    let service_query = ServiceQueryDescription::new(ServiceQueryParameters::new(
        journal.applications[0].parse()?,
    ));
    let mut apps = client.clone();
    let mut list = client.clone();
    let (apps, list, ping) = tokio::try_join!(
        apps.get_application_list(Request::new(app_query), deadline()),
        list.get_service_list(Request::new(service_query), deadline()),
        client.ping(Request::new(()), deadline()),
    )?;
    assert_eq!(body(apps, generation).items.len(), 2);
    assert_eq!(body(list, generation).items.len(), 2);
    assert!(!body(ping, generation).node_name.is_empty());
    Ok(json!({
        "generation": generation, "native_start_ms": start_ms, "native_end_ms": furpc_support::now_ms(),
        "applications": applications.iter().map(application_evidence).collect::<Vec<_>>(),
        "services": services.iter().map(service_evidence).collect::<Vec<_>>(),
        "application_page_lengths": app_page_lengths, "service_page_lengths": service_page_lengths,
        "filters_and_parameter_exclusion": true, "typed_fields_match_http": true,
        "native_missing_application_code": error_code, "native_error_origin": error_origin,
        "same_generation_after_error": true, "mixed_concurrent_operations": 3,
    }))
}

async fn scenario(mut journal: Journal) -> Result<Value> {
    let path = output("MSSF_LIST_CONFIG")?;
    let report = output("MSSF_LIST_REPORT")?;
    if path == report {
        return Err("list journal and report must differ".into());
    }
    match std::fs::remove_file(&report) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let http = Http::new()?;
    let nodes = http.nodes().await?;
    let nodes = nodes["Items"]
        .as_array()
        .ok_or("missing independent nodes")?;
    if nodes.len() != 5 || nodes.iter().any(|node| node["NodeStatus"] != "Up") {
        return Err("requires independent five-node onebox".into());
    }
    let runtime = nodes[0]["CodeVersion"]
        .as_str()
        .ok_or("missing runtime")?
        .to_owned();
    http.prepare(&mut journal, &path).await?;
    http.wait_ready(&journal).await?;
    let observed_apps = http.applications(&journal).await?;
    let mut observed_services = vec![];
    for application in &journal.applications {
        observed_services.extend(http.services(application).await?);
    }
    let (mut reads_client, generation) = furpc_support::connect("127.0.0.1:19000").await?;
    let mut reads = observe_furpc(
        &mut reads_client,
        generation,
        &journal,
        &observed_apps,
        &observed_services,
    )
    .await?;
    let native_applications = reads
        .as_object_mut()
        .unwrap()
        .remove("applications")
        .unwrap();
    let native_services = reads.as_object_mut().unwrap().remove("services").unwrap();
    let channel = FabricClient::endpoint_clear("127.0.0.1:19000".parse()?)?
        .connect_timeout(Duration::from_secs(30))
        .connect()
        .await?;
    let client = FabricClient::new(
        channel,
        ClientOptions {
            default_timeout: Duration::from_secs(30),
            ..Default::default()
        },
    )?;
    let topology = topology::observe(&mut reads_client, generation, &http, &journal).await?;
    let deployment = deployment::observe(&mut reads_client, generation, &http, &journal).await?;
    let movement = topology::move_primary(
        &client,
        &mut reads_client,
        generation,
        &http,
        &journal,
        &topology,
    )
    .await?;
    client.close().await?;
    Ok(json!({
        "runtime_version": runtime, "archive_sha256": journal.archive_sha256,
        "native_source_commit": "3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d",
        "owned_applications": journal.applications, "owned_type": journal.type_name,
        "initial_absence_confirmed": true,
        "application_count": native_applications.as_array().unwrap().len(),
        "service_count": native_services.as_array().unwrap().len(),
        "application_native_continuation_followed": true, "service_native_continuation_followed": true,
        "filters_and_parameter_exclusion": true, "mixed_furpc_connection": true,
        "native_missing_application_code": reads["native_missing_application_code"],
        "reuse_after_native_error": true, "furpc": reads,
        "independent_applications": observed_apps, "independent_services": observed_services,
        "native_applications": native_applications, "native_services": native_services,
        "topology": topology, "deployment_inspection": deployment, "movement": movement,
        "qualification_limit": "volatile implicit-host stateful kind; not persisted application data replication",
    }))
}

#[tokio::test]
#[ignore = "owns two apps/type/upload on disposable onebox; requires MSSF_LIST_* and archive hash"]
async fn onebox_native_application_service_lists() -> Result<()> {
    let path = output("MSSF_LIST_CONFIG")?;
    if path == output("MSSF_LIST_REPORT")? {
        return Err("list journal and report must differ".into());
    }
    let journal = Journal::new(owner()?, std::env::var("MSSF_TEST_ARCHIVE_SHA256")?)?;
    // Reservation failure must not enter cleanup and adopt/delete a preexisting
    // same-owner journal. Only this invocation's newly reserved identity enters
    // the scenario/cleanup pair.
    journal.reserve(&path)?;
    let expected = journal.clone();
    let mut report = fixture::with_cleanup(scenario(journal), async {
        let current = Journal::load(&path, &owner()?)?;
        if current.applications != expected.applications
            || current.upload != expected.upload
            || current.type_name != expected.type_name
            || current.archive_sha256 != expected.archive_sha256
        {
            return Err("refusing replaced workload journal during cleanup".into());
        }
        oracle::cleanup(&output("MSSF_LIST_CONFIG")?, &owner()?, &Http::new()?).await
    })
    .await?;
    report["owned_cleanup_confirmed"] = json!(true);
    std::fs::write(
        output("MSSF_LIST_REPORT")?,
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(())
}
