//! Real nonempty native queries against an independently prepared owned workload.
use mssf_client::*;
use serde_json::{json, Value};
use std::{collections::BTreeSet, num::NonZeroU64, time::Duration};

#[path = "support/list_fixture.rs"]
mod oracle;
use oracle::{output, owner, Http, Journal, Result};
#[path = "support/fixture.rs"]
mod fixture;
#[path = "support/rest.rs"]
mod rest;

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
    assert!(observed["Parameters"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["Key"] == "Evidence" && p["Value"] == "owned-list-value"));
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
        ServiceKind::Stateless => assert_eq!(observed["ServiceKind"], "Stateless"),
        ServiceKind::Stateful {
            has_persisted_state,
        } => {
            assert_eq!(observed["ServiceKind"], "Stateful");
            assert_eq!(has_persisted_state, observed["HasPersistedState"]);
        }
    }
}

async fn scenario(mut journal: Journal) -> Result<Value> {
    let path = output("MSSF_LIST_CONFIG")?;
    let report = output("MSSF_LIST_REPORT")?;
    if path == report {
        return Err("list journal and report must differ".into());
    }
    match std::fs::remove_file(&report) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let http = Http::new()?;
    let nodes = http.nodes().await?;
    let nodes = nodes["Items"]
        .as_array()
        .ok_or("missing independent node list")?;
    if nodes.len() != 5 || nodes.iter().any(|n| n["NodeStatus"] != "Up") {
        return Err("requires independent five-node onebox".into());
    }
    let runtime = nodes[0]["CodeVersion"]
        .as_str()
        .ok_or("missing runtime version")?
        .to_string();
    http.prepare(&mut journal, &path).await?;
    http.wait_ready(&journal).await?;
    let observed_apps = http.applications(&journal).await?;
    let client =
        FabricClient::connect_clear("127.0.0.1:19000".parse()?, Duration::from_secs(30)).await?;
    let mut app_query = ApplicationQueryDescription {
        parameters: ApplicationQueryParameters {
            filter: ApplicationListFilter::TypeName(journal.type_name.clone()),
            ..Default::default()
        },
        paging: Paging {
            max_results: NonZeroU64::new(1),
            ..Default::default()
        },
    };
    let first = client.get_application_list(&app_query).await?;
    assert_eq!(first.items.len(), 1);
    assert!(
        first.continuation_token.is_some(),
        "two owned apps must force a real native token"
    );
    app_query.paging.continuation_token = first.continuation_token.clone();
    let second = client
        .get_application_list_with_timeout(&app_query, Duration::from_secs(30))
        .await?;
    assert_eq!(second.items.len(), 1);
    assert!(second.continuation_token.is_none());
    let combined: Vec<_> = first.items.into_iter().chain(second.items).collect();
    assert_eq!(
        combined
            .iter()
            .map(|a| a.name.as_str())
            .collect::<BTreeSet<_>>(),
        journal.applications.iter().map(String::as_str).collect()
    );
    for app in &combined {
        compare_application(
            app,
            observed_apps
                .iter()
                .find(|a| a["Name"] == app.name.as_str())
                .unwrap(),
        );
    }
    app_query.paging.continuation_token = None;
    app_query.paging.max_results = None;
    app_query.parameters.exclude_application_parameters = true;
    let excluded = client.get_application_list(&app_query).await?;
    assert_eq!(excluded.items.len(), 2);
    assert!(excluded.items.iter().all(|a| a.parameters.is_empty()));
    app_query.parameters.filter = ApplicationListFilter::Name(journal.applications[0].parse()?);
    assert_eq!(
        client.get_application_list(&app_query).await?.items.len(),
        1
    );
    app_query.parameters.filter =
        ApplicationListFilter::Name(format!("{}-absent", journal.applications[0]).parse()?);
    assert!(client
        .get_application_list(&app_query)
        .await?
        .items
        .is_empty());
    app_query.parameters.filter = ApplicationListFilter::DefinitionKind(
        ApplicationDefinitionKindFilter::SERVICE_FABRIC_APPLICATION_DESCRIPTION,
    );
    let definitions = client.get_application_list(&app_query).await?;
    assert!(journal
        .applications
        .iter()
        .all(|name| definitions.items.iter().any(|a| a.name.as_str() == name)));

    let mut service_observations = Vec::new();
    let mut native_services = Vec::new();
    for name in &journal.applications {
        let observed = http.services(name).await?;
        let mut query = ServiceQueryDescription::new(ServiceQueryParameters::new(name.parse()?));
        query.paging.max_results = NonZeroU64::new(1);
        let first = client.get_service_list(&query).await?;
        assert_eq!(first.items.len(), 1);
        assert!(first.continuation_token.is_some());
        query.paging.continuation_token = first.continuation_token.clone();
        let second = client
            .get_service_list_with_timeout(&query, Duration::from_secs(30))
            .await?;
        assert_eq!(second.items.len(), 1);
        assert!(second.continuation_token.is_none());
        let services: Vec<_> = first.items.into_iter().chain(second.items).collect();
        assert_eq!(
            services
                .iter()
                .map(|s| &s.name)
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
        assert!(services.iter().any(|s| s.kind == ServiceKind::Stateless));
        assert!(services.iter().any(|s| s.kind
            == ServiceKind::Stateful {
                has_persisted_state: false
            }));
        for service in &services {
            compare_service(
                service,
                observed
                    .iter()
                    .find(|s| s["Name"] == service.name.as_str())
                    .unwrap(),
            );
            native_services.push(json!({
                "name": service.name, "type_name": service.type_name,
                "manifest_version": service.manifest_version, "kind": format!("{:?}", service.kind),
                "status": format!("{:?}", service.status), "health_state": format!("{:?}", service.health_state),
                "is_service_group": service.is_service_group,
            }));
        }
        query.paging.continuation_token = None;
        query.paging.max_results = None;
        query.parameters.filter = ServiceListFilter::TypeName("ListStateful".into());
        assert_eq!(client.get_service_list(&query).await?.items.len(), 1);
        query.parameters.filter = ServiceListFilter::Name(format!("{name}/Stateless").parse()?);
        assert_eq!(client.get_service_list(&query).await?.items.len(), 1);
        query.parameters.filter = ServiceListFilter::Name(format!("{name}/Absent").parse()?);
        assert!(client.get_service_list(&query).await?.items.is_empty());
        service_observations.extend(observed);
    }
    let app_query = ApplicationQueryDescription::new(ApplicationQueryParameters {
        filter: ApplicationListFilter::TypeName(journal.type_name.clone()),
        ..Default::default()
    });
    let service_query = ServiceQueryDescription::new(ServiceQueryParameters::new(
        journal.applications[0].parse()?,
    ));
    let node_query = NodeQueryDescription::default();
    let (a, s, n, p) = tokio::join!(
        client.get_application_list(&app_query),
        client.get_service_list(&service_query),
        client.get_node_list(&node_query),
        client.ping()
    );
    assert_eq!(a?.items.len(), 2);
    assert_eq!(s?.items.len(), 2);
    assert_eq!(n?.items.len(), 5);
    p?;
    let missing = client
        .get_service_list(&ServiceQueryDescription::new(ServiceQueryParameters::new(
            format!("{}-absent", journal.applications[0]).parse()?,
        )))
        .await;
    let code = match missing {
        Err(Error::Remote { code, .. }) => code,
        other => {
            return Err(format!("missing application must remain native error: {other:?}").into())
        }
    };
    assert!(!client.is_closed());
    assert_eq!(
        client.get_application_list(&app_query).await?.items.len(),
        2
    );
    client.ping().await?;
    client.close().await?;
    Ok(json!({
        "runtime_version": runtime, "archive_sha256": journal.archive_sha256,
        "native_source_commit": "3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d",
        "owned_applications": journal.applications, "owned_type": journal.type_name,
        "initial_absence_confirmed": true, "application_count": 2, "service_count": 4,
        "application_native_continuation_followed": true, "service_native_continuation_followed": true,
        "filters_and_parameter_exclusion": true, "mixed_shared_native_connection": true,
        "native_missing_application_code": code, "reuse_after_native_error": true,
        "independent_applications": observed_apps, "independent_services": service_observations,
        "native_applications": combined.iter().map(|app| json!({
            "name": app.name, "type_name": app.type_name, "type_version": app.type_version,
            "status": format!("{:?}", app.status), "health_state": format!("{:?}", app.health_state),
            "parameters": app.parameters, "definition_kind": format!("{:?}", app.definition_kind),
        })).collect::<Vec<_>>(),
        "native_services": native_services,
        "qualification_limit": "volatile implicit-host stateful kind; not persisted application data replication"
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
