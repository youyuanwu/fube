//! Self-contained native writes; HTTP observes and cleans only the pre-owned name.
use std::path::Path;

use fube_client::{DomainFailure, FurpcClient, FurpcClientError};
use serde_json::{json, Value};

#[path = "support/name_oracle.rs"]
mod oracle;
use oracle::*;
#[path = "support/fixture.rs"]
mod fixture;
#[path = "support/furpc.rs"]
mod furpc_support;
#[path = "support/rest.rs"]
mod rest;

async fn mutate(
    client: &mut FurpcClient,
    generation: u64,
    journal: &mut Journal,
    path: &Path,
    delete: bool,
    expected_error: Option<i64>,
) -> Result<Option<(i64, &'static str)>> {
    let request = furpc::Request::new(journal.name.parse()?);
    journal.record(path, Stage::Pending)?;
    let outcome = if delete {
        client.delete_name(request, furpc_support::deadline()).await
    } else {
        client.create_name(request, furpc_support::deadline()).await
    };
    settle(
        journal,
        path,
        outcome.map(|response| furpc_support::body(response, generation)),
        expected_error,
    )
}

fn settle(
    journal: &mut Journal,
    path: &Path,
    outcome: std::result::Result<(), FurpcClientError>,
    expected_error: Option<i64>,
) -> Result<Option<(i64, &'static str)>> {
    // Unknown errors deliberately keep Pending, including timeout/cancel/disconnect.
    // Even an observed absence cannot prove an abandoned create will not commit later.
    match outcome {
        Ok(()) => {
            journal.record(path, Stage::Settled)?;
            if expected_error.is_some() {
                return Err(
                    "native write unexpectedly succeeded instead of returning an error".into(),
                );
            }
            Ok(None)
        }
        Err(error) => {
            let (code, origin) = match &error {
                FurpcClientError::Domain(failure) => (failure.code, "domain"),
                FurpcClientError::Native(furpc::Error::RemoteFault(fault)) => {
                    (fault.code(), "native_fault")
                }
                _ => return Err(error.into()),
            };
            if Some(code) != expected_error || (code != DUPLICATE && code != MISSING) {
                return Err(error.into());
            }
            journal.record(path, Stage::Settled)?;
            Ok(Some((code, origin)))
        }
    }
}

async fn lifecycle() -> Result<Value> {
    let path = output("MSSF_NAME_CONFIG")?;
    let report = output("MSSF_NAME_REPORT")?;
    if path == report {
        return Err("name journal and report must differ".into());
    }
    match std::fs::remove_file(&report) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let http = Http::new()?;
    let runtime = http.runtime().await?;
    let mut journal = Journal::new(owner()?, std::env::var("MSSF_TEST_ARCHIVE_SHA256")?)?;
    journal.reserve(&path)?;
    confirm_absent(&mut journal, &path, &http).await?;
    let native_start_ms = furpc_support::now_ms();
    let (mut client, generation) = furpc_support::connect("127.0.0.1:19000").await?;
    let name: fube_client::NamingUri = journal.name.parse()?;
    assert!(!furpc_support::body(
        client
            .name_exists(furpc::Request::new(name.clone()), furpc_support::deadline())
            .await?,
        generation
    ));
    mutate(&mut client, generation, &mut journal, &path, false, None).await?;
    let http_present_after_create = http.exists(&journal.name).await?;
    assert!(http_present_after_create);
    assert!(furpc_support::body(
        client
            .name_exists(furpc::Request::new(name.clone()), furpc_support::deadline())
            .await?,
        generation
    ));
    let (duplicate, duplicate_origin) = mutate(
        &mut client,
        generation,
        &mut journal,
        &path,
        false,
        Some(DUPLICATE),
    )
    .await?
    .ok_or("duplicate create did not return a native error")?;
    assert!(furpc_support::body(
        client
            .name_exists(furpc::Request::new(name.clone()), furpc_support::deadline())
            .await?,
        generation
    ));
    mutate(&mut client, generation, &mut journal, &path, true, None).await?;
    let http_present_after_delete = http.exists(&journal.name).await?;
    assert!(!http_present_after_delete);
    assert!(!furpc_support::body(
        client
            .name_exists(furpc::Request::new(name.clone()), furpc_support::deadline())
            .await?,
        generation
    ));
    let (missing, missing_origin) = mutate(
        &mut client,
        generation,
        &mut journal,
        &path,
        true,
        Some(MISSING),
    )
    .await?
    .ok_or("missing delete did not return a native error")?;
    let mut ping = client.clone();
    let mut exists = client.clone();
    let deadline = furpc_support::deadline();
    let (first, gateway, second) = tokio::try_join!(
        client.name_exists(furpc::Request::new(name.clone()), deadline),
        ping.ping(furpc::Request::new(()), deadline),
        exists.name_exists(furpc::Request::new(name.clone()), deadline),
    )?;
    assert!(!furpc_support::body(first, generation));
    furpc_support::body(gateway, generation);
    assert!(!furpc_support::body(second, generation));
    mutate(&mut client, generation, &mut journal, &path, false, None).await?;
    mutate(&mut client, generation, &mut journal, &path, true, None).await?;
    assert!(!furpc_support::body(
        client
            .name_exists(furpc::Request::new(name), furpc_support::deadline())
            .await?,
        generation
    ));
    furpc_support::body(
        client
            .ping(furpc::Request::new(()), furpc_support::deadline())
            .await?,
        generation,
    );
    let final_http_present = http.exists(&journal.name).await?;
    assert!(!final_http_present);
    Ok(json!({
        "profile": "clear",
        "runtime_version": runtime,
        "archive_sha256": journal.archive_sha256,
        "owned_name": journal.name,
        "initial_http_absent": true,
        "native_create_succeeded": true,
        "http_present_after_create": true,
        "duplicate_create_code": duplicate,
        "reuse_after_duplicate": true,
        "native_delete_succeeded": true,
        "http_absent_after_delete": true,
        "missing_delete_code": missing,
        "reuse_after_missing": true,
        "writes_share_native_client": true,
        "furpc": {
            "generation": generation,
            "same_generation_after_error": true,
            "mixed_concurrent_operations": 3,
            "duplicate_error_origin": duplicate_origin,
            "missing_error_origin": missing_origin,
            "http_present_after_create": http_present_after_create,
            "http_present_after_delete": http_present_after_delete,
            "final_http_present": final_http_present,
            "native_start_ms": native_start_ms,
            "native_end_ms": furpc_support::now_ms(),
        },
    }))
}

#[tokio::test]
#[ignore = "owns a unique name on a disposable onebox; requires MSSF_NAME_* and archive hash"]
async fn onebox_native_name_lifecycle() -> Result<()> {
    let mut report = fixture::with_cleanup(lifecycle(), async {
        cleanup(&output("MSSF_NAME_CONFIG")?, &owner()?, &Http::new()?).await
    })
    .await?;
    // Never publish stale/partial success before owned cleanup has completed.
    report["owned_cleanup_confirmed"] = json!(true);
    std::fs::write(
        output("MSSF_NAME_REPORT")?,
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(())
}

#[test]
fn only_conclusive_success_or_expected_native_rejections_settle_ownership() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    for (outcome, expected, settled, accepted) in [
        (
            Err(FurpcClientError::Domain(DomainFailure {
                code: DUPLICATE,
                message: "unexpected duplicate".into(),
            })),
            None,
            false,
            false,
        ),
        (
            Err(FurpcClientError::Domain(DomainFailure {
                code: MISSING,
                message: "wrong rejection".into(),
            })),
            Some(DUPLICATE),
            false,
            false,
        ),
        (
            Err(FurpcClientError::Native(furpc::Error::Timeout("gateway"))),
            None,
            false,
            false,
        ),
        (
            Err(FurpcClientError::Protocol("malformed success")),
            None,
            false,
            false,
        ),
        (
            Err(FurpcClientError::Decode(fubst::Error::Protocol(
                "nonempty name write success",
            ))),
            None,
            false,
            false,
        ),
        (
            Err(FurpcClientError::Domain(DomainFailure {
                code: DUPLICATE,
                message: "expected duplicate".into(),
            })),
            Some(DUPLICATE),
            true,
            true,
        ),
        (
            Err(FurpcClientError::Domain(DomainFailure {
                code: MISSING,
                message: "expected missing".into(),
            })),
            Some(MISSING),
            true,
            true,
        ),
        (Ok(()), None, true, true),
        (Ok(()), Some(DUPLICATE), true, false),
    ] {
        let directory = Directory(std::path::PathBuf::from(format!(
            "name-outcome-{}",
            uuid::Uuid::new_v4()
        )));
        std::fs::create_dir(&directory.0).unwrap();
        let path = directory.0.join("journal.json");
        let mut journal = Journal::new("test-owner".into(), "a".repeat(64)).unwrap();
        journal.reserve(&path).unwrap();
        journal.record(&path, Stage::Absent).unwrap();
        journal.record(&path, Stage::Pending).unwrap();
        assert_eq!(
            settle(&mut journal, &path, outcome, expected).is_ok(),
            accepted
        );
        assert_eq!(
            Journal::load(&path, "test-owner").unwrap().stage,
            if settled {
                Stage::Settled
            } else {
                Stage::Pending
            },
        );
    }
}
