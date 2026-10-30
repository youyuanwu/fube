//! Independent native-source expectations, NOT captures or deployed qualification.
//! Native @3988b45: HealthReport.cpp:1318-1353; *EntityHealthInformation.h
//! FABRIC_FIELDS_02/03/04; HealthReport.h; LoadOrMoveCostDescription.h:89,
//! .cpp:121-141; ReplicaRole.cpp:54-83; LocalLoadReportingComponent.cpp:204-232.
use super::*;
use fube_transport::serialization::{Decoder, Encoder};

pub(super) const PARTITION: &str = "00112233-4455-6677-8899-aabbccddeeff";
pub(super) async fn without_advancing_time<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::pin!(future);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(value) = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(match future.as_mut().poll(cx) {
                std::task::Poll::Ready(value) => Some(value),
                std::task::Poll::Pending => None,
            })
        })
        .await
        {
            return value;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "report must progress without advancing a reporting interval"
        );
        tokio::task::yield_now().await;
    }
}
pub(super) const GUID_BYTES: [u8; 16] = [
    0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
];
pub(super) fn information() -> crate::HealthInformation {
    crate::HealthInformation {
        source_id: "User".into(),
        property: "Ready".into(),
        time_to_live_seconds: 0,
        state: crate::HealthState::Warning,
        description: "detail".into(),
        sequence_number: 45,
        remove_when_expired: true,
    }
}
pub(super) fn assert_fault(
    message: &Message,
    configuration: i64,
    data_loss: i64,
    incarnation: i64,
    fault: i64,
) {
    let p = message.properties().unwrap();
    assert_eq!((p.actor, p.action.as_str()), (12, "RAReportFault"));
    let mut d = Decoder::new(&message.body);
    d.object(|d| {
        d.object(|d| {
            d.object(|d| {
                assert_eq!(d.guid()?, GUID_BYTES);
                assert!(d.is_scope_end()?);
                Ok(())
            })?;
            d.object(|d| {
                d.object(|d| {
                    assert_eq!(d.guid()?, GUID_BYTES);
                    Ok(())
                })?;
                assert_eq!(d.i64()?, 0);
                assert_eq!(d.i64()?, 0);
                assert_eq!(d.string()?, "");
                assert_eq!(d.i64()?, 1);
                assert!(d.is_scope_end()?);
                Ok(())
            })?;
            d.object(|d| {
                // Native epoch field order is configuration BEFORE data loss.
                assert_eq!(d.i64()?, configuration);
                assert_eq!(d.i64()?, data_loss);
                assert!(d.is_scope_end()?);
                Ok(())
            })?;
            d.object(|d| {
                assert_eq!(d.i64()?, 0);
                assert_eq!(d.i64()?, 0);
                assert!(d.is_scope_end()?);
                Ok(())
            })?;
            assert_eq!(d.i32()?, 1);
            assert_eq!(d.i32()?, 1);
            assert!(d.is_scope_end()?);
            Ok(())
        })?;
        d.object(|d| {
            d.object(|d| {
                assert_eq!(d.byte_array()?, vec![1; 16]);
                assert_eq!(d.u64()?, 3);
                assert!(d.is_scope_end()?);
                Ok(())
            })?;
            assert_eq!(d.i64()?, 100);
            assert_eq!(d.i64()?, incarnation);
            assert_eq!(d.i64()?, 4);
            assert_eq!(d.i64()?, 0);
            assert!(d.boolean()?);
            assert_eq!(d.i64()?, -1);
            assert_eq!(d.i64()?, -1);
            assert_eq!(d.i64()?, 2);
            // Version is native lifecycle metadata, not changed by reporting.
            d.object(|_| Ok(()))?;
            d.string()?;
            d.string()?;
            assert!(d.is_scope_end()?);
            Ok(())
        })?;
        assert_eq!(d.i64()?, fault);
        d.object(|d| {
            d.object(|d| {
                assert_ne!(d.guid()?, [0; 16]);
                assert_eq!(d.u64()?, 0);
                assert!(d.is_scope_end()?);
                Ok(())
            })?;
            assert_eq!(d.i64()?, 2);
            assert!(d.is_scope_end()?);
            Ok(())
        })?;
        assert!(d.is_scope_end()?);
        Ok(())
    })
    .unwrap();
    d.finish().unwrap();
}
pub(super) fn key(kind: i64) -> String {
    if kind == 3 {
        format!("{PARTITION}+Ready")
    } else {
        format!("{PARTITION}+100+Ready")
    }
}
fn hex(s: &str) -> Vec<u8> {
    s.split_whitespace()
        .map(|b| u8::from_str_radix(b, 16).unwrap())
        .collect()
}
pub(super) fn assert_health(message: &Message, kind: i64, ttl: i64, sequence: Option<i64>) -> i64 {
    let properties = message.properties().unwrap();
    assert_eq!(
        (properties.actor, properties.action.as_str()),
        (7, "ReportHealth")
    );
    // Independent literal entity scopes: kind, GUID (native LE), public ID,
    // then invalid replica-instance -1 ONLY for kind 1. No RAP incarnation.
    let fields = match kind {
        1 => "09 01 0c 33 22 11 00 55 44 77 66 88 99 aa bb cc dd ee ff 09 80 64 09 7f",
        2 => "09 02 0c 33 22 11 00 55 44 77 66 88 99 aa bb cc dd ee ff 09 80 64",
        3 => "09 03 0c 33 22 11 00 55 44 77 66 88 99 aa bb cc dd ee ff",
        _ => unreachable!(),
    };
    let fields = hex(fields);
    let mut entity = vec![1, 0]; // pointer, object
    entity.extend_from_slice(&((fields.len() + 16) as u32).to_le_bytes());
    entity.extend_from_slice(&[1, 0, 0, 0, 4, kind as u8, 0, 0, 0, 0x1f]);
    entity.extend_from_slice(&fields);
    entity.extend_from_slice(&[0x2f, 0x3f]);
    assert!(message
        .body
        .windows(entity.len())
        .any(|bytes| bytes == entity));
    let mut d = Decoder::new(&message.body);
    let reports = d
        .object(|d| {
            let rows = d.object_array(|d| {
                d.pointer(|d, tag| {
                    assert_eq!(tag, &(kind as i32).to_le_bytes());
                    assert_eq!(d.i64()?, kind);
                    assert_eq!(d.guid()?, GUID_BYTES);
                    if kind != 3 {
                        assert_eq!(d.i64()?, 100);
                    }
                    if kind == 1 {
                        assert_eq!(d.i64()?, -1);
                    }
                    assert!(d.is_scope_end()?); // exact entity field count
                    Ok(())
                })?
                .unwrap();
                assert_eq!(d.string()?, "User");
                assert_eq!(d.string()?, "Ready");
                assert_eq!(d.i64()?, ttl);
                assert_eq!(d.i64()?, 2);
                assert_eq!(d.string()?, "detail");
                let actual = d.i64()?;
                if let Some(expected) = sequence {
                    assert_eq!(actual, expected);
                } else {
                    assert!(actual > 0);
                }
                assert!(d.i64()? > 116_444_736_000_000_000);
                d.object(|d| {
                    assert!(d.object_array(|_| Ok(()))?.is_empty());
                    assert!(d.is_scope_end()?);
                    Ok(())
                })?;
                assert!(d.boolean()?);
                assert!(d.is_scope_end()?);
                Ok(actual)
            })?;
            assert!(d.object_array(|_| Ok(()))?.is_empty());
            assert!(d.is_scope_end()?);
            Ok(rows)
        })
        .unwrap();
    d.finish().unwrap();
    assert_eq!(reports.len(), 1);
    reports[0]
}
pub(super) async fn health_reply(
    h: &mut Harness,
    message: &Message,
    kind: i64,
    sequence: i64,
    wrong: Option<usize>,
) {
    // Native HealthReportResult: kind/source/sequence/entity+property/error.
    // Construct independently with the low-level codec, not runtime reply DTOs.
    let mut e = Encoder::new();
    let expected_key = key(kind);
    e.object(|e| {
        e.object_array(&[()], |e, _| {
            e.i64(if wrong == Some(0) {
                if kind == 3 {
                    2
                } else {
                    3
                }
            } else {
                kind
            });
            e.string(if wrong == Some(1) { "Other" } else { "User" })?;
            e.i64(sequence + i64::from(wrong == Some(2)));
            e.string(if wrong == Some(3) {
                "wrong+100+Ready"
            } else if wrong == Some(4) {
                "00112233-4455-6677-8899-aabbccddeeff+101+Ready"
            } else if wrong == Some(5) {
                "00112233-4455-6677-8899-aabbccddeeff+100+Other"
            } else {
                &expected_key
            })?;
            e.i64(if wrong == Some(6) { -123 } else { 0 });
            Ok(())
        })?;
        e.object_array::<()>(&[], |_, _| Ok(()))
    })
    .unwrap();
    write_message(
        &mut h.peer,
        &Message {
            headers: vec![
                Header::actor(16).unwrap(),
                Header::action("OperationSuccessAction").unwrap(),
                Header::relates_to(message.properties().unwrap().id).unwrap(),
            ],
            body: e.finish(),
        },
    )
    .await
    .unwrap();
}
pub(super) fn assert_load(
    message: &Message,
    stateful: bool,
    primary: bool,
    metric: &str,
    value: u32,
) {
    assert_eq!(
        (
            message.properties().unwrap().actor,
            message.properties().unwrap().action
        ),
        (12, "RAPReportLoad".into())
    );
    let mut d = Decoder::new(&message.body);
    let mut stamp = 0;
    d.object(|d| {
        assert!(d.object_array(|_| Ok(()))?.is_empty());
        let reports = d.object_array(|d| {
            assert_eq!(d.guid()?, GUID_BYTES);
            assert_eq!(d.string()?, "fabric:/Model/Service");
            assert_eq!(d.boolean()?, stateful);
            for is_primary in [true, false] {
                let rows = d.object_array(|d| {
                    assert_eq!(d.string()?, metric);
                    assert_eq!(d.u32()?, value);
                    assert_eq!(d.u32()?, 1);
                    assert_eq!(d.f64()?, f64::from(value));
                    assert_eq!(d.f64()?, 1.0);
                    let time = d.i64()?;
                    assert!(time > 0);
                    stamp = time;
                    assert!(d.is_scope_end()?);
                    Ok(())
                })?;
                assert_eq!(rows.len(), usize::from(primary == is_primary));
            }
            let nodes = d.object_array(|d| {
                assert_eq!(d.byte_array()?, vec![1; 16]);
                let rows = d.object_array(|d| {
                    assert_eq!(d.string()?, metric);
                    assert_eq!(d.u32()?, value);
                    assert_eq!(d.u32()?, 1);
                    assert_eq!(d.f64()?, f64::from(value));
                    assert_eq!(d.f64()?, 1.0);
                    assert_eq!(d.i64()?, stamp);
                    assert!(d.is_scope_end()?);
                    Ok(())
                })?;
                assert_eq!(rows.len(), 1);
                assert!(d.is_scope_end()?);
                Ok(())
            })?;
            assert_eq!(nodes.len(), usize::from(!primary));
            assert!(d.is_scope_end()?);
            Ok(())
        })?;
        assert_eq!(reports.len(), 1);
        assert_eq!(d.i64()?, stamp);
        assert!(d.is_scope_end()?);
        Ok(())
    })
    .unwrap();
    d.finish().unwrap();
}

#[tokio::test]
async fn canonical_stateless_reporting_native_literals_immediate_and_per_record_results() {
    let mut h = Harness::new().await;
    let mut request = h.request(1, 17, 0);
    request.unit.id.id = Guid(GUID_BYTES);
    request.unit.consistency.id.id = Guid(GUID_BYTES);
    request.service.metrics.0.push(Metric {
        name: "Work".into(),
        weight: 1,
        primary: 0,
        secondary: 0,
    });
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    let integration = h
        .calls
        .provider
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .capture(PARTITION.parse().unwrap(), 100)
        .unwrap();
    for (metric, value) in [("Work", 731), ("_MoveCost_", 3)] {
        let (partition, observation) = integration.observe_report();
        let call = h
            .engine
            .shared
            .execution
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                let result: Result<()> = if metric == "Work" {
                    partition.report_load(&[crate::LoadMetric {
                        name: metric.into(),
                        value,
                    }])
                } else {
                    partition.report_move_cost(crate::MoveCost::High)
                };
                result
            });
        let (result, message) = tokio::join!(call, h.receive("RAPReportLoad"));
        result.unwrap();
        assert_load(&message, false, false, metric, value);
        assert!(observation
            .result()
            .unwrap()
            .unwrap()
            .health_sequence
            .is_none());
    }
    let mut records = Vec::new();
    for kind in [2, 3] {
        for option in [None, Some(false), Some(true)] {
            let (partition, observation) = integration.observe_report();
            let reuse = partition.clone();
            assert!(observation.result().is_none());
            let call = h
                .engine
                .shared
                .execution
                .try_reserve()
                .unwrap()
                .run_owned(move || {
                    let result: Result<()> = match (kind, option) {
                        (2, None) => partition.report_instance_health(&information()),
                        (3, None) => partition.report_partition_health(&information()),
                        (2, Some(immediate)) => partition.report_instance_health2(
                            &information(),
                            Some(&crate::HealthReportSendOption { immediate }),
                        ),
                        (3, Some(immediate)) => partition.report_partition_health2(
                            &information(),
                            Some(&crate::HealthReportSendOption { immediate }),
                        ),
                        _ => unreachable!(),
                    };
                    result
                });
            tokio::pin!(call);
            // Paused scheduling: emission must progress WITHOUT advancing time.
            tokio::time::pause();
            let before = tokio::time::Instant::now();
            let message = without_advancing_time(h.receive("ReportHealth")).await;
            assert_eq!(tokio::time::Instant::now(), before);
            assert_health(&message, kind, i64::MAX, Some(45));
            poll_pending_once(call.as_mut()).await;
            assert!(observation.result().is_none()); // immediate is NOT HM success
            health_reply(&mut h, &message, kind, 45, None).await;
            without_advancing_time(call).await.unwrap();
            tokio::time::resume();
            assert_eq!(
                observation.result().unwrap().unwrap().health_sequence,
                Some(45)
            );
            let result = h
                .engine
                .shared
                .execution
                .try_reserve()
                .unwrap()
                .run_owned(move || reuse.report_instance_health(&information()))
                .await;
            assert_eq!(result.unwrap_err().kind(), crate::ErrorKind::InvalidState);
            records.push(observation);
        }
    }
    for record in records {
        assert_eq!(record.result().unwrap().unwrap().health_sequence, Some(45));
    }
    h.no_reply().await; // no hidden health retries
    let reporting = integration.partition.reporting.clone();
    assert!(reporting
        .health(&health_report(), crate::reporting::HealthTarget::Replica)
        .await
        .is_err());
    // Two specific records: an overloaded second call must not replace the
    // first call's pending/result cell or steal its receipt.
    let (first, first_record) = integration.observe_report();
    let (second, second_record) = integration.observe_report();
    let first_call = h
        .engine
        .shared
        .execution
        .try_reserve()
        .unwrap()
        .run_owned(move || first.report_partition_health(&information()));
    let message = h.receive("ReportHealth").await;
    let second_error = h
        .engine
        .shared
        .execution
        .try_reserve()
        .unwrap()
        .run_owned(move || second.report_instance_health(&information()))
        .await
        .unwrap_err();
    assert_eq!(second_error.kind(), crate::ErrorKind::ResourceExhausted);
    assert!(second_record.result().unwrap().is_err());
    assert!(first_record.result().is_none());
    // An unrelated transport correlation cannot finish the held call.
    let mut unrelated = message.clone();
    unrelated.headers = vec![Header::message_id(MessageId {
        guid: [0x99; 16],
        index: 999,
    })
    .unwrap()];
    health_reply(&mut h, &unrelated, 3, 45, None).await;
    assert!(first_record.result().is_none());
    health_reply(&mut h, &message, 3, 45, None).await;
    first_call.await.unwrap();
    assert_eq!(
        first_record.result().unwrap().unwrap().health_sequence,
        Some(45)
    );
    assert!(second_record.result().unwrap().is_err());
    let (partition, record) = integration.observe_report();
    let call = h
        .engine
        .shared
        .execution
        .try_reserve()
        .unwrap()
        .run_owned(move || partition.report_fault(crate::FaultType::Transient));
    let (result, message) = tokio::join!(call, h.receive("RAReportFault"));
    result.unwrap();
    assert_fault(&message, 1, 1, 17, 1);
    assert_eq!(record.result().unwrap().unwrap().fault.unwrap().attempts, 1);
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    let (partition, observation) = integration.observe_report();
    let error = h
        .engine
        .shared
        .execution
        .try_reserve()
        .unwrap()
        .run_owned(move || partition.report_partition_health(&information()))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::ObjectClosed);
    assert!(observation.result().unwrap().is_err());
    h.no_reply().await;
}
