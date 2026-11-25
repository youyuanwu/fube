use super::*;
use tokio::io::{AsyncWriteExt, BufReader};

#[test]
fn typed_node_controls_round_trip_and_cannot_change_launch_authority() {
    let launch = crate::tests::launches()[0].clone();
    for control in [
        NodeControl::Admit {},
        NodeControl::Stop {},
        NodeControl::DialPeer {
            slot: 0,
            address: "[::1]:65535".parse().unwrap(),
        },
        NodeControl::ReplaceHost {
            replica: i64::MAX,
            instance: i64::MAX,
        },
        NodeControl::PeerDown {
            slot: usize::MAX,
            instance: u64::MAX,
        },
        NodeControl::PeerUpdate {
            slot: usize::MAX,
            instance: u64::MAX,
        },
        NodeControl::CreateIdle {},
        NodeControl::BuildIdle {},
        NodeControl::RemoveIdle {},
    ] {
        let bytes = frame(
            &NodeRequest::new(&launch, control.clone()),
            NODE_CONTROL_LIMIT,
        )
        .unwrap();
        let request: NodeRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(request.authorize(&launch).unwrap(), control);
    }
    let bytes = frame(
        &NodeRequest::new(&launch, NodeControl::Stop {}),
        NODE_CONTROL_LIMIT,
    )
    .unwrap();
    for change_session in [true, false] {
        let mut wrong_owner = launch.clone();
        if change_session {
            wrong_owner.session = uuid::Uuid::new_v4().to_string();
        } else {
            wrong_owner.capability = uuid::Uuid::new_v4().to_string();
        }
        let request: NodeRequest = serde_json::from_slice(&bytes).unwrap();
        assert!(request.authorize(&wrong_owner).is_err());
    }
}

#[tokio::test]
async fn typed_node_reader_preserves_partial_and_batched_frames_across_cancellation() {
    let launch = crate::tests::launches()[0].clone();
    let first = frame(
        &NodeRequest::new(&launch, NodeControl::Stop {}),
        NODE_CONTROL_LIMIT,
    )
    .unwrap();
    let second = frame(
        &NodeRequest::new(&launch, NodeControl::Admit {}),
        NODE_CONTROL_LIMIT,
    )
    .unwrap();
    let (mut writer, reader) = tokio::io::duplex(2 * NODE_CONTROL_LIMIT);
    let mut controls = NodeReader::new(BufReader::new(reader));
    let split = first.len() / 2;
    writer.write_all(&first[..split]).await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(5), controls.next(&launch))
            .await
            .is_err()
    );
    writer.write_all(&first[split..]).await.unwrap();
    writer.write_all(&second).await.unwrap();
    drop(writer);
    assert_eq!(
        controls.next(&launch).await.unwrap(),
        Some(NodeControl::Stop {})
    );
    assert_eq!(
        controls.next(&launch).await.unwrap(),
        Some(NodeControl::Admit {})
    );
    assert!(controls.next(&launch).await.unwrap().is_none());
}

#[tokio::test]
async fn typed_node_reader_rejects_malformed_schema_truncation_and_oversized_frames() {
    let launch = crate::tests::launches()[0].clone();
    let base = serde_json::to_value(NodeRequest::new(
        &launch,
        NodeControl::PeerUpdate {
            slot: 0,
            instance: 1,
        },
    ))
    .unwrap();
    for invalid in 0..6 {
        let mut value = base.clone();
        match invalid {
            0 => value["operation"]["action"] = "unknown".into(),
            1 => {
                value["operation"].as_object_mut().unwrap().remove("slot");
            }
            2 => value["operation"]["slot"] = (-1).into(),
            3 => value["extra"] = true.into(),
            4 => value["operation"]["extra"] = true.into(),
            _ => value["operation"] = serde_json::json!({"action":"stop","extra":true}),
        }
        let bytes = frame(&value, NODE_CONTROL_LIMIT).unwrap();
        let mut controls = NodeReader::new(BufReader::new(bytes.as_slice()));
        assert!(controls.next(&launch).await.is_err());
    }
    let duplicate = format!(
        "{{\"session\":\"{}\",\"session\":\"{}\",\"capability\":\"{}\",\"operation\":{{\"action\":\"stop\"}}}}\n",
        launch.session, launch.session, launch.capability,
    );
    let mut controls = NodeReader::new(BufReader::new(duplicate.as_bytes()));
    assert!(controls.next(&launch).await.is_err());

    let mut bytes = frame(
        &NodeRequest::new(&launch, NodeControl::Stop {}),
        NODE_CONTROL_LIMIT,
    )
    .unwrap();
    bytes.pop();
    let mut controls = NodeReader::new(BufReader::new(bytes.as_slice()));
    assert!(controls
        .next(&launch)
        .await
        .unwrap_err()
        .contains("truncated"));
    let bytes = vec![b'x'; NODE_CONTROL_LIMIT + 1];
    let mut controls = NodeReader::new(BufReader::new(bytes.as_slice()));
    assert!(controls
        .next(&launch)
        .await
        .unwrap_err()
        .contains("256 bytes"));
    let mut oversized = NodeRequest::new(&launch, NodeControl::Stop {});
    oversized.capability = "x".repeat(NODE_CONTROL_LIMIT);
    assert!(frame(&oversized, NODE_CONTROL_LIMIT).is_err());
}
