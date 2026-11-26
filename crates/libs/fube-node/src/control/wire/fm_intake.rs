//! Budgeted FM bodies; generation and domain admission stay in Ingress.
use super::{err, one_way, peer_intake::Payload, Headers, Properties, Result};
#[cfg(test)]
use fube_ra::protocol::NativeReplicaRole;
use fube_ra::protocol::{
    AddInstance, AddInstanceReply, AddPrimary, AddReplica, DeleteReplica, DoReconfiguration,
    GenerationHeader, GenerationUpdate, NodeActivateRequest, NodeDeactivateRequest,
    NodeUpdateServiceRequest, RemoveInstance, ReplicaUp, ServiceTypeNotificationReply,
    ServiceTypeUpdateReply,
};
use furpc::{CodecError, DecodeBuf, Error, Message};

#[cfg(test)]
pub(in crate::control) fn decode_generation_update(
    message: &Message,
) -> Result<(GenerationUpdate, GenerationHeader)> {
    fn parse(
        headers: &Headers,
        src: &mut DecodeBuf<'_>,
    ) -> std::result::Result<Payload<GenerationUpdate>, CodecError> {
        Payload::decode_parts(headers, src, "GenerationUpdate")
    }
    generation_parts(one_way::decode_message(message, parse)?)
}

pub(in crate::control) fn generation_parts(
    payload: Payload<GenerationUpdate>,
) -> Result<(GenerationUpdate, GenerationHeader)> {
    let (body, generation) = payload.into_parts()?;
    if generation.for_fm_replica {
        return Err(err(Error::Protocol(
            "FMM generation control is not supported",
        )));
    }
    Ok((body, generation))
}

#[derive(Clone)]
pub(in crate::control) enum Incoming {
    ServiceTypeStatusReply {
        enabled: bool,
        payload: Payload<ServiceTypeUpdateReply>,
    },
    NodeActivate(Payload<NodeActivateRequest>),
    NodeDeactivate(Payload<NodeDeactivateRequest>),
    NodeUpdateService(Payload<NodeUpdateServiceRequest>),
    AddInstance(Payload<AddInstance>),
    AddPrimary(Payload<AddPrimary>),
    DoReconfiguration(Payload<DoReconfiguration>),
    ReplicaEndpointUpdatedReply(Payload<AddInstanceReply>),
    ReplicaControl {
        removing: bool,
        payload: Payload<AddReplica>,
    },
    RemoveInstance(Payload<RemoveInstance>),
    DeleteReplica(Payload<DeleteReplica>),
    ReplicaUpReply(Payload<ReplicaUp>),
    ServiceTypeNotificationReply(Payload<ServiceTypeNotificationReply>),
    Unsupported,
}

impl Incoming {
    pub(in crate::control) fn decode(message: &Message) -> Result<(String, Self)> {
        one_way::decode_message(message, Self::decode_parts)
    }

    pub(super) fn decode_parts(
        headers: &Headers,
        src: &mut DecodeBuf<'_>,
    ) -> std::result::Result<(String, Self), CodecError> {
        let action = Properties::from_headers(headers)
            .map_err(CodecError::invalid_payload)?
            .action;
        let incoming = match action.as_str() {
            "ServiceTypeEnabledReply" => Self::ServiceTypeStatusReply {
                enabled: true,
                payload: Payload::decode_parts(headers, src, "ServiceTypeEnabledReply")?,
            },
            "ServiceTypeDisabledReply" => Self::ServiceTypeStatusReply {
                enabled: false,
                payload: Payload::decode_parts(headers, src, "ServiceTypeDisabledReply")?,
            },
            "NodeActivateRequest" => {
                Self::NodeActivate(Payload::decode_parts(headers, src, "NodeActivateRequest")?)
            }
            "NodeDeactivateRequest" => Self::NodeDeactivate(Payload::decode_parts(
                headers,
                src,
                "NodeDeactivateRequest",
            )?),
            "NodeUpdateServiceRequest" => Self::NodeUpdateService(Payload::decode_parts(
                headers,
                src,
                "NodeUpdateServiceRequest",
            )?),
            "AddInstance" => Self::AddInstance(Payload::decode_parts(headers, src, "AddInstance")?),
            "AddPrimary" => Self::AddPrimary(Payload::decode_parts(headers, src, "AddPrimary")?),
            "DoReconfiguration" => {
                Self::DoReconfiguration(Payload::decode_parts(headers, src, "DoReconfiguration")?)
            }
            "ReplicaEndpointUpdatedReply" => Self::ReplicaEndpointUpdatedReply(
                Payload::decode_parts(headers, src, "ReplicaEndpointUpdatedReply")?,
            ),
            "AddReplica" => Self::ReplicaControl {
                removing: false,
                payload: Payload::decode_parts(headers, src, "AddReplica")?,
            },
            "RemoveReplica" => Self::ReplicaControl {
                removing: true,
                payload: Payload::decode_parts(headers, src, "RemoveReplica")?,
            },
            "RemoveInstance" => {
                Self::RemoveInstance(Payload::decode_parts(headers, src, "RemoveInstance")?)
            }
            "DeleteReplica" => {
                Self::DeleteReplica(Payload::decode_parts(headers, src, "DeleteReplica")?)
            }
            "ReplicaUpReply" => {
                Self::ReplicaUpReply(Payload::decode_parts(headers, src, "ReplicaUpReply")?)
            }
            "ServiceTypeNotificationReply" => Self::ServiceTypeNotificationReply(
                Payload::decode_parts(headers, src, "ServiceTypeNotificationReply")?,
            ),
            _ => {
                // Keep foreign/unknown action failures at their original admission points.
                one_way::discard(headers, src)?;
                Self::Unsupported
            }
        };
        Ok((action, incoming))
    }
}

#[cfg(test)]
mod tests {
    use super::super::LIMIT;
    use super::*;
    use fube_ra::protocol::*;
    use fubst::serde::ObjectArray;
    use furpc::{header, Body, HeaderId};

    fn generation() -> GenerationHeader {
        GenerationHeader {
            number: GenerationNumber {
                owner: [9; 16],
                generation: 7,
            },
            for_fm_replica: false,
        }
    }

    fn profiles(generation: &GenerationHeader) -> Vec<Message> {
        let mut base = AddInstance::default();
        base.unit.id.guid.0 = [3; 16];
        base.replica.id = 41;
        base.replica.instance = 52;
        base.service.name = "fabric:/fm-service".into();
        let activation = NodeActivationRequest {
            sequence: 53,
            from_fmm: false,
        };
        let reply = AddInstanceReply {
            unit: base.unit.clone(),
            replica: base.replica.clone(),
            error: -7,
        };
        vec![
            service_type_enabled_reply_message(
                &ServiceTypeUpdateReply { sequence: 64 },
                generation,
            )
            .unwrap(),
            service_type_disabled_reply_message(
                &ServiceTypeUpdateReply { sequence: 75 },
                generation,
            )
            .unwrap(),
            node_activate_request_message(&activation, generation).unwrap(),
            node_deactivate_request_message(&activation, generation).unwrap(),
            node_update_service_request_message(
                &NodeUpdateServiceRequest {
                    service: base.service.clone(),
                },
                generation,
            )
            .unwrap(),
            add_instance_message(&base, generation).unwrap(),
            add_primary_message(&base, generation).unwrap(),
            do_reconfiguration_message(
                &DoReconfiguration {
                    configuration: Configuration {
                        unit: base.unit.clone(),
                        replicas: ObjectArray(vec![base.replica.clone()]),
                        service: base.service.clone(),
                    },
                    phase0_duration: 987,
                },
                generation,
            )
            .unwrap(),
            replica_endpoint_updated_reply_message(&reply, generation).unwrap(),
            add_replica_message(&base, generation).unwrap(),
            remove_replica_message(&base, generation).unwrap(),
            remove_instance_message(&RemoveInstance::new(base.clone(), false), generation).unwrap(),
            delete_replica_message(&DeleteReplica::new(base.clone(), true), generation).unwrap(),
            replica_up_reply_message(
                &ReplicaUp {
                    replicas: ObjectArray(vec![FailoverUnitInfo {
                        service: base.service,
                        unit: base.unit,
                        replicas: ObjectArray(vec![ReplicaInfo {
                            description: base.replica,
                            intermediate_role: NativeReplicaRole::Idle,
                        }]),
                        ..Default::default()
                    }]),
                    last: true,
                    ..Default::default()
                },
                generation,
            )
            .unwrap(),
            service_type_notification_reply_message(
                &ServiceTypeNotification {
                    infos: ObjectArray(vec![ServiceTypeInfo {
                        code_package: "original-package".into(),
                        ..Default::default()
                    }]),
                },
                generation,
            )
            .unwrap(),
        ]
    }

    fn rebuild(message: &Message, body: Body, keep: impl Fn(HeaderId) -> bool) -> Message {
        let mut builder = Message::builder().body(body);
        for header in message
            .headers()
            .records()
            .filter(|header| keep(header.id()))
        {
            builder = builder.append_header(header.to_owned());
        }
        builder.build().unwrap()
    }

    #[test]
    fn contiguous_and_segmented_fm_profiles_match_native_factory_decoders() {
        for original in profiles(&generation()) {
            let bytes: Vec<_> = original.body().segments().flatten().copied().collect();
            for split in [0, bytes.len() / 2] {
                let body = Body::from_segments(vec![
                    Body::from_bytes(bytes[..split].to_vec()).unwrap(),
                    Body::from_bytes(bytes[split..].to_vec()).unwrap(),
                ])
                .unwrap();
                let message = rebuild(&original, body, |_| true);
                let (action, incoming) = Incoming::decode(&message).unwrap();
                assert_eq!(action, super::super::action(&original).unwrap());
                match incoming {
                    Incoming::ServiceTypeStatusReply { enabled, payload } => {
                        let expected = if enabled {
                            decode_service_type_enabled_reply(&original)
                        } else {
                            decode_service_type_disabled_reply(&original)
                        };
                        assert_eq!(payload.into_parts().unwrap(), expected.unwrap());
                    }
                    Incoming::NodeActivate(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_node_activate_request(&original).unwrap()
                    ),
                    Incoming::NodeDeactivate(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_node_deactivate_request(&original).unwrap()
                    ),
                    Incoming::NodeUpdateService(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_node_update_service_request(&original).unwrap()
                    ),
                    Incoming::AddInstance(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_add_instance(&original).unwrap()
                    ),
                    Incoming::AddPrimary(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_add_primary(&original).unwrap()
                    ),
                    Incoming::DoReconfiguration(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_do_reconfiguration(&original).unwrap()
                    ),
                    Incoming::ReplicaEndpointUpdatedReply(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_replica_endpoint_updated_reply(&original).unwrap()
                    ),
                    Incoming::ReplicaControl { removing, payload } => {
                        let expected = if removing {
                            decode_remove_replica(&original)
                        } else {
                            decode_add_replica(&original)
                        };
                        assert_eq!(payload.into_parts().unwrap(), expected.unwrap());
                    }
                    Incoming::RemoveInstance(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_remove_instance(&original).unwrap()
                    ),
                    Incoming::DeleteReplica(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_delete_replica(&original).unwrap()
                    ),
                    Incoming::ReplicaUpReply(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_replica_up_reply(&original).unwrap()
                    ),
                    Incoming::ServiceTypeNotificationReply(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_service_type_notification_reply(&original).unwrap()
                    ),
                    Incoming::Unsupported => panic!("native FM profile was unsupported"),
                }
            }
        }
    }

    #[test]
    fn factory_headers_stay_deferred_while_malformed_fm_bodies_fail_first() {
        let original =
            node_activate_request_message(&NodeActivateRequest::default(), &generation()).unwrap();
        let generation_id = HeaderId::new(GENERATION_HEADER).unwrap();
        for id in [
            header::ACTOR,
            header::ACTION,
            header::HIGH_PRIORITY,
            generation_id,
        ] {
            if id != header::ACTION {
                let missing = rebuild(&original, original.body().clone(), |header| header != id);
                let (_, Incoming::NodeActivate(payload)) = Incoming::decode(&missing).unwrap()
                else {
                    panic!("expected a decoded NodeActivate payload");
                };
                assert_eq!(
                    payload.into_parts().unwrap_err(),
                    err(decode_node_activate_request(&missing).unwrap_err())
                );
            }
            let mut builder = Message::builder().body(original.body().clone());
            for header in original.headers().records() {
                builder = builder.append_header(header.to_owned());
            }
            let duplicate = builder
                .append_header(
                    original
                        .headers()
                        .records()
                        .find(|header| header.id() == id)
                        .unwrap()
                        .to_owned(),
                )
                .build()
                .unwrap();
            let (_, Incoming::NodeActivate(payload)) = Incoming::decode(&duplicate).unwrap() else {
                panic!("expected a decoded NodeActivate payload");
            };
            assert_eq!(
                payload.into_parts().unwrap_err(),
                err(decode_node_activate_request(&duplicate).unwrap_err())
            );
            let malformed = rebuild(&duplicate, Body::from_bytes(vec![0xff]).unwrap(), |_| true);
            assert_eq!(
                Incoming::decode(&malformed).err().unwrap(),
                super::super::predecode(&malformed).unwrap_err()
            );
        }
    }

    #[test]
    fn fm_body_allocation_budget_and_foreign_action_failure_points_are_preserved() {
        let body = DoReconfiguration {
            configuration: Configuration {
                replicas: ObjectArray(vec![Replica::default(); 256]),
                ..Default::default()
            },
            ..Default::default()
        };
        let oversized = do_reconfiguration_message(&body, &generation()).unwrap();
        assert!(oversized.body().len() <= LIMIT);
        let expected = super::super::predecode(&oversized).unwrap_err();
        assert!(expected.contains("cluster native decode"));
        assert_eq!(Incoming::decode(&oversized).err().unwrap(), expected);

        let foreign = get_lsn_message(&GetLsn::default(), &generation()).unwrap();
        let (action, incoming) = Incoming::decode(&foreign).unwrap();
        assert_eq!(action, "GetLSN");
        assert!(matches!(incoming, Incoming::Unsupported));
        let malformed = rebuild(&foreign, Body::from_bytes(vec![0xff]).unwrap(), |_| true);
        assert_eq!(
            Incoming::decode(&malformed).err().unwrap(),
            super::super::predecode(&malformed).unwrap_err()
        );
        let unknown = Message::builder()
            .append_header(header::action("UnknownFmControl").unwrap())
            .build()
            .unwrap();
        assert_eq!(
            Incoming::decode(&unknown).err().unwrap(),
            super::super::predecode(&unknown).unwrap_err()
        );
    }

    #[test]
    fn contiguous_and_segmented_generation_updates_match_native_factory_decoder() {
        let original = generation_update_message(
            &GenerationUpdate {
                fm_service_epoch: Epoch {
                    data_loss: 41,
                    configuration: 52,
                },
            },
            &generation(),
        )
        .unwrap();
        let expected = fube_ra::protocol::decode_generation_update(&original).unwrap();
        let bytes: Vec<_> = original.body().segments().flatten().copied().collect();
        for split in [0, bytes.len() / 2] {
            let body = Body::from_segments(vec![
                Body::from_bytes(bytes[..split].to_vec()).unwrap(),
                Body::from_bytes(bytes[split..].to_vec()).unwrap(),
            ])
            .unwrap();
            let message = rebuild(&original, body, |_| true);
            assert_eq!(super::decode_generation_update(&message).unwrap(), expected);
        }
    }

    #[test]
    fn generation_update_intake_preserves_body_header_and_ordinary_fm_failure_order() {
        fn previous_decode(message: &Message) -> Result<(GenerationUpdate, GenerationHeader)> {
            super::super::predecode(message)?;
            fube_ra::protocol::decode_generation_update(message).map_err(err)
        }

        let original =
            generation_update_message(&GenerationUpdate::default(), &generation()).unwrap();
        let generation_id = HeaderId::new(GENERATION_HEADER).unwrap();
        let mut invalid = vec![];
        for id in [
            header::ACTOR,
            header::ACTION,
            header::HIGH_PRIORITY,
            generation_id,
        ] {
            if id != header::ACTION {
                invalid.push(rebuild(&original, original.body().clone(), |header| {
                    header != id
                }));
            }
            let mut builder = Message::builder().body(original.body().clone());
            for header in original.headers().records() {
                builder = builder.append_header(header.to_owned());
            }
            invalid.push(
                builder
                    .append_header(
                        original
                            .headers()
                            .records()
                            .find(|header| header.id() == id)
                            .unwrap()
                            .to_owned(),
                    )
                    .build()
                    .unwrap(),
            );
        }
        let mut fmm = generation();
        fmm.for_fm_replica = true;
        let mut builder = Message::builder().body(original.body().clone());
        for header in original
            .headers()
            .records()
            .filter(|header| header.id() != generation_id)
        {
            builder = builder.append_header(header.to_owned());
        }
        let fmm = builder
            .append_header(fmm.header().unwrap())
            .build()
            .unwrap();
        assert!(super::decode_generation_update(&fmm)
            .unwrap_err()
            .contains("FMM generation control is not supported"));
        invalid.push(fmm);
        for message in invalid {
            assert_eq!(
                super::decode_generation_update(&message).unwrap_err(),
                previous_decode(&message).unwrap_err()
            );
            let malformed = rebuild(&message, Body::from_bytes(vec![0xff]).unwrap(), |_| true);
            assert_eq!(
                super::decode_generation_update(&malformed).unwrap_err(),
                previous_decode(&malformed).unwrap_err()
            );
        }
        let oversized = rebuild(
            &original,
            Body::from_bytes(vec![0xff; LIMIT + 1]).unwrap(),
            |_| true,
        );
        let expected = previous_decode(&oversized).unwrap_err();
        assert!(expected.contains("ResourceLimit"));
        assert_eq!(
            super::decode_generation_update(&oversized).unwrap_err(),
            expected
        );
    }

    #[tokio::test]
    async fn typed_generation_updates_keep_original_link_generation_independent_delivery() {
        let (node, fm) = super::super::tests::links(false).await;
        let old = node.generation();
        let mut current = old.clone();
        current.number.generation += 3;
        node.advance_generation(&current);
        let mut future = current.clone();
        future.number.generation += 4;
        let body = GenerationUpdate::default();
        for generation in [old, future] {
            fm.send(generation_update_message(&body, &generation).unwrap())
                .await
                .unwrap();
            let incoming = node
                .receive_checked(super::super::deadline())
                .await
                .unwrap()
                .unwrap();
            assert!(incoming.reply.is_none());
            assert_eq!(
                super::decode_generation_update(&incoming.message).unwrap(),
                (body.clone(), generation)
            );
            assert_eq!(node.generation(), current);
            assert!(node.is_live());
        }
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }
}
