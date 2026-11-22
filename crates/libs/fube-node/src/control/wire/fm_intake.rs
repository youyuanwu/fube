//! Budgeted FM bodies; generation and domain admission stay in Ingress.
use super::{action, bounded, err, peer_intake::Payload, predecode, Result};
use fube_ra::protocol::{
    AddInstance, AddInstanceReply, AddPrimary, AddReplica, DeleteReplica, DoReconfiguration,
    NodeActivateRequest, NodeDeactivateRequest, NodeUpdateServiceRequest, RemoveInstance,
    ReplicaUp, ServiceTypeNotificationReply, ServiceTypeUpdateReply,
};
use furpc::Message;

pub(in crate::control) enum Incoming<'a> {
    ServiceTypeStatusReply {
        enabled: bool,
        payload: Payload<'a, ServiceTypeUpdateReply>,
    },
    NodeActivate(Payload<'a, NodeActivateRequest>),
    NodeDeactivate(Payload<'a, NodeDeactivateRequest>),
    NodeUpdateService(Payload<'a, NodeUpdateServiceRequest>),
    AddInstance(Payload<'a, AddInstance>),
    AddPrimary(Payload<'a, AddPrimary>),
    DoReconfiguration(Payload<'a, DoReconfiguration>),
    ReplicaEndpointUpdatedReply(Payload<'a, AddInstanceReply>),
    ReplicaControl {
        removing: bool,
        payload: Payload<'a, AddReplica>,
    },
    RemoveInstance(Payload<'a, RemoveInstance>),
    DeleteReplica(Payload<'a, DeleteReplica>),
    ReplicaUpReply(Payload<'a, ReplicaUp>),
    ServiceTypeNotificationReply(Payload<'a, ServiceTypeNotificationReply>),
    Unsupported,
}

impl<'a> Incoming<'a> {
    pub(in crate::control) fn decode(message: &'a Message) -> Result<(String, Self)> {
        bounded(message).map_err(err)?;
        let action = action(message)?;
        let incoming = match action.as_str() {
            "ServiceTypeEnabledReply" => Self::ServiceTypeStatusReply {
                enabled: true,
                payload: Payload::decode(message, "ServiceTypeEnabledReply")?,
            },
            "ServiceTypeDisabledReply" => Self::ServiceTypeStatusReply {
                enabled: false,
                payload: Payload::decode(message, "ServiceTypeDisabledReply")?,
            },
            "NodeActivateRequest" => {
                Self::NodeActivate(Payload::decode(message, "NodeActivateRequest")?)
            }
            "NodeDeactivateRequest" => {
                Self::NodeDeactivate(Payload::decode(message, "NodeDeactivateRequest")?)
            }
            "NodeUpdateServiceRequest" => {
                Self::NodeUpdateService(Payload::decode(message, "NodeUpdateServiceRequest")?)
            }
            "AddInstance" => Self::AddInstance(Payload::decode(message, "AddInstance")?),
            "AddPrimary" => Self::AddPrimary(Payload::decode(message, "AddPrimary")?),
            "DoReconfiguration" => {
                Self::DoReconfiguration(Payload::decode(message, "DoReconfiguration")?)
            }
            "ReplicaEndpointUpdatedReply" => Self::ReplicaEndpointUpdatedReply(Payload::decode(
                message,
                "ReplicaEndpointUpdatedReply",
            )?),
            "AddReplica" => Self::ReplicaControl {
                removing: false,
                payload: Payload::decode(message, "AddReplica")?,
            },
            "RemoveReplica" => Self::ReplicaControl {
                removing: true,
                payload: Payload::decode(message, "RemoveReplica")?,
            },
            "RemoveInstance" => Self::RemoveInstance(Payload::decode(message, "RemoveInstance")?),
            "DeleteReplica" => Self::DeleteReplica(Payload::decode(message, "DeleteReplica")?),
            "ReplicaUpReply" => Self::ReplicaUpReply(Payload::decode(message, "ReplicaUpReply")?),
            "ServiceTypeNotificationReply" => Self::ServiceTypeNotificationReply(Payload::decode(
                message,
                "ServiceTypeNotificationReply",
            )?),
            _ => {
                // Keep foreign/unknown action failures at their original admission points.
                predecode(message)?;
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
                            intermediate_role: 2,
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
}
