//! Typed Node reports for the FM; ledger admission stays with the consumer.
use super::{err, peer_intake::Payload, Headers, Properties, Result, FM_ACTOR, GENERATION_HEADER};
use fube_ra::protocol::{
    AddInstance, AddInstanceReply, Configuration, ConfigurationReply, GenerationNumber,
    NodeActivationReply, NodeUpdateServiceReply, Owner, ReplicaUp, ReportLoad,
    ServiceTypeNotification, ServiceTypeUpdate, FMM_ACTOR,
};
use furpc::{CodecError, DecodeBuf, Error};
use serde::de::DeserializeOwned;

/// A decoded report with original factory-header checks deferred to FM admission.
#[derive(Clone)]
pub struct FmPayload<T>(Payload<T>);

impl<T> FmPayload<T> {
    pub fn into_parts(self) -> Result<(T, Owner)> {
        let action = self.0.action();
        let actors: &[i64] = match action {
            "GenerationUpdateReject"
            | "ServiceTypeEnabled"
            | "ServiceTypeDisabled"
            | "ServiceTypeNotification"
            | "ReportLoad" => &[FM_ACTOR],
            _ => &[FM_ACTOR, FMM_ACTOR],
        };
        let (body, headers, actor) = self.0.factory_parts(actors)?;
        if action == "GenerationUpdateReject"
            && headers
                .records()
                .any(|header| header.id().get() == GENERATION_HEADER)
        {
            return Err(err(Error::Protocol(
                "unexpected generation negotiation header",
            )));
        }
        Ok((
            body,
            if actor == FM_ACTOR {
                Owner::Fm
            } else {
                Owner::Fmm
            },
        ))
    }
}

impl<T: DeserializeOwned> FmPayload<T> {
    fn decode(
        headers: &Headers,
        src: &mut DecodeBuf<'_>,
        action: &'static str,
    ) -> std::result::Result<Self, CodecError> {
        Payload::decode_parts(headers, src, action).map(Self)
    }
}

/// Original-invocation input handoff, not ledger mutation or a semantic ACK.
#[derive(Clone)]
pub enum FmReport {
    ReplicaEndpointUpdated(FmPayload<AddInstance>),
    DoReconfigurationReply(FmPayload<ConfigurationReply>),
    ChangeConfiguration(FmPayload<Configuration>),
    GenerationUpdateReject(FmPayload<GenerationNumber>),
    ServiceTypeStatus {
        enabled: bool,
        payload: FmPayload<ServiceTypeUpdate>,
    },
    NodeActivationReply {
        activate: bool,
        payload: FmPayload<NodeActivationReply>,
    },
    NodeUpdateServiceReply(FmPayload<NodeUpdateServiceReply>),
    ReportLoad(FmPayload<ReportLoad>),
    ReplicaUp(FmPayload<ReplicaUp>),
    ServiceTypeNotification(FmPayload<ServiceTypeNotification>),
    AddReplicaReply(FmPayload<AddInstanceReply>),
    RemoveReplicaReply(FmPayload<AddInstanceReply>),
    LifecycleReply {
        primary: bool,
        removing: bool,
        payload: FmPayload<AddInstanceReply>,
    },
}

impl FmReport {
    pub(super) fn decode_parts(
        headers: &Headers,
        src: &mut DecodeBuf<'_>,
    ) -> std::result::Result<Self, CodecError> {
        let action = Properties::from_headers(headers)
            .map_err(CodecError::invalid_payload)?
            .action;
        Ok(match action.as_str() {
            "ReplicaEndpointUpdated" => Self::ReplicaEndpointUpdated(FmPayload::decode(
                headers,
                src,
                "ReplicaEndpointUpdated",
            )?),
            "DoReconfigurationReply" => Self::DoReconfigurationReply(FmPayload::decode(
                headers,
                src,
                "DoReconfigurationReply",
            )?),
            "ChangeConfiguration" => {
                Self::ChangeConfiguration(FmPayload::decode(headers, src, "ChangeConfiguration")?)
            }
            "GenerationUpdateReject" => Self::GenerationUpdateReject(FmPayload::decode(
                headers,
                src,
                "GenerationUpdateReject",
            )?),
            "ServiceTypeEnabled" => Self::ServiceTypeStatus {
                enabled: true,
                payload: FmPayload::decode(headers, src, "ServiceTypeEnabled")?,
            },
            "ServiceTypeDisabled" => Self::ServiceTypeStatus {
                enabled: false,
                payload: FmPayload::decode(headers, src, "ServiceTypeDisabled")?,
            },
            "NodeActivateReply" => Self::NodeActivationReply {
                activate: true,
                payload: FmPayload::decode(headers, src, "NodeActivateReply")?,
            },
            "NodeDeactivateReply" => Self::NodeActivationReply {
                activate: false,
                payload: FmPayload::decode(headers, src, "NodeDeactivateReply")?,
            },
            "NodeUpdateServiceReply" => Self::NodeUpdateServiceReply(FmPayload::decode(
                headers,
                src,
                "NodeUpdateServiceReply",
            )?),
            "ReportLoad" => Self::ReportLoad(FmPayload::decode(headers, src, "ReportLoad")?),
            "ReplicaUp" => Self::ReplicaUp(FmPayload::decode(headers, src, "ReplicaUp")?),
            "ServiceTypeNotification" => Self::ServiceTypeNotification(FmPayload::decode(
                headers,
                src,
                "ServiceTypeNotification",
            )?),
            "AddReplicaReply" => {
                Self::AddReplicaReply(FmPayload::decode(headers, src, "AddReplicaReply")?)
            }
            "RemoveReplicaReply" => {
                Self::RemoveReplicaReply(FmPayload::decode(headers, src, "RemoveReplicaReply")?)
            }
            "AddInstanceReply" => Self::LifecycleReply {
                primary: false,
                removing: false,
                payload: FmPayload::decode(headers, src, "AddInstanceReply")?,
            },
            "AddPrimaryReply" => Self::LifecycleReply {
                primary: true,
                removing: false,
                payload: FmPayload::decode(headers, src, "AddPrimaryReply")?,
            },
            "RemoveInstanceReply" => Self::LifecycleReply {
                primary: false,
                removing: true,
                payload: FmPayload::decode(headers, src, "RemoveInstanceReply")?,
            },
            "DeleteReplicaReply" => Self::LifecycleReply {
                primary: true,
                removing: true,
                payload: FmPayload::decode(headers, src, "DeleteReplicaReply")?,
            },
            _ => {
                return Err(CodecError::invalid_payload(
                    "unsupported native ingress; no successful ACK",
                ))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::{one_way, tests::links, LIMIT};
    use super::*;
    use fube_ra::protocol::*;
    use fubst::serde::ObjectArray;
    use furpc::{header, Body, HeaderId, Message};

    fn decode(message: &Message) -> Result<FmReport> {
        one_way::decode_message(message, FmReport::decode_parts)
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

    fn profiles() -> Vec<Message> {
        let base = AddInstance::default();
        let reply = AddInstanceReply {
            error: -7,
            ..Default::default()
        };
        vec![
            replica_endpoint_updated_message(&base, Owner::Fm).unwrap(),
            do_reconfiguration_reply_message(&ConfigurationReply::default(), Owner::Fm).unwrap(),
            change_configuration_message(&Configuration::default(), Owner::Fm).unwrap(),
            generation_update_reject_message(&GenerationNumber {
                generation: 41,
                ..Default::default()
            })
            .unwrap(),
            service_type_enabled_message(&ServiceTypeUpdate {
                sequence: 52,
                ..Default::default()
            })
            .unwrap(),
            service_type_disabled_message(&ServiceTypeUpdate {
                sequence: 63,
                ..Default::default()
            })
            .unwrap(),
            node_activate_reply_message(&NodeActivationReply { sequence: 74 }, Owner::Fm).unwrap(),
            node_deactivate_reply_message(&NodeActivationReply { sequence: 85 }, Owner::Fm)
                .unwrap(),
            node_update_service_reply_message(
                &NodeUpdateServiceReply {
                    service: "fabric:/typed".into(),
                    instance: 96,
                    version: 107,
                },
                Owner::Fm,
            )
            .unwrap(),
            report_load_message(&ReportLoad {
                time: 118,
                ..Default::default()
            })
            .unwrap(),
            replica_up_message(
                &ReplicaUp {
                    last: true,
                    ..Default::default()
                },
                Owner::Fm,
            )
            .unwrap(),
            service_type_notification_message(&ServiceTypeNotification {
                infos: ObjectArray(vec![ServiceTypeInfo {
                    code_package: "original-package".into(),
                    ..Default::default()
                }]),
            })
            .unwrap(),
            add_replica_reply_message(&reply, Owner::Fm).unwrap(),
            remove_replica_reply_message(&reply, Owner::Fm).unwrap(),
            add_instance_reply_message(&reply, Owner::Fm).unwrap(),
            add_primary_reply_message(&reply, Owner::Fm).unwrap(),
            remove_instance_reply_message(&reply, Owner::Fm).unwrap(),
            delete_replica_reply_message(&reply, Owner::Fm).unwrap(),
        ]
    }

    #[test]
    fn fm_reports_match_native_decoders_for_contiguous_and_fragmented_bodies() {
        for original in profiles() {
            let bytes: Vec<_> = original.body().segments().flatten().copied().collect();
            for split in [0, bytes.len() / 2] {
                let body = Body::from_segments(vec![
                    Body::from_bytes(bytes[..split].to_vec()).unwrap(),
                    Body::from_bytes(bytes[split..].to_vec()).unwrap(),
                ])
                .unwrap();
                let message = rebuild(&original, body, |_| true);
                match decode(&message).unwrap() {
                    FmReport::ReplicaEndpointUpdated(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_replica_endpoint_updated(&original).unwrap()
                    ),
                    FmReport::DoReconfigurationReply(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_do_reconfiguration_reply(&original).unwrap()
                    ),
                    FmReport::ChangeConfiguration(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_change_configuration(&original).unwrap()
                    ),
                    FmReport::GenerationUpdateReject(payload) => assert_eq!(
                        payload.into_parts().unwrap().0,
                        decode_generation_update_reject(&original).unwrap()
                    ),
                    FmReport::ServiceTypeStatus { enabled, payload } => assert_eq!(
                        payload.into_parts().unwrap().0,
                        if enabled {
                            decode_service_type_enabled(&original)
                        } else {
                            decode_service_type_disabled(&original)
                        }
                        .unwrap()
                    ),
                    FmReport::NodeActivationReply { activate, payload } => assert_eq!(
                        payload.into_parts().unwrap(),
                        if activate {
                            decode_node_activate_reply(&original)
                        } else {
                            decode_node_deactivate_reply(&original)
                        }
                        .unwrap()
                    ),
                    FmReport::NodeUpdateServiceReply(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_node_update_service_reply(&original).unwrap()
                    ),
                    FmReport::ReportLoad(payload) => assert_eq!(
                        payload.into_parts().unwrap().0,
                        decode_report_load(&original).unwrap()
                    ),
                    FmReport::ReplicaUp(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_replica_up(&original).unwrap()
                    ),
                    FmReport::ServiceTypeNotification(payload) => assert_eq!(
                        payload.into_parts().unwrap().0,
                        decode_service_type_notification(&original).unwrap()
                    ),
                    FmReport::AddReplicaReply(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_add_replica_reply(&original).unwrap()
                    ),
                    FmReport::RemoveReplicaReply(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_remove_replica_reply(&original).unwrap()
                    ),
                    FmReport::LifecycleReply {
                        primary,
                        removing,
                        payload,
                    } => {
                        let expected = match (primary, removing) {
                            (true, true) => decode_delete_replica_reply(&original),
                            (true, false) => decode_add_primary_reply(&original),
                            (false, true) => decode_remove_instance_reply(&original),
                            (false, false) => decode_add_instance_reply(&original),
                        };
                        assert_eq!(payload.into_parts().unwrap(), expected.unwrap());
                    }
                }
            }
        }
    }

    #[test]
    fn fm_report_body_failures_precede_deferred_factory_checks_and_keep_budgets() {
        let original = add_primary_reply_message(&AddInstanceReply::default(), Owner::Fm).unwrap();
        let missing = rebuild(&original, original.body().clone(), |id| {
            id != header::HIGH_PRIORITY
        });
        let FmReport::LifecycleReply { payload, .. } = decode(&missing).unwrap() else {
            panic!("lifecycle reply")
        };
        assert_eq!(
            payload.into_parts().unwrap_err(),
            err(decode_add_primary_reply(&missing).unwrap_err())
        );
        let malformed = rebuild(&missing, Body::from_bytes(vec![0xff]).unwrap(), |_| true);
        assert_eq!(
            decode(&malformed).err().unwrap(),
            super::super::predecode(&malformed).unwrap_err()
        );
        let oversized = rebuild(
            &original,
            Body::from_bytes(vec![0xff; LIMIT + 1]).unwrap(),
            |_| true,
        );
        assert!(decode(&oversized).err().unwrap().contains("ResourceLimit"));
        let amplified = do_reconfiguration_reply_message(
            &ConfigurationReply {
                replicas: ObjectArray(vec![Replica::default(); 256]),
                ..Default::default()
            },
            Owner::Fm,
        )
        .unwrap();
        assert!(amplified.body().len() <= LIMIT);
        let expected = super::super::predecode(&amplified).unwrap_err();
        assert!(expected.contains("cluster native decode"));
        assert_eq!(decode(&amplified).err().unwrap(), expected);
    }

    #[test]
    fn fm_report_factory_checks_preserve_fmm_owners_and_generation_reject_profile() {
        let original = replica_up_message(&ReplicaUp::default(), Owner::Fmm).unwrap();
        let FmReport::ReplicaUp(payload) = decode(&original).unwrap() else {
            panic!("replica report")
        };
        assert_eq!(
            payload.into_parts().unwrap(),
            decode_replica_up(&original).unwrap()
        );
        let generation = GenerationHeader {
            number: GenerationNumber::default(),
            for_fm_replica: false,
        };
        let original = generation_update_reject_message(&GenerationNumber::default()).unwrap();
        let mut builder = Message::builder().body(original.body().clone());
        for header in original.headers().records() {
            builder = builder.append_header(header.to_owned());
        }
        let invalid = builder
            .append_header(generation.header().unwrap())
            .build()
            .unwrap();
        let FmReport::GenerationUpdateReject(payload) = decode(&invalid).unwrap() else {
            panic!("generation rejection")
        };
        assert_eq!(
            payload.into_parts().unwrap_err(),
            err(decode_generation_update_reject(&invalid).unwrap_err())
        );
    }

    #[tokio::test]
    async fn fm_report_handoff_ignores_mutable_views_but_not_original_link_authority() {
        let (node, fm) = links(false).await;
        let original = ReportLoad {
            time: 123,
            ..Default::default()
        };
        node.send(report_load_message(&original).unwrap())
            .await
            .unwrap();
        let mut incoming = fm.receive(super::super::deadline()).await.unwrap().unwrap();
        incoming.message = Message::builder()
            .append_header(header::action("NodeUp").unwrap())
            .body(Body::from_bytes(vec![0xff]).unwrap())
            .build()
            .unwrap();
        assert!(incoming
            .fm_report(&node)
            .err()
            .unwrap()
            .contains("foreign Channel"));
        for _ in 0..2 {
            assert_eq!(incoming.action().unwrap(), "ReportLoad");
            let FmReport::ReportLoad(payload) = incoming.fm_report(&fm).unwrap() else {
                panic!("load report")
            };
            assert_eq!(payload.into_parts().unwrap(), (original.clone(), Owner::Fm));
        }
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }
}
