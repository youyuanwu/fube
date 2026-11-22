//! Native FM effect profiles; fences, retries and semantic ACKs stay in Adapter.
use super::{bootstrap::PreparedBodyCodec, deadline, err, outbound, Link, Result, LIMIT};
use fube_ra::{
    entity::Effect,
    protocol::{
        add_instance_reply_message, add_primary_reply_message, add_replica_reply_message,
        change_configuration_message, delete_replica_reply_message,
        do_reconfiguration_reply_message, generation_update_reject_message,
        node_activate_reply_message, node_deactivate_reply_message,
        node_update_service_reply_message, remove_instance_reply_message,
        remove_replica_reply_message, replica_endpoint_updated_message, replica_up_message,
        report_load_message, service_type_disabled_message, service_type_enabled_message,
        service_type_notification_message, GenerationNumber, Owner,
    },
};
use furpc::{Error, Message, OneWayClient, PeerSendService, Request};

pub(in crate::control) struct FmClient {
    link: Link,
    client: OneWayClient<PeerSendService>,
}

impl FmClient {
    pub(in crate::control) fn new(link: &Link) -> Result<Self> {
        Ok(Self {
            link: link.clone(),
            client: OneWayClient::new(PeerSendService::new(link.session.peer.clone()))
                .max_encoded_body_bytes(LIMIT)
                .map_err(err)?,
        })
    }

    fn prepare(effect: &Effect) -> Result<Message> {
        match effect {
            Effect::NodeActivateReply(body) => node_activate_reply_message(body, Owner::Fm),
            Effect::NodeDeactivateReply(body) => node_deactivate_reply_message(body, Owner::Fm),
            Effect::NodeUpdateServiceReply(body) => {
                node_update_service_reply_message(body, Owner::Fm)
            }
            Effect::Reply(body) => add_instance_reply_message(body, Owner::Fm),
            Effect::DoReconfigurationReply(body) => {
                do_reconfiguration_reply_message(body, Owner::Fm)
            }
            Effect::ChangeConfiguration(body) => change_configuration_message(body, Owner::Fm),
            Effect::ReplicaEndpointUpdated(body) => {
                replica_endpoint_updated_message(body, Owner::Fm)
            }
            Effect::PrimaryReply(body) => add_primary_reply_message(body, Owner::Fm),
            Effect::AddReplicaReply(body) => add_replica_reply_message(body, Owner::Fm),
            Effect::RemoveReplicaReply(body) => remove_replica_reply_message(body, Owner::Fm),
            Effect::RemoveReply(body) => remove_instance_reply_message(body, Owner::Fm),
            Effect::DeleteReplicaReply(body) => delete_replica_reply_message(body, Owner::Fm),
            Effect::ServiceTypeStatus {
                enabled: true,
                body,
            } => service_type_enabled_message(body),
            Effect::ServiceTypeStatus {
                enabled: false,
                body,
            } => service_type_disabled_message(body),
            Effect::ReportLoad(body) => report_load_message(body),
            Effect::ServiceTypeWorkCaptured {
                body: Some(body), ..
            } => service_type_notification_message(body),
            Effect::ReplicaUp(body) => replica_up_message(body, Owner::Fm),
            _ => return Err("unsupported FM control effect".into()),
        }
        .map_err(err)
    }

    pub(in crate::control) async fn send(&mut self, effect: &Effect) -> Result<()> {
        self.send_message(Self::prepare(effect)?).await
    }

    pub(in crate::control) async fn generation_update_reject(
        &mut self,
        proposed: &GenerationNumber,
    ) -> Result<()> {
        self.send_message(generation_update_reject_message(proposed).map_err(err)?)
            .await
    }

    async fn send_message(&mut self, message: Message) -> Result<()> {
        let message = self.link.seal(message)?;
        if !self.link.is_live() {
            return Err(err(Error::Closed));
        }
        outbound(self.link.session.role(), &message, false).map_err(err)?;
        let mut request = Request::new(message.body().clone());
        *request.headers_mut() = message.headers().clone();
        self.client
            .send(request, PreparedBodyCodec, deadline())
            .await
            .map(|_| ())
            .map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{decode, tests::links, PToP, GENERATION_HEADER, PTOP};
    use super::*;
    use fube_ra::protocol::*;
    use fubst::serde::{to_vec, ObjectArray};
    use std::time::Duration;
    use tokio::time::Instant;

    fn profiles() -> Vec<(Effect, &'static str, bool, Vec<u8>)> {
        let mut base = AddInstance::default();
        base.unit.id.guid.0 = [3; 16];
        base.replica.id = 41;
        base.replica.instance = 52;
        base.service.name = "fabric:/fm-service".into();
        let replied = AddInstanceReply {
            unit: base.unit.clone(),
            replica: base.replica.clone(),
            error: -7,
        };
        let activation = NodeActivationReply { sequence: 53 };
        let updated = NodeUpdateServiceReply {
            service: base.service.name.clone(),
            instance: 64,
            version: 75,
        };
        let configuration = Configuration {
            unit: base.unit.clone(),
            replicas: ObjectArray(vec![base.replica.clone()]),
            service: base.service.clone(),
        };
        let configured = ConfigurationReply {
            unit: base.unit.clone(),
            replicas: configuration.replicas.clone(),
            error: -8,
        };
        let status = ServiceTypeUpdate {
            types: ObjectArray(vec![ServiceType::default()]),
            sequence: 86,
        };
        let notification = ServiceTypeNotification {
            infos: ObjectArray(vec![ServiceTypeInfo {
                code_package: "original-package".into(),
                ..Default::default()
            }]),
        };
        let report = ReplicaUp {
            replicas: ObjectArray(vec![FailoverUnitInfo {
                service: base.service.clone(),
                unit: base.unit.clone(),
                replicas: ObjectArray(vec![ReplicaInfo {
                    description: base.replica.clone(),
                    intermediate_role: 2,
                }]),
                ..Default::default()
            }]),
            last: true,
            ..Default::default()
        };
        let load = ReportLoad {
            reports: ObjectArray(vec![LoadDescription {
                partition: base.unit.id.guid,
                service: base.service.name.clone(),
                stateful: true,
                primary: ObjectArray(vec![LoadMetricStats {
                    name: "NativeLoad".into(),
                    value: 7,
                    ..Default::default()
                }]),
                ..Default::default()
            }]),
            time: 97,
            ..Default::default()
        };
        vec![
            (
                Effect::NodeActivateReply(activation.clone()),
                "NodeActivateReply",
                false,
                to_vec(&activation).unwrap(),
            ),
            (
                Effect::NodeDeactivateReply(activation.clone()),
                "NodeDeactivateReply",
                false,
                to_vec(&activation).unwrap(),
            ),
            (
                Effect::NodeUpdateServiceReply(updated.clone()),
                "NodeUpdateServiceReply",
                false,
                to_vec(&updated).unwrap(),
            ),
            (
                Effect::Reply(Box::new(replied.clone())),
                "AddInstanceReply",
                true,
                to_vec(&replied).unwrap(),
            ),
            (
                Effect::DoReconfigurationReply(configured.clone()),
                "DoReconfigurationReply",
                true,
                to_vec(&configured).unwrap(),
            ),
            (
                Effect::ChangeConfiguration(Box::new(configuration.clone())),
                "ChangeConfiguration",
                true,
                to_vec(&configuration).unwrap(),
            ),
            (
                Effect::ReplicaEndpointUpdated(Box::new(base.clone())),
                "ReplicaEndpointUpdated",
                false,
                to_vec(&base).unwrap(),
            ),
            (
                Effect::PrimaryReply(Box::new(replied.clone())),
                "AddPrimaryReply",
                true,
                to_vec(&replied).unwrap(),
            ),
            (
                Effect::AddReplicaReply(replied.clone()),
                "AddReplicaReply",
                true,
                to_vec(&replied).unwrap(),
            ),
            (
                Effect::RemoveReplicaReply(replied.clone()),
                "RemoveReplicaReply",
                true,
                to_vec(&replied).unwrap(),
            ),
            (
                Effect::RemoveReply(Box::new(replied.clone())),
                "RemoveInstanceReply",
                true,
                to_vec(&replied).unwrap(),
            ),
            (
                Effect::DeleteReplicaReply(Box::new(replied.clone())),
                "DeleteReplicaReply",
                true,
                to_vec(&replied).unwrap(),
            ),
            (
                Effect::ServiceTypeStatus {
                    enabled: true,
                    body: Box::new(status.clone()),
                },
                "ServiceTypeEnabled",
                false,
                to_vec(&status).unwrap(),
            ),
            (
                Effect::ServiceTypeStatus {
                    enabled: false,
                    body: Box::new(status.clone()),
                },
                "ServiceTypeDisabled",
                false,
                to_vec(&status).unwrap(),
            ),
            (
                Effect::ReportLoad(Box::new(load.clone())),
                "ReportLoad",
                false,
                to_vec(&load).unwrap(),
            ),
            (
                Effect::ServiceTypeWorkCaptured {
                    work: 108,
                    body: Some(Box::new(notification.clone())),
                },
                "ServiceTypeNotification",
                false,
                to_vec(&notification).unwrap(),
            ),
            (
                Effect::ReplicaUp(Box::new(report.clone())),
                "ReplicaUp",
                true,
                to_vec(&report).unwrap(),
            ),
        ]
    }

    #[tokio::test]
    async fn all_fm_profiles_keep_native_bytes_flags_and_one_way_delivery() {
        let (sender, receiver) = links(false).await;
        let mut client = FmClient::new(&sender).unwrap();
        let mut generation = sender.generation();
        generation.number.generation += 3;
        sender.advance_generation(&generation);
        receiver.advance_generation(&generation);
        for (effect, action, high_priority, bytes) in profiles() {
            client.send(&effect).await.unwrap();
            let incoming = receiver.receive_checked(deadline()).await.unwrap().unwrap();
            let headers = incoming.message.headers();
            let common = headers.effective();
            assert_eq!(common.actor.get(), 11);
            assert!(common.action().iter().copied().eq(action.encode_utf16()));
            assert_eq!(common.high_priority, high_priority, "{action}");
            assert!(!common.expects_reply);
            assert!(!common.uncorrelated_reply);
            assert_eq!(common.relates_to, furpc::MessageId::default());
            assert!(incoming.reply.is_none());
            assert!(!headers
                .records()
                .any(|header| header.id().get() == GENERATION_HEADER));
            let mut sources = headers.records().filter(|header| header.id().get() == PTOP);
            let source: PToP = decode(sources.next().unwrap().value()).unwrap();
            assert_eq!(source.from, sender.local);
            assert_eq!(source.to, receiver.local);
            assert!(source.exact_instance);
            assert!(sources.next().is_none());
            assert_eq!(
                incoming
                    .message
                    .body()
                    .segments()
                    .flatten()
                    .copied()
                    .collect::<Vec<_>>(),
                bytes,
                "{action}"
            );
        }
        sender.close().await.unwrap();
        receiver.close().await.unwrap();
    }

    #[tokio::test]
    async fn preparation_failure_is_isolated_and_retirement_revokes_retained_client() {
        let (sender, receiver) = links(false).await;
        let mut client = FmClient::new(&sender).unwrap();
        for effect in [
            Effect::ReleaseUsage { acquisition: 1 },
            Effect::PeerGetLsn {
                target: receiver.local.clone(),
                body: Box::default(),
            },
            Effect::ServiceTypeWorkCaptured {
                work: 2,
                body: None,
            },
        ] {
            assert!(client
                .send(&effect)
                .await
                .unwrap_err()
                .contains("unsupported FM control effect"));
        }
        assert!(client
            .send(&Effect::NodeUpdateServiceReply(NodeUpdateServiceReply {
                service: "x".repeat(LIMIT),
                ..Default::default()
            }))
            .await
            .unwrap_err()
            .contains("ResourceLimit"));
        assert!(sender.is_live());
        let body = NodeDeactivateReply { sequence: 42 };
        let effect = Effect::NodeDeactivateReply(body.clone());
        client.send(&effect).await.unwrap();
        let incoming = receiver.receive_checked(deadline()).await.unwrap().unwrap();
        assert_eq!(
            decode_node_deactivate_reply(&incoming.message).unwrap(),
            (body, Owner::Fm)
        );
        sender.retire();
        assert!(client.send(&effect).await.unwrap_err().contains("Closed"));
        sender.close().await.unwrap();
        receiver.close().await.unwrap();
    }

    #[tokio::test]
    async fn generation_update_reject_keeps_native_one_way_body_and_original_link_authority() {
        let (sender, receiver) = links(false).await;
        let mut client = FmClient::new(&sender).unwrap();
        let generation = sender.generation();
        let proposed = GenerationNumber {
            owner: [4; 16],
            generation: generation.number.generation + 10,
        };
        client.generation_update_reject(&proposed).await.unwrap();
        let incoming = receiver.receive_checked(deadline()).await.unwrap().unwrap();
        let common = incoming.message.headers().effective();
        assert_eq!(common.actor.get(), 11);
        assert!(common
            .action()
            .iter()
            .copied()
            .eq("GenerationUpdateReject".encode_utf16()));
        assert!(common.high_priority);
        assert!(!common.expects_reply);
        assert!(!common.uncorrelated_reply);
        assert_eq!(common.relates_to, furpc::MessageId::default());
        assert!(incoming.reply.is_none());
        assert!(!incoming
            .message
            .headers()
            .records()
            .any(|header| header.id().get() == GENERATION_HEADER));
        assert_eq!(
            decode_generation_update_reject(&incoming.message).unwrap(),
            proposed
        );
        assert_eq!(
            incoming
                .message
                .body()
                .segments()
                .flatten()
                .copied()
                .collect::<Vec<_>>(),
            to_vec(&proposed).unwrap()
        );
        assert_eq!(sender.generation(), generation);
        assert_eq!(receiver.generation(), generation);
        sender.retire();
        assert!(client
            .generation_update_reject(&proposed)
            .await
            .unwrap_err()
            .contains("Closed"));
        sender.close().await.unwrap();
        receiver.close().await.unwrap();
    }

    #[tokio::test]
    async fn fm_client_does_not_bypass_original_relationship_role_policy() {
        for peer in [false, true] {
            let (node, remote) = links(peer).await;
            let (sender, receiver) = if peer { (node, remote) } else { (remote, node) };
            let mut client = FmClient::new(&sender).unwrap();
            assert!(client
                .send(&Effect::NodeActivateReply(NodeActivateReply::default()))
                .await
                .is_err());
            assert!(client
                .generation_update_reject(&sender.generation().number)
                .await
                .is_err());
            assert!(sender.is_live());
            assert!(receiver
                .receive_checked(Instant::now() + Duration::from_millis(5))
                .await
                .unwrap()
                .is_none());
            sender.close().await.unwrap();
            receiver.close().await.unwrap();
        }
    }
}
