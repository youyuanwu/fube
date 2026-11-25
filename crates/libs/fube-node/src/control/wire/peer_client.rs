//! Native peer effect profiles; target, phase and retry policy stay in Adapter.
use super::{err, one_way, Link, Result, LIMIT};
use fube_ra::{
    entity::Effect,
    protocol::{GenerationHeader, RA_ACTOR},
};
use furpc::{OneWayClient, PeerSendService};
use serde::Serialize;

pub(in crate::control) struct PeerClient {
    link: Link,
    client: OneWayClient<PeerSendService>,
}

impl PeerClient {
    pub(in crate::control) fn new(link: &Link) -> Result<Self> {
        Ok(Self {
            link: link.clone(),
            client: OneWayClient::new(PeerSendService::new(link.session.peer.clone()))
                .max_encoded_body_bytes(LIMIT)
                .map_err(err)?,
        })
    }

    pub(in crate::control) async fn send(
        &mut self,
        effect: &Effect,
        generation: &GenerationHeader,
    ) -> Result<()> {
        match effect {
            Effect::PeerCreateReplica { body, .. } => {
                self.send_body(body, "CreateReplica", generation).await
            }
            Effect::PeerCreateReplicaReply { body, .. } => {
                self.send_body(body, "CreateReplicaReply", generation).await
            }
            Effect::PeerActivate { body, .. } => self.send_body(body, "Activate", generation).await,
            Effect::PeerActivateReply { body, .. } => {
                self.send_body(body, "ActivateReply", generation).await
            }
            Effect::PeerDeactivate { body, .. } => {
                self.send_body(body, "Deactivate", generation).await
            }
            Effect::PeerDeactivateReply { body, .. } => {
                self.send_body(body, "DeactivateReply", generation).await
            }
            Effect::PeerContinueSwap { body, .. } => {
                self.send_body(body, "DoReconfiguration", generation).await
            }
            Effect::PeerGetLsn { body, .. } => self.send_body(body, "GetLSN", generation).await,
            Effect::PeerGetLsnReply { body, .. } => {
                self.send_body(body, "GetLSNReply", generation).await
            }
            _ => Err("unsupported peer control effect".into()),
        }
    }

    async fn send_body<T: Serialize + Sync>(
        &mut self,
        body: &T,
        action: &str,
        generation: &GenerationHeader,
    ) -> Result<()> {
        let request = one_way::request(body, action, RA_ACTOR, true, Some(generation))?;
        one_way::send(&self.link, &mut self.client, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::{deadline, decode, tests::links, GENERATION_HEADER};
    use super::*;
    use fube_ra::protocol::*;
    use fubst::serde::{to_vec, ObjectArray};
    use std::time::Duration;
    use tokio::time::Instant;

    fn profiles(target: &NodeInstance) -> Vec<(Effect, &'static str, Vec<u8>)> {
        let mut base = AddInstance::default();
        base.unit.id.guid.0 = [3; 16];
        base.replica.id = 41;
        base.replica.instance = 52;
        base.service.name = "fabric:/peer-service".into();
        let deactivation = ReplicaDeactivationInfo {
            catchup_lsn: 123,
            ..Default::default()
        };
        let created = CreateReplica::new(base.clone(), deactivation.clone());
        let replied = AddInstanceReply {
            unit: base.unit.clone(),
            replica: base.replica.clone(),
            error: -7,
        };
        let configuration = Configuration {
            unit: base.unit.clone(),
            replicas: ObjectArray(vec![base.replica.clone()]),
            service: base.service.clone(),
        };
        let activated = Activate {
            configuration: configuration.clone(),
            deactivation: deactivation.clone(),
        };
        let deactivated = Deactivate {
            configuration: configuration.clone(),
            force: true,
            deactivation: deactivation.clone(),
        };
        let continued = DoReconfiguration {
            configuration,
            phase0_duration: 987,
        };
        let progress = GetLsnReply {
            replica: replied.clone(),
            deactivation,
        };
        vec![
            (
                Effect::PeerCreateReplica {
                    target: target.clone(),
                    body: Box::new(created.clone()),
                },
                "CreateReplica",
                to_vec(&created).unwrap(),
            ),
            (
                Effect::PeerCreateReplicaReply {
                    target: target.clone(),
                    body: Box::new(replied.clone()),
                },
                "CreateReplicaReply",
                to_vec(&replied).unwrap(),
            ),
            (
                Effect::PeerActivate {
                    target: target.clone(),
                    body: Box::new(activated.clone()),
                },
                "Activate",
                to_vec(&activated).unwrap(),
            ),
            (
                Effect::PeerActivateReply {
                    target: target.clone(),
                    body: Box::new(replied.clone()),
                },
                "ActivateReply",
                to_vec(&replied).unwrap(),
            ),
            (
                Effect::PeerDeactivate {
                    target: target.clone(),
                    body: Box::new(deactivated.clone()),
                },
                "Deactivate",
                to_vec(&deactivated).unwrap(),
            ),
            (
                Effect::PeerDeactivateReply {
                    target: target.clone(),
                    body: Box::new(replied.clone()),
                },
                "DeactivateReply",
                to_vec(&replied).unwrap(),
            ),
            (
                Effect::PeerContinueSwap {
                    target: target.clone(),
                    body: Box::new(continued.clone()),
                },
                "DoReconfiguration",
                to_vec(&continued).unwrap(),
            ),
            (
                Effect::PeerGetLsn {
                    target: target.clone(),
                    body: Box::new(base.clone()),
                },
                "GetLSN",
                to_vec(&base).unwrap(),
            ),
            (
                Effect::PeerGetLsnReply {
                    target: target.clone(),
                    body: Box::new(progress.clone()),
                },
                "GetLSNReply",
                to_vec(&progress).unwrap(),
            ),
        ]
    }

    #[tokio::test]
    async fn all_peer_profiles_keep_native_bytes_selected_generation_and_one_way_delivery() {
        let (sender, receiver) = links(true).await;
        let mut client = PeerClient::new(&sender).unwrap();
        let mut generation = sender.generation();
        generation.number.generation += 3;
        for (effect, action, bytes) in profiles(&receiver.local) {
            client.send(&effect, &generation).await.unwrap();
            let incoming = receiver.receive_checked(deadline()).await.unwrap().unwrap();
            let headers = incoming.message.headers();
            let common = headers.effective();
            assert_eq!(common.actor.get(), 12);
            assert!(common.action().iter().copied().eq(action.encode_utf16()));
            assert!(common.high_priority);
            assert!(!common.expects_reply);
            assert_eq!(common.relates_to, furpc::MessageId::default());
            assert!(incoming.reply.is_none());
            let mut generations = headers
                .records()
                .filter(|header| header.id().get() == GENERATION_HEADER);
            assert_eq!(
                decode::<GenerationHeader>(generations.next().unwrap().value()).unwrap(),
                generation
            );
            assert!(generations.next().is_none());
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
    async fn preparation_failure_is_isolated_and_retained_client_cannot_reacquire_authority() {
        let (sender, receiver) = links(true).await;
        let mut client = PeerClient::new(&sender).unwrap();
        let generation = sender.generation();
        assert!(client
            .send(&Effect::ReleaseUsage { acquisition: 1 }, &generation)
            .await
            .unwrap_err()
            .contains("unsupported peer control effect"));
        let mut oversized = CreateReplica::default();
        oversized.replica.0.service.name = "x".repeat(LIMIT);
        assert!(client
            .send(
                &Effect::PeerCreateReplica {
                    target: receiver.local.clone(),
                    body: Box::new(oversized),
                },
                &generation,
            )
            .await
            .unwrap_err()
            .contains("ResourceLimit"));
        assert!(sender.is_live());
        let effect = Effect::PeerGetLsnReply {
            target: receiver.local.clone(),
            body: Box::default(),
        };
        client.send(&effect, &generation).await.unwrap();
        let incoming = receiver.receive_checked(deadline()).await.unwrap().unwrap();
        assert_eq!(
            decode_get_lsn_reply(&incoming.message).unwrap(),
            (GetLsnReply::default(), generation.clone())
        );
        sender.close().await.unwrap();
        assert!(client
            .send(&effect, &generation)
            .await
            .unwrap_err()
            .contains("Closed"));
        receiver.close().await.unwrap();
    }

    #[tokio::test]
    async fn peer_client_does_not_bypass_original_relationship_role_policy() {
        let (sender, receiver) = links(false).await;
        let mut client = PeerClient::new(&sender).unwrap();
        let effect = Effect::PeerGetLsn {
            target: receiver.local.clone(),
            body: Box::default(),
        };
        assert!(client.send(&effect, &sender.generation()).await.is_err());
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
