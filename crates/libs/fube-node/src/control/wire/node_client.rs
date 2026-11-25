//! FM-to-Node profiles; selection, generation advancement and retries stay in FM.
use super::{err, one_way, Link, Result, Role, LIMIT};
use fube_ra::protocol::*;
use furpc::{OneWayClient, PeerSendService, Request};
use serde::Serialize;

/// Typed one-way snapshot with its action and FM-selected generation.
pub struct NodeRequest<T>(Request<T>);

impl<T: Clone> NodeRequest<T> {
    fn new(
        body: &T,
        action: &str,
        high_priority: bool,
        generation: &GenerationHeader,
    ) -> Result<Self> {
        one_way::request(
            body.clone(),
            action,
            RA_ACTOR,
            high_priority,
            Some(generation),
        )
        .map(Self)
    }
}

impl NodeRequest<AddInstance> {
    pub fn add_instance(body: &AddInstance, generation: &GenerationHeader) -> Result<Self> {
        Self::new(body, "AddInstance", true, generation)
    }

    pub fn add_primary(body: &AddPrimary, generation: &GenerationHeader) -> Result<Self> {
        Self::new(body, "AddPrimary", true, generation)
    }

    pub fn add_replica(body: &AddReplica, generation: &GenerationHeader) -> Result<Self> {
        Self::new(body, "AddReplica", true, generation)
    }

    pub fn remove_replica(body: &RemoveReplica, generation: &GenerationHeader) -> Result<Self> {
        Self::new(body, "RemoveReplica", true, generation)
    }
}

impl NodeRequest<RemoveInstance> {
    pub fn remove_instance(body: &RemoveInstance, generation: &GenerationHeader) -> Result<Self> {
        Self::new(body, "RemoveInstance", true, generation)
    }

    pub fn delete_replica(body: &DeleteReplica, generation: &GenerationHeader) -> Result<Self> {
        Self::new(body, "DeleteReplica", true, generation)
    }
}

impl NodeRequest<DoReconfiguration> {
    pub fn do_reconfiguration(
        body: &DoReconfiguration,
        generation: &GenerationHeader,
    ) -> Result<Self> {
        Self::new(body, "DoReconfiguration", true, generation)
    }
}

impl NodeRequest<FailoverUnitReply> {
    pub fn change_configuration_reply(
        body: &FailoverUnitReply,
        generation: &GenerationHeader,
    ) -> Result<Self> {
        Self::new(body, "ChangeConfigurationReply", true, generation)
    }
}

impl NodeRequest<AddInstanceReply> {
    pub fn replica_endpoint_updated_reply(
        body: &AddInstanceReply,
        generation: &GenerationHeader,
    ) -> Result<Self> {
        Self::new(body, "ReplicaEndpointUpdatedReply", false, generation)
    }
}

impl NodeRequest<ReplicaUp> {
    pub fn replica_up_reply(body: &ReplicaUp, generation: &GenerationHeader) -> Result<Self> {
        Self::new(body, "ReplicaUpReply", true, generation)
    }
}

impl NodeRequest<ServiceTypeNotificationReply> {
    pub fn service_type_notification_reply(
        body: &ServiceTypeNotificationReply,
        generation: &GenerationHeader,
    ) -> Result<Self> {
        Self::new(body, "ServiceTypeNotificationReply", false, generation)
    }
}

impl NodeRequest<ServiceTypeUpdateReply> {
    pub fn service_type_enabled_reply(
        body: &ServiceTypeUpdateReply,
        generation: &GenerationHeader,
    ) -> Result<Self> {
        Self::new(body, "ServiceTypeEnabledReply", false, generation)
    }

    pub fn service_type_disabled_reply(
        body: &ServiceTypeUpdateReply,
        generation: &GenerationHeader,
    ) -> Result<Self> {
        Self::new(body, "ServiceTypeDisabledReply", false, generation)
    }
}

impl NodeRequest<NodeActivationRequest> {
    pub fn node_activate(
        body: &NodeActivateRequest,
        generation: &GenerationHeader,
    ) -> Result<Self> {
        Self::new(body, "NodeActivateRequest", false, generation)
    }

    pub fn node_deactivate(
        body: &NodeDeactivateRequest,
        generation: &GenerationHeader,
    ) -> Result<Self> {
        Self::new(body, "NodeDeactivateRequest", false, generation)
    }
}

impl NodeRequest<NodeUpdateServiceRequest> {
    pub fn node_update_service(
        body: &NodeUpdateServiceRequest,
        generation: &GenerationHeader,
    ) -> Result<Self> {
        Self::new(body, "NodeUpdateServiceRequest", false, generation)
    }
}

impl NodeRequest<GenerationUpdate> {
    pub fn generation_update(
        body: &GenerationUpdate,
        generation: &GenerationHeader,
    ) -> Result<Self> {
        if generation.for_fm_replica {
            return Err(err(furpc::Error::Protocol(
                "FMM generation control is not supported",
            )));
        }
        Self::new(body, "GenerationUpdate", true, generation)
    }
}

/// Typed one-way sends on one original FM Link, with no reconnect or retry.
pub struct NodeClient {
    link: Link,
    client: OneWayClient<PeerSendService>,
}

impl NodeClient {
    pub fn new(link: &Link) -> Result<Self> {
        if link.session.role() != Role::Fm {
            return Err("FM-to-Node client requires an original FM Link".into());
        }
        Ok(Self {
            link: link.clone(),
            client: OneWayClient::new(PeerSendService::new(link.session.peer.clone()))
                .max_encoded_body_bytes(LIMIT)
                .map_err(err)?,
        })
    }

    /// Await the actual local-write receipt, not remote execution or a semantic ACK.
    pub async fn send<T: Serialize + Send>(&mut self, request: NodeRequest<T>) -> Result<()> {
        one_way::send(&self.link, &mut self.client, request.0).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::{deadline, decode, tests::links, PToP, PTOP};
    use super::*;
    use fubst::serde::ObjectArray;
    use furpc::{header, Message, MessageId};
    use tokio::time::{Duration, Instant};

    async fn profiles(
        generation: &GenerationHeader,
        client: &mut NodeClient,
        node: &Link,
        fm: &Link,
    ) {
        let mut base = AddInstance::default();
        base.unit.id.guid.0 = [3; 16];
        base.replica.id = 41;
        base.replica.instance = 52;
        base.service.name = "fabric:/node-service".into();
        let deleted = DeleteReplica::new(base.clone(), true);
        let removed = RemoveInstance::new(base.clone(), false);
        let reply = AddInstanceReply {
            unit: base.unit.clone(),
            replica: base.replica.clone(),
            error: -7,
        };
        let configured = DoReconfiguration {
            configuration: Configuration {
                unit: base.unit.clone(),
                service: base.service.clone(),
                replicas: ObjectArray(vec![base.replica.clone()]),
            },
            phase0_duration: 63,
        };
        let changed = FailoverUnitReply {
            unit: base.unit.clone(),
            error: -8,
        };
        let report = ReplicaUp {
            last: true,
            ..Default::default()
        };
        let notified = ServiceTypeNotification {
            infos: ObjectArray(vec![ServiceTypeInfo {
                code_package: "original-package".into(),
                ..Default::default()
            }]),
        };
        let status = ServiceTypeUpdateReply { sequence: 74 };
        let activate = NodeActivateRequest {
            sequence: 85,
            from_fmm: false,
        };
        let service = NodeUpdateServiceRequest {
            service: base.service.clone(),
        };
        let update = GenerationUpdate {
            fm_service_epoch: Epoch {
                data_loss: 96,
                configuration: 107,
            },
        };
        macro_rules! profiles {
            ($(($request:expr, $expected:expr)),* $(,)?) => {
                $(profile(client, node, fm, $request, $expected).await;)*
            };
        }
        profiles![
            (
                NodeRequest::add_instance(&base, generation).unwrap(),
                add_instance_message(&base, generation).unwrap()
            ),
            (
                NodeRequest::add_primary(&base, generation).unwrap(),
                add_primary_message(&base, generation).unwrap()
            ),
            (
                NodeRequest::add_replica(&base, generation).unwrap(),
                add_replica_message(&base, generation).unwrap()
            ),
            (
                NodeRequest::remove_replica(&base, generation).unwrap(),
                remove_replica_message(&base, generation).unwrap()
            ),
            (
                NodeRequest::remove_instance(&removed, generation).unwrap(),
                remove_instance_message(&removed, generation).unwrap()
            ),
            (
                NodeRequest::delete_replica(&deleted, generation).unwrap(),
                delete_replica_message(&deleted, generation).unwrap()
            ),
            (
                NodeRequest::do_reconfiguration(&configured, generation).unwrap(),
                do_reconfiguration_message(&configured, generation).unwrap()
            ),
            (
                NodeRequest::change_configuration_reply(&changed, generation).unwrap(),
                change_configuration_reply_message(&changed, generation).unwrap()
            ),
            (
                NodeRequest::replica_endpoint_updated_reply(&reply, generation).unwrap(),
                replica_endpoint_updated_reply_message(&reply, generation).unwrap()
            ),
            (
                NodeRequest::replica_up_reply(&report, generation).unwrap(),
                replica_up_reply_message(&report, generation).unwrap()
            ),
            (
                NodeRequest::service_type_notification_reply(&notified, generation).unwrap(),
                service_type_notification_reply_message(&notified, generation).unwrap()
            ),
            (
                NodeRequest::service_type_enabled_reply(&status, generation).unwrap(),
                service_type_enabled_reply_message(&status, generation).unwrap()
            ),
            (
                NodeRequest::service_type_disabled_reply(&status, generation).unwrap(),
                service_type_disabled_reply_message(&status, generation).unwrap()
            ),
            (
                NodeRequest::node_activate(&activate, generation).unwrap(),
                node_activate_request_message(&activate, generation).unwrap()
            ),
            (
                NodeRequest::node_deactivate(&activate, generation).unwrap(),
                node_deactivate_request_message(&activate, generation).unwrap()
            ),
            (
                NodeRequest::node_update_service(&service, generation).unwrap(),
                node_update_service_request_message(&service, generation).unwrap()
            ),
            (
                NodeRequest::generation_update(&update, generation).unwrap(),
                generation_update_message(&update, generation).unwrap()
            ),
        ];
    }

    #[tokio::test]
    async fn fm_to_node_profiles_keep_native_bytes_headers_and_frozen_generation_on_local_write() {
        let (node, fm) = links(false).await;
        let original = fm.generation();
        let mut current = original.clone();
        current.number.generation += 3;
        fm.advance_generation(&current);
        let mut client = NodeClient::new(&fm).unwrap();
        profiles(&original, &mut client, &node, &fm).await;
        assert_eq!(fm.generation(), current);
        assert_eq!(node.generation(), original);
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }

    async fn profile<T: Serialize + Send>(
        client: &mut NodeClient,
        node: &Link,
        fm: &Link,
        request: NodeRequest<T>,
        expected: Message,
    ) {
        client.send(request).await.unwrap();
        let incoming = node.receive_checked(deadline()).await.unwrap().unwrap();
        let headers = incoming.message.headers();
        let common = headers.effective();
        assert!(!common.expects_reply);
        assert!(!common.uncorrelated_reply);
        assert_eq!(common.relates_to, MessageId::default());
        assert_ne!(common.message_id, MessageId::default());
        assert!(incoming.reply.is_none());
        for header in expected.headers().records() {
            assert_eq!(
                headers
                    .records()
                    .filter(|actual| actual.id() == header.id())
                    .count(),
                1
            );
            assert_eq!(
                headers.first_raw(header.id()).unwrap().value(),
                header.value()
            );
        }
        assert_eq!(
            incoming
                .message
                .body()
                .segments()
                .flatten()
                .copied()
                .collect::<Vec<_>>(),
            expected
                .body()
                .segments()
                .flatten()
                .copied()
                .collect::<Vec<_>>()
        );
        let source: PToP = decode(
            headers
                .first_raw(furpc::HeaderId::new(PTOP).unwrap())
                .unwrap()
                .value(),
        )
        .unwrap();
        assert_eq!(source.from, fm.local);
        assert_eq!(source.to, node.local);
        assert!(source.exact_instance);
        assert!(headers.first_raw(header::EXPECTS_REPLY).is_none());
    }

    #[tokio::test]
    async fn encoding_errors_do_not_submit_or_retire_and_old_clients_never_switch_links() {
        let (node, fm) = links(false).await;
        let generation = fm.generation();
        let mut client = NodeClient::new(&fm).unwrap();
        let mut oversized = AddInstance::default();
        oversized.service.name = "x".repeat(LIMIT);
        let oversized = NodeRequest::add_instance(&oversized, &generation).unwrap();
        assert!(client
            .send(oversized)
            .await
            .unwrap_err()
            .contains("ResourceLimit"));
        let mut fmm = generation.clone();
        fmm.for_fm_replica = true;
        assert!(
            NodeRequest::generation_update(&GenerationUpdate::default(), &fmm)
                .err()
                .unwrap()
                .contains("FMM generation control")
        );
        assert!(fm.is_live());
        assert!(node
            .receive(Instant::now() + Duration::from_millis(5))
            .await
            .unwrap()
            .is_none());
        let body = NodeDeactivateRequest {
            sequence: 123,
            from_fmm: false,
        };
        client
            .send(NodeRequest::node_deactivate(&body, &generation).unwrap())
            .await
            .unwrap();
        let incoming = node.receive(deadline()).await.unwrap().unwrap();
        assert_eq!(
            decode_node_deactivate_request(&incoming.message).unwrap(),
            (body.clone(), generation.clone())
        );

        fm.close().await.unwrap();
        let (successor_node, successor_fm) = links(false).await;
        assert!(client
            .send(NodeRequest::node_deactivate(&body, &generation).unwrap())
            .await
            .unwrap_err()
            .contains("Closed"));
        assert!(successor_node
            .receive(Instant::now() + Duration::from_millis(5))
            .await
            .unwrap()
            .is_none());
        NodeClient::new(&successor_fm)
            .unwrap()
            .send(NodeRequest::node_deactivate(&body, &successor_fm.generation()).unwrap())
            .await
            .unwrap();
        let incoming = successor_node.receive(deadline()).await.unwrap().unwrap();
        assert_eq!(
            decode_node_deactivate_request(&incoming.message).unwrap().0,
            body
        );
        node.close().await.unwrap();
        successor_node.close().await.unwrap();
        successor_fm.close().await.unwrap();
    }

    #[tokio::test]
    async fn request_keeps_its_typed_body_and_generation_snapshot_until_encoding() {
        let (node, fm) = links(false).await;
        let mut generation = fm.generation();
        let mut body = AddInstance::default();
        body.service.name = "fabric:/original".into();
        let expected = (body.clone(), generation.clone());
        let request = NodeRequest::add_instance(&body, &generation).unwrap();
        body.service.name = "fabric:/changed".into();
        generation.number.generation += 1;
        fm.advance_generation(&generation);
        assert_eq!(request.0.body(), &expected.0);
        NodeClient::new(&fm).unwrap().send(request).await.unwrap();
        let incoming = node.receive_checked(deadline()).await.unwrap().unwrap();
        assert_eq!(decode_add_instance(&incoming.message).unwrap(), expected);
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }

    #[tokio::test]
    async fn fm_to_node_client_rejects_node_and_peer_roles_without_transport_side_effects() {
        let (node, fm) = links(false).await;
        assert!(NodeClient::new(&node)
            .err()
            .unwrap()
            .contains("original FM Link"));
        assert!(node.is_live());
        assert!(fm
            .receive(Instant::now() + Duration::from_millis(5))
            .await
            .unwrap()
            .is_none());
        node.close().await.unwrap();
        fm.close().await.unwrap();
        let (first, second) = links(true).await;
        assert!(NodeClient::new(&first)
            .err()
            .unwrap()
            .contains("original FM Link"));
        assert!(first.is_live());
        assert!(second
            .receive(Instant::now() + Duration::from_millis(5))
            .await
            .unwrap()
            .is_none());
        first.close().await.unwrap();
        second.close().await.unwrap();
    }
}
