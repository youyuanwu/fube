use super::{ipc, ra, rap, Error, Result};
use fube_ra::protocol::Service;
use fubst::serde::ObjectArray;

pub(super) struct RapClient {
    lease: ipc::HostLease,
}

pub(super) struct Prepared {
    action: &'static str,
    body: rap::Request,
}

impl RapClient {
    pub(super) fn new(lease: ipc::HostLease) -> Self {
        Self { lease }
    }

    pub(super) fn prepare(effect: &ra::Effect) -> Result<Prepared> {
        let (action, body) = match effect {
            ra::Effect::UpdateConfiguration {
                body, local, flags, ..
            } => (
                "UpdateConfiguration",
                rap::Request {
                    runtime: String::new(),
                    unit: body.unit.clone(),
                    local: (**local).clone(),
                    remote: body.replicas.clone(),
                    service: Service::default(),
                    flags: *flags,
                },
            ),
            ra::Effect::RuntimeOpen {
                body,
                registration,
                flags,
                ..
            } => ("ReplicaOpen", rap::request(registration, body, *flags)),
            ra::Effect::Close {
                body,
                registration,
                flags,
                ..
            } => ("ReplicaClose", rap::request(registration, body, *flags)),
            ra::Effect::UpdateServiceDescription { body, registration } => (
                "ProxyUpdateServiceDescription",
                rap::request(registration, body, 0),
            ),
            ra::Effect::BuildIdle { body, replica, .. } => (
                "ReplicatorBuildIdleReplica",
                rap::idle_request(body, replica),
            ),
            ra::Effect::RemoveIdle { body, replica, .. } => (
                "ReplicatorRemoveIdleReplica",
                rap::idle_request(body, replica),
            ),
            ra::Effect::QueryReplicaStatus { body, registration }
            | ra::Effect::AcknowledgeEndpoint { body, registration } => (
                if matches!(effect, ra::Effect::QueryReplicaStatus { .. }) {
                    "ReplicatorUpdateEpochAndGetStatus"
                } else {
                    "ProxyReplicaEndpointUpdatedReply"
                },
                rap::Request {
                    runtime: registration.runtime.clone(),
                    unit: body.unit.clone(),
                    local: body.replica.clone(),
                    remote: ObjectArray(vec![]),
                    service: Service::default(),
                    flags: 0,
                },
            ),
            _ => return Err(Error::Unsupported),
        };
        Ok(Prepared { action, body })
    }

    pub(super) fn send(&self, request: Prepared) -> furpc::Result<ipc::LifecycleSend> {
        self.lease.send_lifecycle(
            request.action,
            request.body,
            furpc::FubstEncoder::<rap::Request>::default(),
        )
    }

    pub(super) fn take_reply(&self) -> Option<ipc::LifecycleReply> {
        self.lease.take_lifecycle_reply()
    }

    pub(super) fn owns(&self, reply: &ipc::LifecycleReply) -> bool {
        reply.belongs_to(&self.lease)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fube_ra::protocol::{AddInstance, Configuration, Replica};
    use fubst::serde::to_vec;
    use std::sync::Arc;

    #[test]
    fn effect_profiles_preserve_native_runtime_service_remote_and_flag_distinctions() {
        let mut body = AddInstance::default();
        body.unit.id.guid.0 = [1; 16];
        body.replica.id = 10;
        body.service.name = "fabric:/service".into();
        let body = Arc::new(body);
        let remote = Replica {
            id: 20,
            ..Default::default()
        };
        let registration = ra::RuntimeRegistration {
            runtime: "runtime".into(),
            ..Default::default()
        };
        let cases = [
            (
                ra::Effect::RuntimeOpen {
                    token: 1,
                    body: body.clone(),
                    registration: registration.clone(),
                    flags: 4,
                },
                "ReplicaOpen",
                "runtime",
                body.service.clone(),
                vec![],
                4,
            ),
            (
                ra::Effect::Close {
                    token: 2,
                    body: body.clone(),
                    registration: registration.clone(),
                    flags: 8,
                },
                "ReplicaClose",
                "runtime",
                body.service.clone(),
                vec![],
                8,
            ),
            (
                ra::Effect::UpdateServiceDescription {
                    body: body.clone(),
                    registration: registration.clone(),
                },
                "ProxyUpdateServiceDescription",
                "runtime",
                body.service.clone(),
                vec![],
                0,
            ),
            (
                ra::Effect::BuildIdle {
                    body: body.clone(),
                    replica: Box::new(remote.clone()),
                    registration: registration.clone(),
                },
                "ReplicatorBuildIdleReplica",
                "",
                Service::default(),
                vec![remote.clone()],
                0,
            ),
            (
                ra::Effect::RemoveIdle {
                    body: body.clone(),
                    replica: Box::new(remote.clone()),
                    registration: registration.clone(),
                },
                "ReplicatorRemoveIdleReplica",
                "",
                Service::default(),
                vec![remote.clone()],
                0,
            ),
            (
                ra::Effect::QueryReplicaStatus {
                    body: body.clone(),
                    registration: registration.clone(),
                },
                "ReplicatorUpdateEpochAndGetStatus",
                "runtime",
                Service::default(),
                vec![],
                0,
            ),
            (
                ra::Effect::AcknowledgeEndpoint {
                    body: body.clone(),
                    registration: registration.clone(),
                },
                "ProxyReplicaEndpointUpdatedReply",
                "runtime",
                Service::default(),
                vec![],
                0,
            ),
            (
                ra::Effect::UpdateConfiguration {
                    body: Box::new(Configuration {
                        unit: body.unit.clone(),
                        replicas: ObjectArray(vec![body.replica.clone(), remote.clone()]),
                        service: body.service.clone(),
                    }),
                    local: Box::new(body.replica.clone()),
                    registration,
                    flags: 3,
                },
                "UpdateConfiguration",
                "",
                Service::default(),
                vec![body.replica.clone(), remote],
                3,
            ),
        ];
        for (effect, action, runtime, service, replicas, flags) in cases {
            let request = RapClient::prepare(&effect).unwrap();
            assert_eq!(request.action, action);
            assert_eq!(
                to_vec(&request.body).unwrap(),
                to_vec(&rap::Request {
                    runtime: runtime.into(),
                    unit: body.unit.clone(),
                    local: body.replica.clone(),
                    remote: ObjectArray(replicas),
                    service,
                    flags,
                })
                .unwrap(),
                "{action}"
            );
        }
        assert!(matches!(
            RapClient::prepare(&ra::Effect::ReleaseUsage { acquisition: 1 }),
            Err(Error::Unsupported)
        ));
    }
}
