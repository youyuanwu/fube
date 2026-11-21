use super::{decode, ipc, ra, rap, rap_client::RapClient, Error, Result, Work};
use fube_ra::protocol::{AddInstance, FailoverUnit, Replica, ReportLoad, Service};
use serde::Deserialize;
use std::sync::Arc;

pub(super) enum Incoming {
    Command(Work),
    Proxy(ProxyReply),
}

pub(super) struct ProxyReply {
    kind: Kind,
    body: rap::Reply,
}

enum Kind {
    Status,
    Configuration,
    Built,
    Removed,
    Open,
    Closed,
    Unsupported,
}

impl Incoming {
    pub(super) fn decode(client: &RapClient, reply: &ipc::LifecycleReply) -> Result<Self> {
        if !client.owns(reply) {
            return Err(Error::Protocol);
        }
        let action = reply.action().map_err(|_| Error::Protocol)?;
        Self::decode_action(&action, reply.body())
    }

    fn decode_action(action: &str, bytes: &[u8]) -> Result<Self> {
        match action {
            "RAPReportLoad" => {
                let report: ReportLoad = decode(bytes)?;
                Ok(Self::Command(Work::Load(Box::new(report))))
            }
            "ProxyUpdateServiceDescriptionReply" => {
                #[derive(Deserialize)]
                struct Reply {
                    error: i64,
                    unit: FailoverUnit,
                    replica: Replica,
                    service: Service,
                }
                let body: Reply = decode(bytes)?;
                Ok(Self::Command(Work::Description(Box::new(
                    ra::ServiceDescriptionCompletion {
                        error: body.error,
                        unit: body.unit,
                        replica: body.replica,
                        service: body.service,
                    },
                ))))
            }
            "ProxyReplicaEndpointUpdated" => {
                let body: AddInstance = decode(bytes)?;
                Ok(Self::Command(Work::Endpoint(Arc::new(body))))
            }
            _ => {
                let kind = match action {
                    "ReplicatorUpdateEpochAndGetStatusReply" => Kind::Status,
                    "UpdateConfigurationReply" => Kind::Configuration,
                    "ReplicatorBuildIdleReplicaReply" => Kind::Built,
                    "ReplicatorRemoveIdleReplicaReply" => Kind::Removed,
                    "ReplicaOpenReply" => Kind::Open,
                    "ReplicaCloseReply" => Kind::Closed,
                    _ => Kind::Unsupported,
                };
                let body: rap::Reply = decode(bytes)?;
                let valid = match kind {
                    Kind::Built | Kind::Removed => body.remote.0.len() == 1 && body.flags == 0,
                    Kind::Configuration => {
                        (2..=ra::MAX_CONFIGURATION_REPLICAS).contains(&body.remote.0.len())
                            && body.flags & !3 == 0
                    }
                    _ => body.remote.0.is_empty() && body.flags & !12 == 0,
                };
                if !valid {
                    return Err(Error::Protocol);
                }
                Ok(Self::Proxy(ProxyReply { kind, body }))
            }
        }
    }
}

impl ProxyReply {
    pub(super) fn unit(&self) -> &FailoverUnit {
        &self.body.unit
    }

    // Unsupported actions fail only after Node's original state/runtime admission.
    pub(super) fn into_work(self) -> Result<Work> {
        let body = self.body;
        Ok(match self.kind {
            Kind::Status => Work::ReplicaStatus(ra::ConfigurationCompletion {
                unit: body.unit,
                local: body.local,
                replicas: body.remote,
                flags: body.flags,
                error: body.error.error,
            }),
            Kind::Configuration => Work::Configured(ra::ConfigurationCompletion {
                unit: body.unit,
                local: body.local,
                replicas: body.remote,
                flags: body.flags,
                error: body.error.error,
            }),
            Kind::Built | Kind::Removed => {
                let event = ra::BuildCompletion {
                    unit: body.unit,
                    local: body.local,
                    remote: body
                        .remote
                        .0
                        .into_iter()
                        .next()
                        .expect("checked one remote"),
                    error: body.error.error,
                    detail: ra::ProxyErrorDetail {
                        message: body.error.message,
                        api: body.error.api,
                    },
                };
                if matches!(self.kind, Kind::Built) {
                    Work::Built(event)
                } else {
                    Work::Removed(event)
                }
            }
            Kind::Open => Work::Open(ra::OpenCompletion {
                unit: body.unit,
                replica: body.local,
                error: body.error.error,
                flags: body.flags,
                detail: ra::ProxyErrorDetail {
                    message: body.error.message,
                    api: body.error.api,
                },
            }),
            Kind::Closed => Work::Closed(ra::CloseCompletion {
                unit: body.unit,
                replica: body.local,
                error: body.error.error,
            }),
            Kind::Unsupported => return Err(Error::Unsupported),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fubst::serde::{to_vec, ObjectArray};

    fn body(remotes: usize, flags: i64) -> rap::Reply {
        rap::Reply {
            unit: FailoverUnit::default(),
            local: Replica::default(),
            remote: ObjectArray(vec![Replica::default(); remotes]),
            error: rap::ProxyError {
                error: -1,
                message: "native error".into(),
                api: Default::default(),
            },
            flags,
        }
    }

    #[test]
    fn native_proxy_profiles_validate_before_delivering_typed_completions() {
        for (action, remotes, flags) in [
            ("ReplicaOpenReply", 0, 12),
            ("ReplicaCloseReply", 0, 0),
            ("ReplicatorUpdateEpochAndGetStatusReply", 0, 4),
            ("UpdateConfigurationReply", 2, 3),
            ("ReplicatorBuildIdleReplicaReply", 1, 0),
            ("ReplicatorRemoveIdleReplicaReply", 1, 0),
        ] {
            let Incoming::Proxy(proxy) =
                Incoming::decode_action(action, &to_vec(&body(remotes, flags)).unwrap()).unwrap()
            else {
                panic!("expected a runtime-admitted proxy completion");
            };
            match (action, proxy.into_work().unwrap()) {
                ("ReplicaOpenReply", Work::Open(event)) => {
                    assert_eq!(event.error, -1);
                    assert_eq!(event.flags, flags);
                    assert_eq!(event.detail.message, "native error");
                }
                ("ReplicaCloseReply", Work::Closed(event)) => assert_eq!(event.error, -1),
                ("ReplicatorUpdateEpochAndGetStatusReply", Work::ReplicaStatus(event))
                | ("UpdateConfigurationReply", Work::Configured(event)) => {
                    assert_eq!(event.error, -1);
                    assert_eq!(event.flags, flags);
                    assert_eq!(event.replicas.0.len(), remotes);
                }
                ("ReplicatorBuildIdleReplicaReply", Work::Built(event))
                | ("ReplicatorRemoveIdleReplicaReply", Work::Removed(event)) => {
                    assert_eq!(event.error, -1);
                    assert_eq!(event.detail.message, "native error");
                }
                _ => panic!("wrong proxy completion method"),
            }
        }
        for (action, remotes, flags) in [
            ("ReplicaOpenReply", 1, 0),
            ("ReplicaCloseReply", 0, 1),
            ("ReplicatorUpdateEpochAndGetStatusReply", 0, 16),
            ("UpdateConfigurationReply", 1, 0),
            (
                "UpdateConfigurationReply",
                ra::MAX_CONFIGURATION_REPLICAS + 1,
                0,
            ),
            ("UpdateConfigurationReply", 2, 4),
            ("ReplicatorBuildIdleReplicaReply", 0, 0),
            ("ReplicatorBuildIdleReplicaReply", 1, 4),
            ("ReplicatorRemoveIdleReplicaReply", 2, 0),
        ] {
            assert!(
                matches!(
                    Incoming::decode_action(action, &to_vec(&body(remotes, flags)).unwrap()),
                    Err(Error::Protocol)
                ),
                "{action}: {remotes} remotes, flags {flags}"
            );
        }
    }

    #[test]
    fn asynchronous_notifications_preserve_their_distinct_native_schemas() {
        let load = ReportLoad {
            time: 25,
            ..Default::default()
        };
        let decoded = Incoming::decode_action("RAPReportLoad", &to_vec(&load).unwrap()).unwrap();
        assert!(matches!(decoded, Incoming::Command(Work::Load(report)) if *report == load));

        #[derive(serde::Serialize)]
        struct Description {
            error: i64,
            unit: FailoverUnit,
            replica: Replica,
            service: Service,
        }
        let mut body = AddInstance::default();
        body.replica.service_endpoint = "tcp://original".into();
        body.service.name = "fabric:/service".into();
        let bytes = to_vec(&Description {
            error: -1,
            unit: body.unit.clone(),
            replica: body.replica.clone(),
            service: body.service.clone(),
        })
        .unwrap();
        let decoded =
            Incoming::decode_action("ProxyUpdateServiceDescriptionReply", &bytes).unwrap();
        assert!(
            matches!(decoded, Incoming::Command(Work::Description(event))
            if event.error == -1 && event.service == body.service && event.replica == body.replica)
        );

        let decoded =
            Incoming::decode_action("ProxyReplicaEndpointUpdated", &to_vec(&body).unwrap())
                .unwrap();
        assert!(matches!(decoded, Incoming::Command(Work::Endpoint(event)) if *event == body));
    }

    #[test]
    fn unsupported_proxy_keeps_its_partition_until_domain_admission() {
        let mut body = body(0, 0);
        body.unit.id.guid.0 = [9; 16];
        let Incoming::Proxy(proxy) =
            Incoming::decode_action("UnsupportedReply", &to_vec(&body).unwrap()).unwrap()
        else {
            panic!("unsupported proxy must still reach domain admission");
        };
        assert_eq!(proxy.unit().id, body.unit.id);
        assert!(matches!(proxy.into_work(), Err(Error::Unsupported)));
        assert!(matches!(
            Incoming::decode_action("UnsupportedReply", &[1, 2, 3]),
            Err(Error::Protocol)
        ));
    }
}
