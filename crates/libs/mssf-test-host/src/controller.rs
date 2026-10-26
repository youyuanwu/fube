use crate::{ipc::Session, lifecycle_wire::*, require, wire, HostError, Result};
use mssf_transport::{
    message::Message,
    serialization::serde::{from_slice, to_vec, ByteArray, EmptyObjectArray, Guid},
};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitState {
    New,
    OpenPending,
    Open,
    ClosePending,
    Closed,
}

pub struct ServiceUnit {
    pub request: ProxyRequest,
    pub state: UnitState,
}

impl ServiceUnit {
    pub fn new(
        runtime: &wire::RuntimeContext,
        name: &str,
        service_type: &str,
        initialization: &[u8],
    ) -> Self {
        let partition = PartitionId {
            id: Guid(uuid::Uuid::new_v4().to_bytes_le()),
        };
        let mut node = vec![0; 16];
        node[8] = 1;
        Self {
            state: UnitState::New,
            request: ProxyRequest {
                runtime: runtime.id.clone(),
                unit: FailoverUnit {
                    id: partition.clone(),
                    consistency: ConsistencyUnit {
                        id: partition,
                        low: 0,
                        high: 0,
                        name: String::new(),
                        kind: 1,
                    },
                    current: Epoch {
                        configuration: 0,
                        data_loss: 0,
                    },
                    previous: Epoch {
                        configuration: 0,
                        data_loss: 0,
                    },
                    target: 1,
                    minimum: 1,
                },
                local: Replica {
                    node: Node {
                        id: ByteArray(node),
                        instance: 1,
                    },
                    id: 1,
                    instance: 1,
                    current_role: 1,
                    previous_role: 1,
                    up: true,
                    last_lsn: -1,
                    first_lsn: -1,
                    state: 1,
                    version: runtime.code.version.clone(),
                    service_endpoint: String::new(),
                    replication_endpoint: String::new(),
                },
                remote: EmptyObjectArray,
                service: Service {
                    name: name.into(),
                    instance: 1,
                    service_type: ServiceType {
                        package: runtime.code.code.package.package.clone(),
                        name: service_type.into(),
                    },
                    application: runtime.code.application_name.clone(),
                    version: runtime.code.version.clone(),
                    partitions: 1,
                    target: 1,
                    minimum: 1,
                    stateful: false,
                    persisted: false,
                    initialization: ByteArray(initialization.to_vec()),
                    group: false,
                    correlations: EmptyObjectArray,
                    constraints: String::new(),
                    scaleout: 0,
                    metrics: EmptyObjectArray,
                    move_cost: 0,
                    restart_wait: i64::MIN,
                    quorum_wait: i64::MIN,
                    update_version: 1,
                    placement: EmptyObjectArray,
                    standby_keep: i64::MIN,
                    activation_mode: 0,
                    dns: String::new(),
                    scaling: EmptyObjectArray,
                },
                flags: 0,
            },
        }
    }

    pub fn open(&mut self, session: &mut Session) -> Result<()> {
        require(
            self.state == UnitState::New,
            "Open not legal in current unit state",
        )?;
        self.state = UnitState::OpenPending;
        session.one_way(12, "ReplicaOpen", to_vec(&self.request)?)
    }

    pub fn close(&mut self, session: &mut Session, abort: bool) -> Result<()> {
        require(
            self.state == UnitState::Open,
            "Close/Abort not legal in current unit state",
        )?;
        self.request.flags = if abort { 4 } else { 0 };
        self.request.local.state = 2; // Native closing ReplicaDescription defaults to Ready.
        self.state = UnitState::ClosePending;
        session.one_way(12, "ReplicaClose", to_vec(&self.request)?)
    }

    pub fn accept(&mut self, message: &Message) -> Result<()> {
        let properties = message.properties()?;
        let expected = match self.state {
            UnitState::OpenPending => "ReplicaOpenReply",
            UnitState::ClosePending => "ReplicaCloseReply",
            _ => return Err(HostError("unsolicited/late lifecycle reply".into())),
        };
        require(
            properties.actor == 12
                && properties.action == expected
                && !properties.expects_reply
                && properties.relates_to == Default::default(),
            "wrong semantic lifecycle envelope",
        )?;
        let reply: ProxyReply = from_slice(&message.body)?;
        require(
            reply.unit == self.request.unit
                && reply.local.node == self.request.local.node
                && reply.local.id == self.request.local.id
                && reply.local.instance == self.request.local.instance
                && reply.flags == self.request.flags
                && reply.local.current_role == 1
                && reply.local.previous_role == 1
                && reply.local.replication_endpoint.is_empty()
                && reply.local.version == self.request.local.version,
            "stale/mismatched lifecycle reply identity/flags",
        )?;
        require(
            reply.error.code == 0,
            &format!(
                "native {expected} error {}: {} (API {:?})",
                reply.error.code, reply.error.message, reply.error.api
            ),
        )?;
        if self.state == UnitState::OpenPending {
            // An application endpoint is an opaque string. Network validation
            // and probes belong to callers, not native lifecycle admission.
            self.request.local.service_endpoint = reply.local.service_endpoint;
            self.state = UnitState::Open;
        } else {
            self.state = UnitState::Closed;
        }
        Ok(())
    }
}

pub fn validate_endpoint(endpoint: &str) -> Result<SocketAddr> {
    let address: SocketAddr = endpoint
        .parse()
        .map_err(|_| HostError("invalid native Open endpoint".into()))?;
    require(
        address.ip().is_loopback() && address.port() != 0,
        "non-loopback/zero Open endpoint",
    )?;
    Ok(address)
}

pub fn ping(endpoint: &str) -> Result<()> {
    let mut stream =
        TcpStream::connect_timeout(&validate_endpoint(endpoint)?, Duration::from_secs(1))?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    stream.write_all(b"ping\n")?;
    let mut response = [0; 22];
    stream.read_exact(&mut response)?;
    require(
        &response == b"native-stateless-pong\n",
        "wrong application ping response",
    )
}

pub fn stopped(endpoint: &str) -> Result<()> {
    require_connection_refused(TcpStream::connect_timeout(
        &validate_endpoint(endpoint)?,
        Duration::from_secs(1),
    ))
}

fn require_connection_refused(connection: std::io::Result<TcpStream>) -> Result<()> {
    match connection {
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => Ok(()),
        Err(error) => Err(HostError(format!("endpoint shutdown not proven: {error}"))),
        Ok(_) => Err(HostError(
            "endpoint still accepting connections after Close/Abort".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mssf_transport::message::Header;

    fn unit() -> ServiceUnit {
        ServiceUnit::new(
            &wire::RuntimeContext {
                id: "observed-runtime".into(),
                host: wire::HostContext {
                    host: "owned".into(),
                    kind: 1,
                    pid: 42,
                    container: false,
                    activator: false,
                },
                code: wire::expected_code(),
            },
            "fabric:/NativeTestHost/Singleton",
            wire::SERVICE_TYPE,
            b"native-test-host",
        )
    }
    fn reply(unit: &ServiceUnit) -> ProxyReply {
        let mut local = unit.request.local.clone();
        local.service_endpoint = "127.0.0.1:12345".into();
        ProxyReply {
            unit: unit.request.unit.clone(),
            local,
            remote: EmptyObjectArray,
            error: ProxyError {
                code: 0,
                message: String::new(),
                api: ApiName {
                    api: 0,
                    interface: 0,
                    metadata: String::new(),
                },
            },
            flags: 0,
        }
    }
    fn message(reply: &ProxyReply, action: &str) -> Message {
        Message {
            headers: vec![Header::actor(12).unwrap(), Header::action(action).unwrap()],
            body: to_vec(reply).unwrap(),
        }
    }

    #[test]
    fn reject_wrong_incarnation_partition_flags_epoch_and_action() {
        let mut unit = unit();
        unit.state = UnitState::OpenPending;
        let original = reply(&unit);
        for mutation in 0..6 {
            let mut wrong = original.clone();
            match mutation {
                0 => wrong.local.instance += 1,
                1 => wrong.local.id += 1,
                2 => wrong.unit.id.id = Guid([42; 16]),
                3 => wrong.flags = 4,
                4 => wrong.unit.current.configuration += 1,
                _ => wrong.local.node.instance += 1,
            }
            assert!(unit.accept(&message(&wrong, "ReplicaOpenReply")).is_err());
            assert_eq!(unit.state, UnitState::OpenPending);
        }
        assert!(unit
            .accept(&message(&original, "ReplicaCloseReply"))
            .is_err());
        unit.accept(&message(&original, "ReplicaOpenReply"))
            .unwrap();
        assert_eq!(unit.state, UnitState::Open);
        assert!(unit
            .accept(&message(&original, "ReplicaOpenReply"))
            .is_err());
    }

    #[test]
    fn native_error_is_not_converted_into_success() {
        let mut unit = unit();
        unit.state = UnitState::OpenPending;
        let mut failure = reply(&unit);
        failure.error.code = -1;
        failure.error.message = "actual native failure".into();
        let error = unit
            .accept(&message(&failure, "ReplicaOpenReply"))
            .unwrap_err();
        assert!(error.0.contains("actual native failure"));
        assert_eq!(unit.state, UnitState::OpenPending);
    }

    #[test]
    fn opaque_and_empty_endpoints_are_native_application_values() {
        for endpoint in [
            "",
            "opaque:application/endpoint?x=1&y=2",
            r#"{"url":"custom"}"#,
        ] {
            let mut unit = unit();
            unit.state = UnitState::OpenPending;
            let mut value = reply(&unit);
            value.local.service_endpoint = endpoint.into();
            unit.accept(&message(&value, "ReplicaOpenReply")).unwrap();
            assert_eq!(unit.state, UnitState::Open);
            assert_eq!(unit.request.local.service_endpoint, endpoint);
        }
    }

    #[test]
    fn missing_factory_and_other_errors_are_not_ambiguous_retries() {
        for code in [-2147017774, -2147017733, -1] {
            let mut unit = unit();
            unit.state = UnitState::OpenPending;
            let mut failure = reply(&unit);
            failure.error.code = code;
            assert!(unit.accept(&message(&failure, "ReplicaOpenReply")).is_err());
            // No success transition, no replay/new incarnation or automatic retry.
            assert_eq!(unit.state, UnitState::OpenPending);
            assert_eq!(unit.request.local.instance, 1);
        }
    }

    #[test]
    fn probe_rejects_nonloopback_and_listening_endpoints() {
        assert!(validate_endpoint("10.0.0.1:1234").is_err());
        assert!(validate_endpoint("127.0.0.1:0").is_err());
        assert!(validate_endpoint("canned-success").is_err());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        assert!(stopped(&address)
            .unwrap_err()
            .0
            .contains("endpoint still accepting connections"));
    }

    #[test]
    fn shutdown_requires_connection_refused_not_timeout_or_other_errors() {
        use std::io::ErrorKind;

        // Error classification is independent of OS loopback retry timing.
        require_connection_refused(Err(ErrorKind::ConnectionRefused.into())).unwrap();
        for kind in [
            ErrorKind::TimedOut,
            ErrorKind::WouldBlock,
            ErrorKind::ConnectionReset,
            ErrorKind::PermissionDenied,
            ErrorKind::NotConnected,
            ErrorKind::Interrupted,
            ErrorKind::Other,
        ] {
            assert!(require_connection_refused(Err(kind.into()))
                .unwrap_err()
                .0
                .starts_with("endpoint shutdown not proven:"));
        }
    }
}
