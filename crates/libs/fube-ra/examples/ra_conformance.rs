//! Compiler-free consumer of independently native-produced fixtures.
mod support;

use clap::Parser;
use fube_ra::protocol::*;
use fube_transport::{
    message::{Header, Message, ACTION, ACTOR, HIGH_PRIORITY},
    serialization::serde::{from_slice, to_vec, CountedStrings, ObjectArray},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    fmt::Debug,
    fs,
    path::{Path, PathBuf},
};
use support::Rich;

#[derive(Parser)]
#[command(about = "Consume independently native-produced RA protocol fixtures")]
struct Arguments {
    #[arg(required_unless_present = "identity")]
    native_input: Option<PathBuf>,
    #[arg(required_unless_present = "identity")]
    rust_output: Option<PathBuf>,
    #[arg(long, conflicts_with_all = ["native_input", "rust_output"])]
    identity: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Scalar<T> {
    value: T,
}

// Schema-only adapter for the cluster tool's private PToP shape. This example
// does not execute its Link/session admission or claim Federation compatibility.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
struct PToP {
    from: NodeInstance,
    to: NodeInstance,
    actor: i64,
    from_ring: String,
    to_ring: String,
    exact_instance: bool,
}

fn filled<T: Default + Rich>(c: &mut i32) -> T {
    let mut value = T::default();
    value.rich(c);
    value
}

fn enumeration(c: &mut i32) -> i64 {
    *c += 1;
    i64::from(*c % 5)
}

macro_rules! rich_fields {
    ($t:ty: $($field:ident),+ $(,)?) => {
        impl Rich for $t {
            fn rich(&mut self, c: &mut i32) { $(self.$field.rich(c);)+ }
        }
    };
}
rich_fields!(FabricCodeVersion: major, minor, build, hotfix);
rich_fields!(FabricConfigVersion: value);
rich_fields!(FabricVersion: code, config);
rich_fields!(FabricVersionInstance: version, instance);
rich_fields!(ServicePackageInfo: package, version);
rich_fields!(ServicePackageResourceGovernance: governed, legacy_cpu_cores, memory_mb, cpu_cores);
rich_fields!(NodeActivationRequest: sequence, from_fmm);
rich_fields!(NodeActivationReply: sequence);
rich_fields!(NodeUpdateServiceRequest: service);
rich_fields!(NodeUpdateServiceReply: service, instance, version);
rich_fields!(ServiceTypeUpdate: types, sequence);
rich_fields!(ServiceTypeUpdateReply: sequence);
rich_fields!(LoadMetricValue: name, value);
rich_fields!(LoadMetricStats: name, value, count, weighted_sum, weight, time);
rich_fields!(NodeLoads: node, loads);
rich_fields!(ReportLoad: legacy, reports, time);
rich_fields!(ReplicaDeactivationInfo: epoch, catchup_lsn);

impl Rich for LegacyLoadReport {
    fn rich(&mut self, c: &mut i32) {
        self.unit.rich(c);
        self.role = enumeration(c);
        self.metrics.rich(c);
    }
}
impl Rich for LoadDescription {
    fn rich(&mut self, c: &mut i32) {
        self.partition.rich(c);
        self.service.rich(c);
        self.stateful.rich(c);
        self.primary.rich(c);
        self.secondary.rich(c);
        self.nodes = ObjectArray(vec![filled(c)]);
    }
}

fn strings(c: &mut i32) -> CountedStrings<Vec<String>> {
    CountedStrings(vec![filled(c), filled(c)])
}

fn entry<K: Default + Rich, V: Default + Rich>(c: &mut i32) -> MapEntry<K, V> {
    MapEntry {
        key: filled(c),
        value: filled(c),
    }
}

fn rich_uri(c: &mut i32) -> Uri {
    Uri {
        kind: enumeration(c),
        scheme: filled(c),
        authority: filled(c),
        host_kind: enumeration(c),
        host: filled(c),
        port: filled(c),
        path: filled(c),
        query: filled(c),
        fragment: filled(c),
        segments: strings(c),
    }
}

fn fresh_version() -> FabricVersionInstance {
    FabricVersionInstance {
        version: FabricVersion {
            code: FabricCodeVersion {
                major: 1,
                minor: 0,
                build: 960,
                hotfix: 0,
            },
            config: FabricConfigVersion {
                value: "__default__".into(),
            },
        },
        instance: 0,
    }
}

fn fresh_node_up() -> NodeUp {
    NodeUp {
        node: NodeDescription {
            name: "node-2".into(),
            node_type: "stateless".into(),
            address: "127.0.0.2".into(),
            cluster_connection_port: 45000,
            version: fresh_version(),
            ..NodeDescription::default()
        },
        packages: ObjectArray(vec![]),
        any_replica_found: false,
    }
}

fn rich_node_up() -> NodeUp {
    let c = &mut 0;
    let mut node = NodeDescription {
        upgrade_domain: filled(c),
        fault_domains: ObjectArray(vec![rich_uri(c), rich_uri(c)]),
        properties: ObjectArray(vec![entry(c)]),
        capacity_ratios: ObjectArray(vec![entry(c)]),
        capacities: ObjectArray(vec![entry(c)]),
        version: filled(c),
        name: filled(c),
        node_type: filled(c),
        address: filled(c),
        cluster_connection_port: filled(c),
        http_gateway_port: 0,
    };
    *c += 1;
    node.http_gateway_port = 60000 + *c as u16;
    let packages = filled(c);
    let _: bool = filled(c);
    NodeUp {
        node,
        packages,
        any_replica_found: true,
    }
}

fn rich_package_upgrade(c: &mut i32) -> ServicePackageUpgradeSpecification {
    ServicePackageUpgradeSpecification {
        package_name: filled(c),
        rollout: filled(c),
        code_package_info_included: filled(c),
        code_package_names: strings(c),
    }
}

fn rich_removal(c: &mut i32) -> ServiceTypeRemovalSpecification {
    ServiceTypeRemovalSpecification {
        package_name: filled(c),
        rollout: filled(c),
        type_names: CountedStrings(vec![filled(c)]),
    }
}

fn rich_upgrade(c: &mut i32) -> UpgradeDescription {
    UpgradeDescription {
        specification: ApplicationUpgradeSpecification {
            application: filled(c),
            version: filled(c),
            instance: filled(c),
            upgrade_type: enumeration(c),
            monitored: filled(c),
            packages: ObjectArray(vec![rich_package_upgrade(c), rich_package_upgrade(c)]),
            application_name: filled(c),
            removed_types: ObjectArray(vec![rich_removal(c), rich_removal(c)]),
            manual: filled(c),
            resource_governance: ObjectArray(vec![entry(c)]),
            container_images: ObjectArray(vec![MapEntry {
                key: filled(c),
                value: CodePackageContainerImages { images: strings(c) },
            }]),
            networks: strings(c),
        },
    }
}

fn rich_ack() -> NodeUpAck {
    let c = &mut 0;
    let upgrades = ObjectArray(vec![rich_upgrade(c), rich_upgrade(c)]);
    let version = filled(c);
    let _: bool = filled(c);
    NodeUpAck {
        upgrades,
        version,
        activated: false,
        activation_sequence: filled(c),
    }
}

fn direct_ptop() -> PToP {
    PToP {
        from: NodeInstance {
            id: [0x11; 16],
            instance: 0x8000_0000_0000_0001,
        },
        to: NodeInstance {
            id: [0x22; 16],
            instance: 0x8000_0000_0000_0002,
        },
        actor: 0,
        from_ring: String::new(),
        to_ring: String::new(),
        exact_instance: true,
    }
}

fn rich_ptop() -> PToP {
    let c = &mut 0;
    let from = filled(c);
    let to = filled(c);
    let _ = enumeration(c);
    let from_ring = filled(c);
    let to_ring = filled(c);
    let _: bool = filled(c);
    PToP {
        from,
        to,
        actor: 2,
        from_ring,
        to_ring,
        exact_instance: false,
    }
}

fn rich_deletion() -> DeleteReplica {
    let mut body = support::rich_request();
    body.unit.target = 1;
    body.unit.minimum = 1;
    body.service.target = 1;
    body.service.minimum = 1;
    body.service.stateful = true;
    body.service.persisted = false;
    body.replica.current_role = 4;
    body.replica.previous_role = 1;
    body.replica.state = 2;
    body.replica.up = true;
    DeleteReplica::new(body, false)
}

fn rich_creation(initial: bool) -> CreateReplica {
    let c = &mut 0;
    let mut body = filled::<AddInstance>(c);
    let mut info = filled::<ReplicaDeactivationInfo>(c);
    body.unit.target = 1;
    body.unit.minimum = 1;
    body.service.target = 1;
    body.service.minimum = 1;
    body.service.stateful = true;
    body.service.persisted = false;
    body.replica.current_role = 2;
    body.replica.previous_role = 1;
    body.replica.state = 1;
    body.replica.up = true;
    if initial {
        info.epoch = body.unit.current;
        info.catchup_lsn = 0;
    }
    CreateReplica::new(body, info)
}

fn rich_creation_reply(error: i64) -> CreateReplicaReply {
    let mut reply = support::rich_reply(error);
    reply.unit.target = 1;
    reply.unit.minimum = 1;
    reply.replica.current_role = 2;
    reply.replica.previous_role = 1;
    reply.replica.state = 1;
    reply.replica.up = true;
    reply
}

fn rich_deletion_reply(error: i64) -> DeleteReplicaReply {
    let mut body = support::rich_reply(error);
    body.unit.target = 1;
    body.unit.minimum = 1;
    body.replica = Replica {
        node: body.replica.node,
        id: body.replica.id,
        instance: body.replica.instance,
        state: 3,
        up: false,
        ..Replica::default()
    };
    body
}

fn native_envelope(input: &Path, body: &str, ack: bool) -> Message {
    let mut headers: Vec<_> = [
        (ACTOR, if ack { "actor-13" } else { "actor-11" }),
        (
            ACTION,
            if ack {
                "action-NodeUpAck"
            } else {
                "action-NodeUp"
            },
        ),
        (HIGH_PRIORITY, "priority"),
    ]
    .into_iter()
    .map(|(id, name)| Header {
        id,
        value: fs::read(input.join(format!("{name}.bin"))).unwrap(),
    })
    .collect();
    if ack {
        headers.push(Header {
            id: GENERATION_HEADER,
            value: fs::read(input.join("fm-generation.bin")).unwrap(),
        });
    }
    Message {
        headers,
        body: fs::read(input.join(format!("{body}.bin"))).unwrap(),
    }
}

fn factory_contract(native: &Message, rust: &Message) {
    let expected = native.properties().unwrap();
    let actual = rust.properties().unwrap();
    assert_eq!(actual.actor, expected.actor);
    assert_eq!(actual.action, expected.action);
    assert_eq!(actual.high_priority, expected.high_priority);
    assert_eq!(rust.headers.len(), native.headers.len());
}

fn reject_prefixes<T: DeserializeOwned>(input: &Path, name: &str) {
    let bytes = fs::read(input.join(format!("{name}.bin"))).unwrap();
    for end in 0..bytes.len() {
        assert!(
            from_slice::<T>(&bytes[..end]).is_err(),
            "{name}: prefix {end}"
        );
    }
}

fn check<T: Serialize + DeserializeOwned + Debug + PartialEq>(
    input: &Path,
    output: &Path,
    name: &str,
    expected: T,
) {
    let bytes = fs::read(input.join(format!("{name}.bin"))).expect("prebuilt native fixture");
    let decoded: T = from_slice(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
    assert_eq!(decoded, expected, "native -> Rust fields: {name}");
    // Rust produces independently constructed values, not a byte echo.
    fs::write(
        output.join(format!("{name}.bin")),
        to_vec(&expected).unwrap(),
    )
    .unwrap();
    println!("{name} native->Rust OK; Rust output produced");
}

fn stage_two_envelope(
    input: &Path,
    name: &str,
    action: &str,
    actor: &str,
    generation: Option<bool>,
) -> Message {
    let mut headers: Vec<_> = [
        (ACTOR, actor.to_string()),
        (ACTION, format!("action-{action}")),
        (HIGH_PRIORITY, "priority-false".to_string()),
    ]
    .into_iter()
    .map(|(id, name)| Header {
        id,
        value: fs::read(input.join(format!("{name}.bin"))).unwrap(),
    })
    .collect();
    if let Some(fmm) = generation {
        headers.push(Header {
            id: GENERATION_HEADER,
            value: fs::read(input.join(if fmm {
                "fmm-generation.bin"
            } else {
                "fm-generation.bin"
            }))
            .unwrap(),
        });
    }
    Message {
        headers,
        body: fs::read(input.join(format!("{name}.bin"))).unwrap(),
    }
}

fn check_incoming<T: Serialize + DeserializeOwned + Debug + PartialEq + Clone>(
    input: &Path,
    output: &Path,
    name: &str,
    action: &str,
    expected: T,
    make: fn(&T, &GenerationHeader) -> fube_transport::Result<Message>,
    decode: fn(&Message) -> fube_transport::Result<(T, GenerationHeader)>,
) {
    check(input, output, name, expected.clone());
    for fmm in [false, true] {
        let native = stage_two_envelope(input, name, action, "actor-12", Some(fmm));
        let generation = support::rich_generation(fmm);
        assert_eq!(
            decode(&native).unwrap(),
            (expected.clone(), generation.clone())
        );
        let made = make(&expected, &generation).unwrap();
        factory_contract(&native, &made);
        assert_eq!(
            made.body,
            fs::read(output.join(format!("{name}.bin"))).unwrap()
        );
        assert_eq!(decode(&made).unwrap(), (expected.clone(), generation));
    }
    reject_prefixes::<T>(input, name);
}

fn check_owner_outgoing<T: Serialize + DeserializeOwned + Debug + PartialEq + Clone>(
    input: &Path,
    output: &Path,
    name: &str,
    action: &str,
    expected: T,
    make: fn(&T, Owner) -> fube_transport::Result<Message>,
    decode: fn(&Message) -> fube_transport::Result<(T, Owner)>,
) {
    check(input, output, name, expected.clone());
    for (actor, owner) in [("actor-11", Owner::Fm), ("actor-10", Owner::Fmm)] {
        let native = stage_two_envelope(input, name, action, actor, None);
        assert_eq!(decode(&native).unwrap(), (expected.clone(), owner));
        let made = make(&expected, owner).unwrap();
        factory_contract(&native, &made);
        assert_eq!(
            made.body,
            fs::read(output.join(format!("{name}.bin"))).unwrap()
        );
        assert_eq!(decode(&made).unwrap(), (expected.clone(), owner));
    }
    reject_prefixes::<T>(input, name);
}

fn check_fm_outgoing<T: Serialize + DeserializeOwned + Debug + PartialEq + Clone>(
    input: &Path,
    output: &Path,
    name: &str,
    action: &str,
    expected: T,
    make: fn(&T) -> fube_transport::Result<Message>,
    decode: fn(&Message) -> fube_transport::Result<T>,
) {
    check(input, output, name, expected.clone());
    let native = stage_two_envelope(input, name, action, "actor-11", None);
    assert_eq!(decode(&native).unwrap(), expected);
    let made = make(&expected).unwrap();
    factory_contract(&native, &made);
    assert_eq!(
        made.body,
        fs::read(output.join(format!("{name}.bin"))).unwrap()
    );
    assert_eq!(decode(&made).unwrap(), expected);
    reject_prefixes::<T>(input, name);
}

fn stage_two(input: &Path, output: &Path) {
    for (profile, rich) in [("empty", false), ("rich", true)] {
        macro_rules! expected {
            ($t:ty) => {
                if rich {
                    filled::<$t>(&mut 0)
                } else {
                    <$t>::default()
                }
            };
        }
        for (root, action, make, decode) in [
            (
                "node-activate-request",
                "NodeActivateRequest",
                node_activate_request_message as fn(&NodeActivationRequest, &GenerationHeader) -> _,
                decode_node_activate_request as fn(&Message) -> _,
            ),
            (
                "node-deactivate-request",
                "NodeDeactivateRequest",
                node_deactivate_request_message,
                decode_node_deactivate_request,
            ),
        ] {
            let mut body = expected!(NodeActivationRequest);
            body.from_fmm = false;
            check_incoming(
                input,
                output,
                &format!("{profile}-{root}"),
                action,
                body,
                make,
                decode,
            );
        }
        for (root, action, make, decode) in [
            (
                "node-activate-reply",
                "NodeActivateReply",
                node_activate_reply_message as fn(&NodeActivationReply, Owner) -> _,
                decode_node_activate_reply as fn(&Message) -> _,
            ),
            (
                "node-deactivate-reply",
                "NodeDeactivateReply",
                node_deactivate_reply_message,
                decode_node_deactivate_reply,
            ),
        ] {
            check_owner_outgoing(
                input,
                output,
                &format!("{profile}-{root}"),
                action,
                expected!(NodeActivationReply),
                make,
                decode,
            );
        }
        check_incoming(
            input,
            output,
            &format!("{profile}-node-update-service-request"),
            "NodeUpdateServiceRequest",
            expected!(NodeUpdateServiceRequest),
            node_update_service_request_message,
            decode_node_update_service_request,
        );
        check_owner_outgoing(
            input,
            output,
            &format!("{profile}-node-update-service-reply"),
            "NodeUpdateServiceReply",
            expected!(NodeUpdateServiceReply),
            node_update_service_reply_message,
            decode_node_update_service_reply,
        );
        for (root, action, make, decode) in [
            (
                "service-type-enabled",
                "ServiceTypeEnabled",
                service_type_enabled_message as fn(&ServiceTypeUpdate) -> _,
                decode_service_type_enabled as fn(&Message) -> _,
            ),
            (
                "service-type-disabled",
                "ServiceTypeDisabled",
                service_type_disabled_message,
                decode_service_type_disabled,
            ),
        ] {
            check_fm_outgoing(
                input,
                output,
                &format!("{profile}-{root}"),
                action,
                expected!(ServiceTypeUpdate),
                make,
                decode,
            );
        }
        for (root, action, make, decode) in [
            (
                "service-type-enabled-reply",
                "ServiceTypeEnabledReply",
                service_type_enabled_reply_message
                    as fn(&ServiceTypeUpdateReply, &GenerationHeader) -> _,
                decode_service_type_enabled_reply as fn(&Message) -> _,
            ),
            (
                "service-type-disabled-reply",
                "ServiceTypeDisabledReply",
                service_type_disabled_reply_message,
                decode_service_type_disabled_reply,
            ),
        ] {
            check_incoming(
                input,
                output,
                &format!("{profile}-{root}"),
                action,
                expected!(ServiceTypeUpdateReply),
                make,
                decode,
            );
        }
        check_owner_outgoing(
            input,
            output,
            &format!("{profile}-replica-endpoint-updated"),
            "ReplicaEndpointUpdated",
            expected!(AddInstance),
            replica_endpoint_updated_message,
            decode_replica_endpoint_updated,
        );
        let endpoint_reply = if rich {
            support::rich_reply(-2_147_017_398)
        } else {
            AddInstanceReply::default()
        };
        check_incoming(
            input,
            output,
            &format!("{profile}-replica-endpoint-updated-reply"),
            "ReplicaEndpointUpdatedReply",
            endpoint_reply,
            replica_endpoint_updated_reply_message,
            decode_replica_endpoint_updated_reply,
        );
        check_fm_outgoing(
            input,
            output,
            &format!("{profile}-report-load"),
            "ReportLoad",
            expected!(ReportLoad),
            report_load_message,
            decode_report_load,
        );
    }
    for (root, action, make, decode) in [
        (
            "node-activate-request",
            "NodeActivateRequest",
            node_activate_request_message as fn(&NodeActivationRequest, &GenerationHeader) -> _,
            decode_node_activate_request as fn(&Message) -> _,
        ),
        (
            "node-deactivate-request",
            "NodeDeactivateRequest",
            node_deactivate_request_message,
            decode_node_deactivate_request,
        ),
    ] {
        check_incoming(
            input,
            output,
            &format!("fmm-{root}"),
            action,
            NodeActivationRequest {
                sequence: i64::MIN + 17,
                from_fmm: true,
            },
            make,
            decode,
        );
    }
    for legacy in [false, true] {
        let c = &mut 0;
        let mut body = ReportLoad::default();
        if legacy {
            body.legacy = filled(c);
        } else {
            body.reports = filled(c);
        }
        body.time = filled(c);
        check_fm_outgoing(
            input,
            output,
            if legacy {
                "legacy-report-load"
            } else {
                "modern-report-load"
            },
            "ReportLoad",
            body,
            report_load_message,
            decode_report_load,
        );
    }
}

fn generation_number(generation: i64, high: u64, low: u64) -> GenerationNumber {
    let mut owner = [0; 16];
    owner[..8].copy_from_slice(&high.to_le_bytes());
    owner[8..].copy_from_slice(&low.to_le_bytes());
    GenerationNumber { generation, owner }
}

fn peer_contracts(input: &Path, output: &Path) {
    for (name, body) in [
        ("default-create-replica", CreateReplica::default()),
        ("rich-create-replica", rich_creation(false)),
        ("initial-create-replica", rich_creation(true)),
    ] {
        check_peer(
            input,
            output,
            name,
            "CreateReplica",
            body,
            create_replica_message,
            decode_create_replica,
        );
    }
    for (name, body) in [
        (
            "default-create-replica-reply",
            CreateReplicaReply::default(),
        ),
        ("rich-create-replica-reply-0", rich_creation_reply(0)),
        (
            "rich-create-replica-reply--2147017398",
            rich_creation_reply(-2_147_017_398),
        ),
    ] {
        check_peer(
            input,
            output,
            name,
            "CreateReplicaReply",
            body,
            create_replica_reply_message,
            decode_create_replica_reply,
        );
    }
    for action in ["CreateReplica", "CreateReplicaReply"] {
        check(
            input,
            output,
            &format!("action-{action}"),
            Scalar {
                value: action.to_string(),
            },
        );
    }
}

fn configuration_contracts(input: &Path, output: &Path) {
    let c = &mut 0;
    let config = filled::<Configuration>(c);
    let duration = filled::<i64>(c);
    for (name, body) in [
        ("default-do-reconfiguration", DoReconfiguration::default()),
        (
            "rich-do-reconfiguration",
            DoReconfiguration {
                configuration: config,
                phase0_duration: duration,
            },
        ),
    ] {
        check_peer(
            input,
            output,
            name,
            "DoReconfiguration",
            body,
            do_reconfiguration_message,
            decode_do_reconfiguration,
        );
    }
    let c = &mut 0;
    let config = filled::<Configuration>(c);
    let deactivation = filled::<ReplicaDeactivationInfo>(c);
    for (name, body) in [
        ("default-activate", Activate::default()),
        (
            "rich-activate",
            Activate {
                configuration: config,
                deactivation,
            },
        ),
    ] {
        check_peer(
            input,
            output,
            name,
            "Activate",
            body,
            activate_message,
            decode_activate,
        );
    }
    for (name, body) in [
        ("default-activate-reply", ActivateReply::default()),
        ("rich-activate-reply", filled::<ActivateReply>(&mut 0)),
    ] {
        check_peer(
            input,
            output,
            name,
            "ActivateReply",
            body,
            activate_reply_message,
            decode_activate_reply,
        );
    }
    for (name, body) in [
        (
            "default-do-reconfiguration-reply",
            ConfigurationReply::default(),
        ),
        (
            "rich-do-reconfiguration-reply",
            filled::<ConfigurationReply>(&mut 0),
        ),
    ] {
        check(input, output, name, body.clone());
        for (actor, owner) in [("actor-11", Owner::Fm), ("actor-10", Owner::Fmm)] {
            let mut native = stage_two_envelope(input, name, "DoReconfigurationReply", actor, None);
            native
                .headers
                .iter_mut()
                .find(|h| h.id == HIGH_PRIORITY)
                .unwrap()
                .value = fs::read(input.join("priority.bin")).unwrap();
            assert_eq!(
                decode_do_reconfiguration_reply(&native).unwrap(),
                (body.clone(), owner)
            );
            factory_contract(
                &native,
                &do_reconfiguration_reply_message(&body, owner).unwrap(),
            );
            assert!(decode_add_replica_reply(&native).is_err());
        }
        reject_prefixes::<ConfigurationReply>(input, name);
    }
    for action in [
        "DoReconfiguration",
        "DoReconfigurationReply",
        "Activate",
        "ActivateReply",
    ] {
        check(
            input,
            output,
            &format!("action-{action}"),
            Scalar {
                value: action.to_string(),
            },
        );
    }
}

fn replica_contracts(input: &Path, output: &Path) {
    let mut request = support::rich_request();
    request.unit.target = 1;
    request.unit.minimum = 1;
    request.service.target = 1;
    request.service.minimum = 1;
    request.service.stateful = true;
    request.service.persisted = false;
    request.replica.current_role = 2;
    request.replica.previous_role = 1;
    request.replica.state = 1;
    request.replica.up = true;
    for (name, body) in [
        ("default-add-replica", AddReplica::default()),
        ("rich-add-replica", request.clone()),
    ] {
        check(input, output, name, body.clone());
        for fmm in [false, true] {
            let native = generation_envelope(input, name, "AddReplica", true, fmm);
            let generation = support::rich_generation(fmm);
            assert_eq!(
                decode_add_replica(&native).unwrap(),
                (body.clone(), generation.clone())
            );
            factory_contract(&native, &add_replica_message(&body, &generation).unwrap());
            assert!(decode_add_primary(&native).is_err());
            assert!(decode_create_replica(&native).is_err());
        }
        reject_prefixes::<AddReplica>(input, name);
    }
    let mut replies = vec![(
        "default-add-replica-reply".to_owned(),
        AddReplicaReply::default(),
    )];
    for error in [0, -2_147_017_398] {
        let mut reply = support::rich_reply(error);
        reply.unit.target = 1;
        reply.unit.minimum = 1;
        reply.replica.current_role = 2;
        reply.replica.previous_role = 1;
        reply.replica.state = if error == 0 { 2 } else { 1 };
        reply.replica.up = true;
        reply.replica.service_endpoint.clear();
        replies.push((format!("rich-add-replica-reply-{error}"), reply));
    }
    for (name, body) in replies {
        check(input, output, &name, body.clone());
        for (actor, owner) in [("actor-11", Owner::Fm), ("actor-10", Owner::Fmm)] {
            let mut native = stage_two_envelope(input, &name, "AddReplicaReply", actor, None);
            native
                .headers
                .iter_mut()
                .find(|h| h.id == HIGH_PRIORITY)
                .unwrap()
                .value = fs::read(input.join("priority.bin")).unwrap();
            assert_eq!(
                decode_add_replica_reply(&native).unwrap(),
                (body.clone(), owner)
            );
            factory_contract(&native, &add_replica_reply_message(&body, owner).unwrap());
            assert!(decode_add_primary_reply(&native).is_err());
            assert!(decode_create_replica_reply(&native).is_err());
        }
        reject_prefixes::<AddReplicaReply>(input, &name);
    }
    for (name, body) in [
        ("default-remove-replica", RemoveReplica::default()),
        ("rich-remove-replica", request.clone()),
    ] {
        check_peer(
            input,
            output,
            name,
            "RemoveReplica",
            body,
            remove_replica_message,
            decode_remove_replica,
        );
        let native = generation_envelope(input, name, "RemoveReplica", true, false);
        assert!(decode_add_replica(&native).is_err());
        assert!(decode_remove_instance(&native).is_err());
        assert!(decode_delete_replica(&native).is_err());
    }
    let mut replies = vec![(
        "default-remove-replica-reply".to_owned(),
        RemoveReplicaReply::default(),
    )];
    let mut cancelled_create = RemoveReplicaReply {
        unit: request.unit,
        replica: request.replica,
        error: 0,
    };
    cancelled_create.replica.service_endpoint.clear();
    cancelled_create.replica.replication_endpoint.clear();
    replies.push((
        "in-create-remove-replica-reply".to_owned(),
        cancelled_create,
    ));
    for error in [0, -2_147_017_398] {
        let mut reply = support::rich_reply(error);
        reply.unit.target = 1;
        reply.unit.minimum = 1;
        reply.replica.current_role = 2;
        reply.replica.previous_role = 1;
        reply.replica.state = 3;
        reply.replica.up = true;
        reply.replica.service_endpoint.clear();
        replies.push((format!("rich-remove-replica-reply-{error}"), reply));
    }
    for (name, body) in replies {
        check(input, output, &name, body.clone());
        for (actor, owner) in [("actor-11", Owner::Fm), ("actor-10", Owner::Fmm)] {
            let mut native = stage_two_envelope(input, &name, "RemoveReplicaReply", actor, None);
            native
                .headers
                .iter_mut()
                .find(|h| h.id == HIGH_PRIORITY)
                .unwrap()
                .value = fs::read(input.join("priority.bin")).unwrap();
            assert_eq!(
                decode_remove_replica_reply(&native).unwrap(),
                (body.clone(), owner)
            );
            factory_contract(
                &native,
                &remove_replica_reply_message(&body, owner).unwrap(),
            );
            assert!(decode_add_replica_reply(&native).is_err());
            assert!(decode_delete_replica_reply(&native).is_err());
        }
        reject_prefixes::<RemoveReplicaReply>(input, &name);
    }
    for action in [
        "AddReplica",
        "AddReplicaReply",
        "RemoveReplica",
        "RemoveReplicaReply",
    ] {
        check(
            input,
            output,
            &format!("action-{action}"),
            Scalar {
                value: action.to_owned(),
            },
        );
    }
}

fn check_peer<T: Serialize + DeserializeOwned + Debug + PartialEq + Clone>(
    input: &Path,
    output: &Path,
    name: &str,
    action: &str,
    expected: T,
    make: fn(&T, &GenerationHeader) -> fube_transport::Result<Message>,
    decode: fn(&Message) -> fube_transport::Result<(T, GenerationHeader)>,
) {
    check(input, output, name, expected.clone());
    for fmm in [false, true] {
        let mut native = stage_two_envelope(input, name, action, "actor-12", Some(fmm));
        native
            .headers
            .iter_mut()
            .find(|h| h.id == HIGH_PRIORITY)
            .unwrap()
            .value = fs::read(input.join("priority.bin")).unwrap();
        let generation = support::rich_generation(fmm);
        assert_eq!(
            decode(&native).unwrap(),
            (expected.clone(), generation.clone())
        );
        let made = make(&expected, &generation).unwrap();
        factory_contract(&native, &made);
        assert_eq!(made.body, to_vec(&expected).unwrap());
        assert_eq!(decode(&made).unwrap(), (expected.clone(), generation));
        native.headers.retain(|h| h.id != GENERATION_HEADER);
        assert!(decode(&native).is_err());
    }
    reject_prefixes::<T>(input, name);
}

fn generation_envelope(
    input: &Path,
    name: &str,
    action: &str,
    incoming: bool,
    fmm: bool,
) -> Message {
    let mut message = stage_two_envelope(
        input,
        name,
        action,
        if incoming { "actor-12" } else { "actor-11" },
        incoming.then_some(fmm),
    );
    message.headers[2] = Header {
        id: HIGH_PRIORITY,
        value: fs::read(input.join("priority.bin")).unwrap(),
    };
    message
}

fn reject_generation_envelopes(
    original: &Message,
    with_generation: bool,
    decode: fn(&Message) -> fube_transport::Result<()>,
) {
    for id in [ACTOR, ACTION, HIGH_PRIORITY]
        .into_iter()
        .chain(with_generation.then_some(GENERATION_HEADER))
    {
        let mut missing = original.clone();
        missing.headers.retain(|h| h.id != id);
        assert!(decode(&missing).is_err());
        let header = original.headers.iter().find(|h| h.id == id).unwrap();
        let mut duplicate = original.clone();
        duplicate.headers.push(header.clone());
        assert!(decode(&duplicate).is_err());
        let mut malformed = original.clone();
        malformed
            .headers
            .iter_mut()
            .find(|h| h.id == id)
            .unwrap()
            .value
            .pop();
        assert!(decode(&malformed).is_err());
    }
    let mut wrong = original.clone();
    wrong.headers[0] = Header::actor(FMM_ACTOR).unwrap();
    assert!(decode(&wrong).is_err());
    wrong.headers[0] = original.headers[0].clone();
    wrong.headers[1] = Header::action("LFUMUpload").unwrap();
    assert!(decode(&wrong).is_err());
    if !with_generation {
        let mut unexpected = original.clone();
        unexpected
            .headers
            .push(GenerationHeader::default().header().unwrap());
        assert!(decode(&unexpected).is_err());
    }
    let mut trailing = original.clone();
    trailing.body.push(0);
    assert!(decode(&trailing).is_err());
    let mut shape = original.clone();
    shape.body = to_vec(&GenerationHeader::default()).unwrap();
    assert!(decode(&shape).is_err());
}

fn generation_handshake(input: &Path, output: &Path) {
    let empty = fs::read(input.join("empty-generation-proposal.bin")).unwrap();
    assert!(empty.is_empty());
    for fmm in [false, true] {
        let native = generation_envelope(
            input,
            "empty-generation-proposal",
            "GenerationProposal",
            true,
            fmm,
        );
        let expected = support::rich_generation(fmm);
        assert_eq!(decode_generation_proposal(&native).unwrap(), expected);
        let made = generation_proposal_message(&expected).unwrap();
        factory_contract(&native, &made);
        assert_eq!(made.body, empty);
        assert_eq!(decode_generation_proposal(&made).unwrap(), expected);
        reject_generation_envelopes(&native, true, |m| decode_generation_proposal(m).map(|_| ()));
        let mut bad = native.clone();
        bad.body = to_vec(&GenerationNumber::default()).unwrap();
        assert!(decode_generation_proposal(&bad).is_err());
    }
    fs::write(output.join("empty-generation-proposal.bin"), []).unwrap();
    let proposed = generation_number(i64::MAX, 0x8000_0000_0000_0001, u64::MAX);
    let current = generation_number(i64::MAX, 0x8000_0000_0000_0002, 0);
    let mut replies = vec![(
        "default-generation-proposal-reply".to_string(),
        GenerationProposalReply::default(),
    )];
    for error in [0, -2_147_017_398, -2_147_017_441] {
        // The native rich traversal consumes counter, owner, counter, owner, error.
        replies.push((
            format!("rich-generation-proposal-reply-{error}"),
            GenerationProposalReply {
                proposed: support::rich_generation(false).number,
                current: generation_number(
                    -0x1234_5678_0003,
                    u64::from_le_bytes([4, 5, 6, 7, 8, 9, 10, 11]),
                    u64::from_le_bytes([12, 13, 14, 15, 16, 17, 18, 19]),
                ),
                error,
            },
        ));
    }
    replies.push((
        "tie-generation-proposal-reply".to_string(),
        GenerationProposalReply {
            proposed: proposed.clone(),
            current: current.clone(),
            error: -2_147_017_398,
        },
    ));
    for (name, body) in replies {
        check(input, output, &name, body.clone());
        let native = generation_envelope(input, &name, "GenerationProposalReply", false, false);
        assert_eq!(decode_generation_proposal_reply(&native).unwrap(), body);
        let made = generation_proposal_reply_message(&body).unwrap();
        factory_contract(&native, &made);
        assert_eq!(
            made.body,
            fs::read(output.join(format!("{name}.bin"))).unwrap()
        );
        assert_eq!(decode_generation_proposal_reply(&made).unwrap(), body);
        reject_generation_envelopes(&native, false, |m| {
            decode_generation_proposal_reply(m).map(|_| ())
        });
        reject_prefixes::<GenerationProposalReply>(input, &name);
    }
    assert!(proposed.older_than(&current));
    let lower_low = generation_number(i64::MAX, 0x8000_0000_0000_0001, u64::MAX - 1);
    assert!(lower_low.older_than(&proposed));
    assert!(!proposed.older_than(&lower_low));
    for (name, body) in [
        ("default-generation-update", GenerationUpdate::default()),
        (
            "rich-generation-update",
            GenerationUpdate {
                fm_service_epoch: Epoch {
                    configuration: i64::MIN + 17,
                    data_loss: i64::MAX - 29,
                },
            },
        ),
    ] {
        check(input, output, name, body.clone());
        let native = generation_envelope(input, name, "GenerationUpdate", true, false);
        let generation = support::rich_generation(false);
        assert_eq!(
            decode_generation_update(&native).unwrap(),
            (body.clone(), generation.clone())
        );
        let made = generation_update_message(&body, &generation).unwrap();
        factory_contract(&native, &made);
        assert_eq!(
            made.body,
            fs::read(output.join(format!("{name}.bin"))).unwrap()
        );
        assert_eq!(
            decode_generation_update(&made).unwrap(),
            (body.clone(), generation)
        );
        reject_generation_envelopes(&native, true, |m| decode_generation_update(m).map(|_| ()));
        let mut fmm = generation_envelope(input, name, "GenerationUpdate", true, true);
        assert!(decode_generation_update(&fmm).is_err());
        fmm.body.clear();
        assert!(decode_generation_update(&fmm).is_err());
        assert!(generation_update_message(&body, &support::rich_generation(true)).is_err());
        reject_prefixes::<GenerationUpdate>(input, name);
    }
    for (name, body) in [
        (
            "default-generation-update-reject",
            GenerationNumber::default(),
        ),
        ("high-generation-update-reject", proposed),
        ("tie-generation-update-reject", lower_low),
    ] {
        check(input, output, name, body.clone());
        let native = generation_envelope(input, name, "GenerationUpdateReject", false, false);
        assert_eq!(decode_generation_update_reject(&native).unwrap(), body);
        let made = generation_update_reject_message(&body).unwrap();
        factory_contract(&native, &made);
        assert_eq!(
            made.body,
            fs::read(output.join(format!("{name}.bin"))).unwrap()
        );
        assert_eq!(decode_generation_update_reject(&made).unwrap(), body);
        reject_generation_envelopes(&native, false, |m| {
            decode_generation_update_reject(m).map(|_| ())
        });
        reject_prefixes::<GenerationNumber>(input, name);
    }
}

fn main() {
    let args = Arguments::parse();
    if args.identity {
        println!(
            "{}",
            option_env!("FUBE_RA_SOURCE_DIGEST").unwrap_or("unbound")
        );
        return;
    }
    let input = args
        .native_input
        .as_deref()
        .expect("clap requires native input");
    let output = args
        .rust_output
        .as_deref()
        .expect("clap requires Rust output");
    assert!(input.is_dir() && output.is_dir());
    peer_contracts(input, output);
    configuration_contracts(input, output);
    replica_contracts(input, output);
    check(input, output, "default-request", AddInstance::default());
    check(input, output, "rich-request", support::rich_request());
    check(input, output, "default-reply", AddInstanceReply::default());
    let mut primary = support::rich_request();
    primary.replica.current_role = 4;
    primary.replica.previous_role = 1;
    primary.replica.state = 1;
    primary.replica.up = true;
    let mut primary_reply = support::rich_reply(0);
    primary_reply.replica.current_role = 4;
    primary_reply.replica.previous_role = 1;
    primary_reply.replica.state = 2;
    primary_reply.replica.up = true;
    check(input, output, "default-primary", AddPrimary::default());
    check(input, output, "rich-primary", primary.clone());
    check(
        input,
        output,
        "default-primary-reply",
        AddPrimaryReply::default(),
    );
    check(input, output, "rich-primary-reply", primary_reply.clone());
    for (name, body) in [
        ("default-primary", AddPrimary::default()),
        ("rich-primary", primary),
    ] {
        let native = Message {
            headers: [
                (ACTOR, "actor-12"),
                (ACTION, "action-AddPrimary"),
                (HIGH_PRIORITY, "priority"),
                (GENERATION_HEADER, "fm-generation"),
            ]
            .into_iter()
            .map(|(id, name)| Header {
                id,
                value: fs::read(input.join(format!("{name}.bin"))).unwrap(),
            })
            .collect(),
            body: fs::read(input.join(format!("{name}.bin"))).unwrap(),
        };
        let generation = support::rich_generation(false);
        assert_eq!(
            decode_add_primary(&native).unwrap(),
            (body.clone(), generation.clone())
        );
        factory_contract(&native, &add_primary_message(&body, &generation).unwrap());
        assert!(decode_add_instance(&native).is_err());
        reject_prefixes::<AddPrimary>(input, name);
    }
    for (name, body) in [
        ("default-primary-reply", AddPrimaryReply::default()),
        ("rich-primary-reply", primary_reply),
    ] {
        for (actor, owner) in [("actor-11", Owner::Fm), ("actor-10", Owner::Fmm)] {
            let native = Message {
                headers: [
                    (ACTOR, actor),
                    (ACTION, "action-AddPrimaryReply"),
                    (HIGH_PRIORITY, "priority"),
                ]
                .into_iter()
                .map(|(id, name)| Header {
                    id,
                    value: fs::read(input.join(format!("{name}.bin"))).unwrap(),
                })
                .collect(),
                body: fs::read(input.join(format!("{name}.bin"))).unwrap(),
            };
            assert_eq!(
                decode_add_primary_reply(&native).unwrap(),
                (body.clone(), owner)
            );
            factory_contract(&native, &add_primary_reply_message(&body, owner).unwrap());
            assert!(decode_add_instance_reply(&native).is_err());
        }
        reject_prefixes::<AddPrimaryReply>(input, name);
    }
    for error in [0, -2_147_017_398, -2_147_017_441] {
        check(
            input,
            output,
            &format!("rich-reply-{error}"),
            support::rich_reply(error),
        );
    }
    check(
        input,
        output,
        "default-generation",
        GenerationHeader::default(),
    );
    check(
        input,
        output,
        "fm-generation",
        support::rich_generation(false),
    );
    check(
        input,
        output,
        "fmm-generation",
        support::rich_generation(true),
    );
    for value in [10_i64, 11, 12, 13] {
        check(input, output, &format!("actor-{value}"), Scalar { value });
    }
    check(
        input,
        output,
        "action-request",
        Scalar {
            value: "AddInstance".to_string(),
        },
    );
    check(
        input,
        output,
        "action-reply",
        Scalar {
            value: "AddInstanceReply".to_string(),
        },
    );
    check(input, output, "priority", Scalar { value: true });
    check(input, output, "default-remove", RemoveInstance::default());
    for force in [false, true] {
        check(
            input,
            output,
            &format!("rich-remove-{}", u8::from(force)),
            support::rich_remove(force),
        );
    }
    check(
        input,
        output,
        "default-remove-reply",
        RemoveInstanceReply::default(),
    );
    check(input, output, "rich-remove-reply", support::rich_reply(0));
    for (name, body) in [
        ("default-delete-replica", DeleteReplica::default()),
        ("rich-delete-replica", rich_deletion()),
    ] {
        check(input, output, name, body.clone());
        for from_fmm in [false, true] {
            let native = Message {
                headers: [
                    (ACTOR, "actor-12"),
                    (ACTION, "action-DeleteReplica"),
                    (HIGH_PRIORITY, "priority"),
                    (
                        GENERATION_HEADER,
                        if from_fmm {
                            "fmm-generation"
                        } else {
                            "fm-generation"
                        },
                    ),
                ]
                .into_iter()
                .map(|(id, name)| Header {
                    id,
                    value: fs::read(input.join(format!("{name}.bin"))).unwrap(),
                })
                .collect(),
                body: fs::read(input.join(format!("{name}.bin"))).unwrap(),
            };
            let generation = support::rich_generation(from_fmm);
            assert_eq!(
                decode_delete_replica(&native).unwrap(),
                (body.clone(), generation.clone())
            );
            let made = delete_replica_message(&body, &generation).unwrap();
            factory_contract(&native, &made);
            assert_eq!(
                decode_delete_replica(&made).unwrap(),
                (body.clone(), generation)
            );
            assert!(decode_remove_instance(&native).is_err());
            assert!(decode_add_primary(&native).is_err());
        }
        reject_prefixes::<DeleteReplica>(input, name);
    }
    for (name, body) in [
        (
            "default-delete-replica-reply",
            DeleteReplicaReply::default(),
        ),
        ("rich-delete-replica-reply-0", rich_deletion_reply(0)),
        (
            "rich-delete-replica-reply--2147017398",
            rich_deletion_reply(-2_147_017_398),
        ),
    ] {
        check(input, output, name, body.clone());
        for (actor, owner) in [("actor-11", Owner::Fm), ("actor-10", Owner::Fmm)] {
            let native = Message {
                headers: [
                    (ACTOR, actor),
                    (ACTION, "action-DeleteReplicaReply"),
                    (HIGH_PRIORITY, "priority"),
                ]
                .into_iter()
                .map(|(id, name)| Header {
                    id,
                    value: fs::read(input.join(format!("{name}.bin"))).unwrap(),
                })
                .collect(),
                body: fs::read(input.join(format!("{name}.bin"))).unwrap(),
            };
            assert_eq!(
                decode_delete_replica_reply(&native).unwrap(),
                (body.clone(), owner)
            );
            let made = delete_replica_reply_message(&body, owner).unwrap();
            factory_contract(&native, &made);
            assert_eq!(
                decode_delete_replica_reply(&made).unwrap(),
                (body.clone(), owner)
            );
            assert!(decode_remove_instance_reply(&native).is_err());
            assert!(decode_add_primary_reply(&native).is_err());
        }
        reject_prefixes::<DeleteReplicaReply>(input, name);
    }
    check(
        input,
        output,
        "explicit-empty-replica-up",
        ReplicaUp::default(),
    );
    for last in [false, true] {
        for from_fmm in [false, true] {
            check(
                input,
                output,
                &format!("rich-replica-up-{}-{}", u8::from(last), u8::from(from_fmm)),
                support::rich_replica_up(last, from_fmm),
            );
        }
    }
    for action in [
        "RemoveInstance",
        "RemoveInstanceReply",
        "ReplicaUp",
        "ReplicaUpReply",
        "AddPrimary",
        "AddPrimaryReply",
        "DeleteReplica",
        "DeleteReplicaReply",
        "ServiceTypeNotification",
        "ServiceTypeNotificationReply",
        "NodeUp",
        "NodeUpAck",
        "NodeActivateRequest",
        "NodeActivateReply",
        "NodeDeactivateRequest",
        "NodeDeactivateReply",
        "NodeUpdateServiceRequest",
        "NodeUpdateServiceReply",
        "ServiceTypeEnabled",
        "ServiceTypeDisabled",
        "ServiceTypeEnabledReply",
        "ServiceTypeDisabledReply",
        "ReplicaEndpointUpdated",
        "ReplicaEndpointUpdatedReply",
        "ReportLoad",
        "GenerationProposal",
        "GenerationProposalReply",
        "GenerationUpdate",
        "GenerationUpdateReject",
    ] {
        check(
            input,
            output,
            &format!("action-{action}"),
            Scalar {
                value: action.to_string(),
            },
        );
    }
    check(
        input,
        output,
        "empty-service-type-notification",
        ServiceTypeNotification::default(),
    );
    check(
        input,
        output,
        "rich-service-type-notification",
        support::rich_notification(),
    );
    check(
        input,
        output,
        "empty-service-type-notification-reply",
        ServiceTypeNotificationReply::default(),
    );
    check(
        input,
        output,
        "partial-service-type-notification-reply",
        support::partial_notification_reply(),
    );
    check(input, output, "priority-false", Scalar { value: false });
    for (name, expected, fresh) in [
        ("fresh-node-up", fresh_node_up(), true),
        ("rich-node-up", rich_node_up(), false),
    ] {
        check(input, output, name, expected.clone());
        let native = native_envelope(input, name, false);
        let decoded = decode_node_up(&native).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(decoded.validate_fresh_stateless().is_ok(), fresh);
        let made = node_up_message(&expected).unwrap();
        factory_contract(&native, &made);
        assert_eq!(decode_node_up(&made).unwrap(), expected);
        reject_prefixes::<NodeUp>(input, name);
    }
    let fresh_ack = NodeUpAck {
        upgrades: ObjectArray(vec![]),
        version: fresh_version(),
        activated: true,
        activation_sequence: 7,
    };
    for (name, expected, fresh) in [
        ("fresh-node-up-ack", fresh_ack, true),
        ("rich-node-up-ack", rich_ack(), false),
    ] {
        check(input, output, name, expected.clone());
        let native = native_envelope(input, name, true);
        let (decoded, generation) = decode_node_up_ack(&native).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(generation, support::rich_generation(false));
        assert_eq!(
            decoded.validate_fresh_activated(&fresh_version()).is_ok(),
            fresh
        );
        let made = node_up_ack_message(&expected, &generation).unwrap();
        factory_contract(&native, &made);
        assert_eq!(decode_node_up_ack(&made).unwrap(), (expected, generation));
        reject_prefixes::<NodeUpAck>(input, name);
    }
    check(input, output, "direct-ptop", direct_ptop());
    check(input, output, "rich-ptop", rich_ptop());
    reject_prefixes::<PToP>(input, "rich-ptop");
    for name in [
        "rich-service-type-notification",
        "partial-service-type-notification-reply",
    ] {
        let bytes = fs::read(input.join(format!("{name}.bin"))).unwrap();
        for end in 0..bytes.len() {
            assert!(from_slice::<ServiceTypeNotification>(&bytes[..end]).is_err());
        }
    }
    for force in [false, true] {
        let bytes = fs::read(input.join(format!("rich-remove-{}.bin", u8::from(force)))).unwrap();
        for end in 0..bytes.len() {
            assert!(from_slice::<RemoveInstance>(&bytes[..end]).is_err());
        }
    }
    for name in ["rich-request", "rich-reply-0"] {
        let bytes = fs::read(input.join(format!("{name}.bin"))).unwrap();
        for end in 0..bytes.len() {
            if name == "rich-request" {
                assert!(from_slice::<AddInstance>(&bytes[..end]).is_err());
            } else {
                assert!(from_slice::<AddInstanceReply>(&bytes[..end]).is_err());
            }
        }
    }
    stage_two(input, output);
    generation_handshake(input, output);
    println!("116 cases verified; all selected-body proper prefixes rejected");
}

#[cfg(test)]
mod argument_tests {
    use super::*;

    #[test]
    fn identity_and_fixture_paths_are_exclusive_and_complete() {
        assert!(
            Arguments::try_parse_from(["ra_conformance", "--identity"])
                .unwrap()
                .identity
        );
        let args = Arguments::try_parse_from(["ra_conformance", "/input", "/output"]).unwrap();
        assert_eq!(args.native_input, Some(PathBuf::from("/input")));
        assert_eq!(args.rust_output, Some(PathBuf::from("/output")));
        for args in [
            vec![],
            vec!["/input"],
            vec!["--identity", "/input", "/output"],
            vec!["/input", "/output", "extra"],
        ] {
            assert!(
                Arguments::try_parse_from(std::iter::once("ra_conformance").chain(args)).is_err()
            );
        }
    }
}
