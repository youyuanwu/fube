//! Compiler-free consumer of independently native-produced fixtures.
mod support;

use fube_ra::protocol::*;
use fube_transport::{
    message::{Header, Message, ACTION, ACTOR, HIGH_PRIORITY},
    serialization::serde::{from_slice, to_vec, CountedStrings, ObjectArray},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{fmt::Debug, fs, path::Path};
use support::Rich;

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

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() == 2 && args[1] == "--identity" {
        println!(
            "{}",
            option_env!("FUBE_RA_SOURCE_DIGEST").unwrap_or("unbound")
        );
        return;
    }
    assert_eq!(
        args.len(),
        3,
        "usage: ra_conformance NATIVE_INPUT RUST_OUTPUT"
    );
    let input = Path::new(&args[1]);
    let output = Path::new(&args[2]);
    assert!(input.is_dir() && output.is_dir());
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
    println!("58 cases verified; all selected-body proper prefixes rejected");
}
