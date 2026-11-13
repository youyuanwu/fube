//! Source-order oracles use the low-level codec, not the derived schemas under
//! test. They catch reordered same-typed fields, missing object/derived scopes,
//! string-vector metadata and map flattening. This is source conformance only;
//! no native serializer or native node is executed by these tests.

use super::*;
use crate::protocol::{PackageVersion, RA_ACTOR};
use fube_transport::{
    message::Header,
    serialization::{
        serde::{from_slice, to_vec},
        Encoder,
    },
};

fn version() -> FabricVersionInstance {
    FabricVersionInstance {
        version: FabricVersion {
            code: FabricCodeVersion {
                major: 3,
                minor: 4,
                build: 5,
                hotfix: 6,
            },
            config: FabricConfigVersion {
                value: "cfg".into(),
            },
        },
        instance: u64::MAX - 1,
    }
}

fn package(application: &str, number: u32, name: &str) -> PackageId {
    PackageId {
        application: ApplicationId {
            type_name: application.into(),
            number,
        },
        name: name.into(),
    }
}

fn rich_node_up() -> NodeUp {
    NodeUp {
        node: NodeDescription {
            upgrade_domain: "UD".into(),
            fault_domains: ObjectArray(vec![Uri {
                kind: 2,
                scheme: "fd".into(),
                authority: "authority".into(),
                host_kind: 3,
                host: "host".into(),
                port: 123,
                path: "/rack/node".into(),
                query: "q".into(),
                fragment: "f".into(),
                segments: CountedStrings(vec!["rack".into(), "node".into()]),
            }]),
            properties: ObjectArray(vec![MapEntry {
                key: "property".into(),
                value: "value".into(),
            }]),
            capacity_ratios: ObjectArray(vec![MapEntry {
                key: "ratio".into(),
                value: 17,
            }]),
            capacities: ObjectArray(vec![MapEntry {
                key: "capacity".into(),
                value: 29,
            }]),
            version: version(),
            name: "node-2".into(),
            node_type: "stateless".into(),
            address: "127.0.0.2".into(),
            cluster_connection_port: 45000,
            http_gateway_port: 19080,
        },
        packages: ObjectArray(vec![ServicePackageInfo {
            package: package("AppType", 12, "Pkg"),
            version: VersionInstance {
                version: PackageVersion {
                    application: ApplicationVersion {
                        value: Rollout { major: 2, minor: 3 },
                    },
                    value: Rollout { major: 4, minor: 5 },
                },
                instance: 77,
            },
        }]),
        any_replica_found: true,
    }
}

fn rich_ack() -> NodeUpAck {
    NodeUpAck {
        upgrades: ObjectArray(vec![UpgradeDescription {
            specification: ApplicationUpgradeSpecification {
                application: ApplicationId {
                    type_name: "UpgradeAppType".into(),
                    number: 11,
                },
                version: ApplicationVersion {
                    value: Rollout { major: 2, minor: 3 },
                },
                instance: u64::MAX,
                upgrade_type: -91,
                monitored: true,
                packages: ObjectArray(vec![ServicePackageUpgradeSpecification {
                    package_name: "UpgradePkg".into(),
                    rollout: Rollout { major: 4, minor: 5 },
                    code_package_info_included: true,
                    code_package_names: CountedStrings(vec!["CodeA".into(), "CodeB".into()]),
                }]),
                application_name: "fabric:/app".into(),
                removed_types: ObjectArray(vec![ServiceTypeRemovalSpecification {
                    package_name: "RemovedPkg".into(),
                    rollout: Rollout { major: 6, minor: 7 },
                    type_names: CountedStrings(vec!["TypeA".into(), "TypeB".into()]),
                }]),
                manual: true,
                resource_governance: ObjectArray(vec![MapEntry {
                    key: package("GovernedType", 13, "GovernedPkg"),
                    value: ServicePackageResourceGovernance {
                        governed: true,
                        legacy_cpu_cores: 3,
                        memory_mb: 1024,
                        cpu_cores: 2.5,
                    },
                }]),
                container_images: ObjectArray(vec![MapEntry {
                    key: package("ImagesType", 14, "ImagesPkg"),
                    value: CodePackageContainerImages {
                        images: CountedStrings(vec!["image-a".into(), "image-b".into()]),
                    },
                }]),
                networks: CountedStrings(vec!["network-a".into(), "network-b".into()]),
            },
        }]),
        version: version(),
        activated: false,
        activation_sequence: -9,
    }
}

fn source_version(e: &mut Encoder) -> Result<()> {
    e.object(|e| {
        e.object(|e| {
            e.object(|e| {
                e.u32(3);
                e.u32(4);
                e.u32(5);
                e.u32(6);
                Ok(())
            })?;
            e.object(|e| e.string("cfg"))
        })?;
        e.u64(u64::MAX - 1);
        Ok(())
    })
}

fn source_rollout(e: &mut Encoder, major: u32, minor: u32) -> Result<()> {
    e.object(|e| {
        e.u32(major);
        e.u32(minor);
        Ok(())
    })
}

fn source_package(e: &mut Encoder, application: &str, number: u32, name: &str) -> Result<()> {
    e.object(|e| {
        e.object(|e| {
            e.string(application)?;
            e.u32(number);
            Ok(())
        })?;
        e.string(name)
    })
}

fn source_node_up() -> Vec<u8> {
    let mut e = Encoder::new();
    e.object(|e| {
        e.object(|e| {
            e.string("UD")?;
            e.object_array(&[()], |e, _| {
                e.i64(2);
                e.string("fd")?;
                e.string("authority")?;
                e.i64(3);
                e.string("host")?;
                e.i32(123);
                e.string("/rack/node")?;
                e.string("q")?;
                e.string("f")?;
                e.u32(2);
                e.string("rack")?;
                e.string("node")
            })?;
            e.object_array(&[()], |e, _| {
                e.string("property")?;
                e.string("value")
            })?;
            e.object_array(&[()], |e, _| {
                e.string("ratio")?;
                e.u32(17);
                Ok(())
            })?;
            e.object_array(&[()], |e, _| {
                e.string("capacity")?;
                e.u32(29);
                Ok(())
            })?;
            source_version(e)?;
            e.string("node-2")?;
            e.string("stateless")?;
            e.string("127.0.0.2")?;
            e.u32(45000);
            e.u16(19080);
            Ok(())
        })?;
        e.object_array(&[()], |e, _| {
            source_package(e, "AppType", 12, "Pkg")?;
            e.object(|e| {
                e.object(|e| {
                    e.object(|e| source_rollout(e, 2, 3))?;
                    source_rollout(e, 4, 5)
                })?;
                e.u64(77);
                Ok(())
            })
        })?;
        e.boolean(true);
        Ok(())
    })
    .unwrap();
    e.finish()
}

fn source_ack() -> Vec<u8> {
    let mut e = Encoder::new();
    e.object(|e| {
        e.object_array(&[()], |e, _| {
            e.object(|e| {
                e.object(|e| {
                    e.string("UpgradeAppType")?;
                    e.u32(11);
                    Ok(())
                })?;
                e.object(|e| source_rollout(e, 2, 3))?;
                e.u64(u64::MAX);
                e.i64(-91);
                e.boolean(true);
                e.object_array(&[()], |e, _| {
                    e.string("UpgradePkg")?;
                    source_rollout(e, 4, 5)?;
                    e.boolean(true);
                    e.u32(2);
                    e.string("CodeA")?;
                    e.string("CodeB")
                })?;
                e.string("fabric:/app")?;
                e.object_array(&[()], |e, _| {
                    e.string("RemovedPkg")?;
                    source_rollout(e, 6, 7)?;
                    e.u32(2);
                    e.string("TypeA")?;
                    e.string("TypeB")
                })?;
                e.boolean(true);
                e.object_array(&[()], |e, _| {
                    source_package(e, "GovernedType", 13, "GovernedPkg")?;
                    e.object(|e| {
                        e.boolean(true);
                        e.u32(3);
                        e.u32(1024);
                        e.f64(2.5);
                        Ok(())
                    })
                })?;
                e.object_array(&[()], |e, _| {
                    source_package(e, "ImagesType", 14, "ImagesPkg")?;
                    e.object(|e| {
                        e.u32(2);
                        e.string("image-a")?;
                        e.string("image-b")
                    })
                })?;
                e.u32(2);
                e.string("network-a")?;
                e.string("network-b")
            })
        })?;
        source_version(e)?;
        e.boolean(false);
        e.i64(-9);
        Ok(())
    })
    .unwrap();
    e.finish()
}

#[test]
fn node_up_matches_source_scopes_order_map_uri_and_package_closure() {
    let expected = source_node_up();
    assert_eq!(to_vec(&rich_node_up()).unwrap(), expected);
    assert_eq!(from_slice::<NodeUp>(&expected).unwrap(), rich_node_up());
}

#[test]
fn ack_matches_full_source_upgrade_closure_without_discarding_unsupported_data() {
    let expected = source_ack();
    assert_eq!(to_vec(&rich_ack()).unwrap(), expected);
    assert_eq!(from_slice::<NodeUpAck>(&expected).unwrap(), rich_ack());
    let message = node_up_ack_message(&rich_ack(), &generation()).unwrap();
    let (decoded, actual_generation) = decode_node_up_ack(&message).unwrap();
    assert_eq!(decoded, rich_ack());
    assert_eq!(actual_generation, generation());
    assert!(decoded.validate_fresh_activated(&version()).is_err());
}

fn generation() -> GenerationHeader {
    GenerationHeader {
        number: crate::protocol::GenerationNumber {
            generation: -3,
            owner: [17; 16],
        },
        for_fm_replica: false,
    }
}

#[test]
fn fresh_constructors_and_factories_preserve_native_header_asymmetry() {
    let version = FabricVersionInstance::native_default();
    assert_eq!(version.version.code.major, 1);
    assert_eq!(version.version.code.build, 960);
    assert_eq!(version.version.config.value, "__default__");
    assert_eq!(FabricVersionInstance::default().version.code.major, 0);
    let node = NodeUp::fresh_stateless(
        "node-1".into(),
        "stateless".into(),
        "127.0.0.1:25001".parse().unwrap(),
        version.clone(),
    )
    .unwrap();
    assert_eq!(node.node.name, "node-1");
    assert_eq!(node.node.address, "127.0.0.1");
    assert_eq!(node.node.cluster_connection_port, 25001);
    assert_eq!(node.node.http_gateway_port, 0);
    assert_eq!(node.node.version, version);
    node.validate_fresh_stateless().unwrap();
    let message = node_up_message(&node).unwrap();
    assert!(!message.headers.iter().any(|h| h.id == GENERATION_HEADER));
    assert_eq!(
        unique(&message, ACTOR).unwrap().read(|d| d.i64()).unwrap(),
        11
    );
    assert_eq!(
        unique(&message, ACTION)
            .unwrap()
            .read(|d| d.string())
            .unwrap(),
        "NodeUp"
    );
    assert!(unique(&message, HIGH_PRIORITY)
        .unwrap()
        .read(|d| d.boolean())
        .unwrap());
    assert_eq!(decode_node_up(&message).unwrap(), node);
    let ack = NodeUpAck::fresh_activated(version.clone(), 42).unwrap();
    ack.validate_fresh_activated(&version).unwrap();
    assert_eq!(ack.activation_sequence, 42);
    let reply = node_up_ack_message(&ack, &generation()).unwrap();
    assert_eq!(
        unique(&reply, ACTOR).unwrap().read(|d| d.i64()).unwrap(),
        13
    );
    assert_eq!(
        unique(&reply, ACTION)
            .unwrap()
            .read(|d| d.string())
            .unwrap(),
        "NodeUpAck"
    );
    assert!(unique(&reply, HIGH_PRIORITY)
        .unwrap()
        .read(|d| d.boolean())
        .unwrap());
    assert_eq!(decode_node_up_ack(&reply).unwrap(), (ack, generation()));
    assert!(decode_node_up(&reply).is_err());
    assert!(decode_node_up_ack(&message).is_err());
}

#[test]
fn explicit_profile_validation_rejects_recovery_upgrades_and_inactive_targets() {
    let mut node = rich_node_up();
    node.any_replica_found = false;
    assert!(node.validate_fresh_stateless().is_err());
    node.packages.0.clear();
    node.any_replica_found = true;
    assert!(node.validate_fresh_stateless().is_err());
    node.any_replica_found = false;
    node.validate_fresh_stateless().unwrap();

    let valid = NodeUpAck::fresh_activated(version(), 0).unwrap();
    let mut unsupported = valid.clone();
    unsupported.upgrades = rich_ack().upgrades;
    assert!(unsupported.validate_fresh_activated(&version()).is_err());
    unsupported = valid.clone();
    unsupported.version.instance -= 1;
    assert!(unsupported.validate_fresh_activated(&version()).is_err());
    unsupported = valid.clone();
    unsupported.activated = false;
    assert!(unsupported.validate_fresh_activated(&version()).is_err());
    unsupported = valid;
    unsupported.activation_sequence = -1;
    assert!(unsupported.validate_fresh_activated(&version()).is_err());
    assert!(NodeUpAck::fresh_activated(version(), -1).is_err());
    for address in ["0.0.0.0:1", "224.0.0.1:1", "127.0.0.1:0"] {
        assert!(NodeUp::fresh_stateless(
            "node".into(),
            "stateless".into(),
            address.parse().unwrap(),
            version(),
        )
        .is_err());
    }
}

#[test]
fn fresh_node_up_preserves_container_addresses_through_native_serialization() {
    for address in ["192.0.2.11:19002", "[2001:db8::11]:19002"] {
        let address: SocketAddr = address.parse().unwrap();
        let node =
            NodeUp::fresh_stateless("node".into(), "stateful".into(), address, version()).unwrap();
        let decoded = decode_node_up(&node_up_message(&node).unwrap()).unwrap();
        assert_eq!(decoded.node.address, address.ip().to_string());
        assert_eq!(
            decoded.node.cluster_connection_port,
            u32::from(address.port())
        );
        decoded.validate_fresh_stateless().unwrap();
    }
}

#[test]
fn bootstrap_rejects_wrong_missing_duplicate_and_malformed_contract_headers() {
    let request = node_up_message(&rich_node_up()).unwrap();
    let reply = node_up_ack_message(&rich_ack(), &generation()).unwrap();
    for (original, ack) in [(request, false), (reply, true)] {
        let decode = |message: &Message| {
            if ack {
                decode_node_up_ack(message).map(|_| ())
            } else {
                decode_node_up(message).map(|_| ())
            }
        };
        let mut required = vec![ACTOR, ACTION, HIGH_PRIORITY];
        if ack {
            required.push(GENERATION_HEADER);
        }
        for id in required {
            let mut bad = original.clone();
            bad.headers.retain(|h| h.id != id);
            assert!(decode(&bad).is_err());
            let mut bad = original.clone();
            bad.headers.push(unique(&original, id).unwrap().clone());
            assert!(decode(&bad).is_err());
            let mut bad = original.clone();
            bad.headers
                .iter_mut()
                .find(|h| h.id == id)
                .unwrap()
                .value
                .clear();
            assert!(decode(&bad).is_err());
        }
        let mut bad = original;
        bad.headers
            .iter_mut()
            .find(|h| h.id == ACTOR)
            .unwrap()
            .value = Header::actor(RA_ACTOR).unwrap().value;
        assert!(decode(&bad).is_err());
    }
}

#[test]
fn bounded_decode_rejects_truncation_trailing_bytes_and_reservation_exhaustion() {
    for (original, ack) in [
        (node_up_message(&rich_node_up()).unwrap(), false),
        (
            node_up_ack_message(&rich_ack(), &generation()).unwrap(),
            true,
        ),
    ] {
        let decode = |message: &Message| {
            if ack {
                decode_node_up_ack(message).map(|_| ())
            } else {
                decode_node_up(message).map(|_| ())
            }
        };
        let mut bad = original.clone();
        for end in 0..original.body.len() {
            bad.body = original.body[..end].to_vec();
            assert!(decode(&bad).is_err());
        }
        bad = original.clone();
        bad.body.push(0);
        assert!(decode(&bad).is_err());
        bad.body = vec![0; BOOTSTRAP_MAX_BODY_BYTES + 1];
        assert!(matches!(decode(&bad), Err(Error::ResourceLimit(_))));
        bad = original.clone();
        bad.headers.push(Header {
            id: 0xffff,
            value: vec![0; BOOTSTRAP_MAX_HEADER_BYTES],
        });
        assert!(matches!(decode(&bad), Err(Error::ResourceLimit(_))));
        bad = original.clone();
        bad.headers
            .extend((0..BOOTSTRAP_MAX_HEADERS).map(|_| Header {
                id: 0xffff,
                value: vec![],
            }));
        assert!(matches!(decode(&bad), Err(Error::ResourceLimit(_))));
        let mut charges = vec![];
        let mut reserve = |bytes| {
            charges.push(bytes);
            Err(Error::ResourceLimit("test reservation"))
        };
        let result = if ack {
            decode_node_up_ack_with_budget(&original, &mut reserve).map(|_| ())
        } else {
            decode_node_up_with_budget(&original, &mut reserve).map(|_| ())
        };
        assert!(matches!(result, Err(Error::ResourceLimit(_))));
        assert_eq!(charges.len(), 1);
    }
}

#[test]
fn caller_reservations_cover_body_allocations_not_only_headers() {
    let mut node = rich_node_up();
    node.node.name = "n".repeat(8192);
    let mut ack = rich_ack();
    ack.version.version.config.value = "c".repeat(8192);
    for (message, is_ack) in [
        (node_up_message(&node).unwrap(), false),
        (node_up_ack_message(&ack, &generation()).unwrap(), true),
    ] {
        let mut charges = vec![];
        let mut reserve = |bytes| {
            charges.push(bytes);
            if bytes > BOOTSTRAP_MAX_HEADER_BYTES {
                Err(Error::ResourceLimit("test body reservation"))
            } else {
                Ok(())
            }
        };
        let result = if is_ack {
            decode_node_up_ack_with_budget(&message, &mut reserve).map(|_| ())
        } else {
            decode_node_up_with_budget(&message, &mut reserve).map(|_| ())
        };
        assert!(matches!(result, Err(Error::ResourceLimit(_))));
        assert!(charges.iter().any(|&bytes| bytes >= 8192));
    }
}

#[test]
fn compact_wire_arrays_cannot_amplify_past_default_allocation_budget() {
    let mut node = NodeUp::default();
    node.node.fault_domains = ObjectArray(vec![
        Uri {
            kind: 0,
            scheme: String::new(),
            authority: String::new(),
            host_kind: 0,
            host: String::new(),
            port: 0,
            path: String::new(),
            query: String::new(),
            fragment: String::new(),
            segments: CountedStrings(vec![]),
        };
        2000
    ]);
    let message = node_up_message(&node).unwrap();
    assert!(message.body.len() < BOOTSTRAP_MAX_BODY_BYTES);
    assert!(matches!(
        decode_node_up(&message),
        Err(Error::ResourceLimit("bootstrap decode allocation"))
    ));
}
