//! Rust differential regression to the already-evidenced production RA helper,
//! not independent native H1 execution. Native source: ServiceModel identifiers
//! and pal/wc16.c:131-150. No fixture/runtime name restrictions are applied.
mod common;
use common::*;
use fube_hosting::*;
use fube_ra::{entity as ra, protocol as wire};
use std::cmp::Ordering;

fn ra_type(id: &ServiceTypeId) -> wire::ServiceType {
    wire::ServiceType {
        package: wire::PackageId {
            application: wire::ApplicationId {
                type_name: id.package.application.type_name.clone(),
                number: id.package.application.number,
            },
            name: id.package.name.clone(),
        },
        name: id.name.clone(),
    }
}
#[test]
fn all_pal_operands_match_existing_production_helper() {
    for unit in 0..=u16::MAX {
        assert_eq!(
            native_upper_unit(unit),
            ra::native_upper_unit(unit),
            "{unit:x}"
        );
    }
}
#[test]
fn text_and_structured_differential_without_normalization() {
    let names = [
        "",
        "Widget",
        "widget",
        "é",
        "É",
        "σ",
        "ς",
        "𐐀",
        "𐐨",
        "a\0ignored",
        "A",
        "\u{7fff}",
        "\u{8000}",
        "\0App",
    ];
    let mut ids = Vec::new();
    for (n, app) in [(0, ""), (0, "\0App"), (1, "App"), (2, "app"), (1, "é")] {
        for pkg in ["", "Pkg", "pkg", "Other"] {
            for name in names {
                ids.push(ServiceTypeId {
                    package: PackageId {
                        application: ApplicationId {
                            type_name: app.into(),
                            number: n,
                        },
                        name: pkg.into(),
                    },
                    name: name.into(),
                });
            }
        }
    }
    for a in &ids {
        for b in &ids {
            assert_eq!(
                compare_service_types(a, b),
                ra::compare_service_types(&ra_type(a), &ra_type(b))
            );
        }
    }
    for a in names {
        for b in names {
            let mut left = ra_type(&ServiceTypeId::default());
            let mut right = left.clone();
            left.name = a.into();
            right.name = b.into();
            assert_eq!(
                compare_names(a, b),
                ra::compare_service_types(&left, &right)
            );
        }
    }
    assert_ne!(compare_names("é", "É"), Ordering::Equal);
    assert_eq!(compare_names("a\0x", "A"), Ordering::Equal);
    assert_eq!(compare_names("\u{8000}", "\u{7fff}"), Ordering::Less);
}
#[test]
fn type_instance_uses_package_even_for_empty_apps_and_public_is_not_key() {
    let mut a = type_id("Widget");
    a.service_type.package.application = ApplicationId::default();
    let mut b = a.clone();
    b.service_type.package.name = "Other".into();
    assert_eq!(
        compare_service_types(&a.service_type, &b.service_type),
        Ordering::Equal
    );
    assert_ne!(compare_type_instances(&a, &b), Ordering::Equal);
    b = a.clone();
    b.public_activation = uuid::Uuid::from_u128(2).to_string();
    assert_eq!(compare_type_instances(&a, &b), Ordering::Equal);
    a.activation = Activation::from_uuid(uuid::Uuid::from_u128(0xff));
    b.activation = Activation::from_uuid(uuid::Uuid::from_u128(0x100));
    assert_eq!(compare_type_instances(&a, &b), Ordering::Less);
}
#[test]
fn native_number_precedes_application_name() {
    let mut a = package("Pkg", 1);
    let b = package("Pkg", 2);
    a.application.type_name = "Z".into();
    assert_eq!(compare_packages(&a, &b), Ordering::Less);
}
#[test]
fn native_guid_shape_rejects_uuid_parser_extensions_and_preserves_spelling() {
    let mut h = hosting();
    completed_host(&mut h, "H");
    for public in [
        "00112233445566778899aabbccddeeff",
        "{00112233-4455-6677-8899-aabbccddeeff}",
        "urn:uuid:00112233-4455-6677-8899-aabbccddeeff",
        "00112233_4455-6677-8899-aabbccddeeff",
    ] {
        let mut c = context("R", "H");
        c.package.public_activation = public.into();
        assert_eq!(h.add_runtime(c), Err(Error::InvalidInput));
        assert!(h.runtime_records().is_empty());
    }
    for public in [
        "00112233-4455-6677-8899-aabbccddeeff",
        "00112233-4455-6677-8899-AABBCCDDEEFF",
    ] {
        let mut c = context("R", "H");
        c.package.public_activation = public.into();
        c.package.activation = Activation::from_uuid(uuid::Uuid::from_u128(1));
        assert_eq!(h.register_runtime(c.clone()).result, Ok(()));
        assert_eq!(
            h.find_runtime("R").unwrap().package.public_activation,
            public
        );
        h.remove_runtime_entry("R").unwrap();
    }
    let mut c = context("R", "H");
    c.package.activation = Activation::from_uuid(uuid::Uuid::from_u128(1));
    assert_eq!(h.add_runtime(c), Err(Error::InvalidInput));
}

#[test]
fn native_constructor_preconditions_veto_core_admission_not_raw_comparison_data() {
    for (package, error) in [
        (
            PackageId {
                application: ApplicationId {
                    type_name: String::new(),
                    number: 1,
                },
                name: "Pkg".into(),
            },
            Error::NativeInvariant("empty application name with nonzero number"),
        ),
        (
            PackageId {
                application: ApplicationId {
                    type_name: "App".into(),
                    number: 1,
                },
                name: String::new(),
            },
            Error::NativeInvariant("nonempty application with empty package name"),
        ),
    ] {
        let mut h = hosting();
        completed_host(&mut h, "H");
        let id = TypeInstance {
            service_type: ServiceTypeId {
                package: package.clone(),
                name: "Widget".into(),
            },
            ..TypeInstance::default()
        };
        // Raw native comparator probes remain possible even for constructor-invalid triples.
        assert_eq!(
            compare_service_types(&id.service_type, &id.service_type),
            Ordering::Equal
        );
        assert_eq!(
            h.declare_type(id.clone(), "component".into(), Ok(())),
            Err(error.clone())
        );
        let mut c = context("R", "H");
        c.package.package = package.clone();
        assert_eq!(h.register_runtime(c.clone()).result, Err(error.clone()));
        assert!(h.runtime_records().is_empty());
        assert_eq!(h.register_type(&id, &c).result, Err(error.clone()));
        assert_eq!(
            h.lookup(&request(&id, c.version.clone()), &LookupModel::default()),
            Err(error.clone())
        );
        let mapping = PackageInstance {
            package,
            activation: Activation::from_uuid(uuid::Uuid::from_u128(1)),
            public_activation: uuid::Uuid::from_u128(2).to_string(),
        };
        assert_eq!(h.bind_public_activation(mapping), Err(error));
        assert!(h.type_entries().is_empty());
        assert!(h.closed_registered_events().is_empty());
        assert!(h.obligations().is_empty());
        assert_eq!(h.get_type_state(&type_id("Absent")).unwrap().sequence, 1);
    }
}
#[test]
fn valid_raw_emptiness_non_ascii_and_nul_remain_distinct_from_adhoc_capability() {
    let mut h = hosting();
    completed_host(&mut h, "H");
    let mut id = type_id("Widget");
    id.service_type.package.application.type_name = "é\0App".into();
    id.service_type.package.name = "Pkg\0suffix".into();
    h.declare_type(id.clone(), "component".into(), Ok(()))
        .unwrap();
    let mut c = context("R", "H");
    c.package = id.package_instance();
    h.register_runtime(c.clone()).result.unwrap();
    h.register_type(&id, &c).result.unwrap();
    let result = h
        .lookup(&request(&id, c.version), &LookupModel::default())
        .unwrap();
    assert_eq!(result.result, Ok(()));
    assert_eq!(result.candidate.unwrap().type_instance, id);
    let adhoc = TypeInstance::default();
    assert_eq!(
        h.declare_type(adhoc.clone(), "component".into(), Ok(())),
        Err(Error::UnsupportedCapability)
    );
    let result = h
        .lookup(
            &request(&adhoc, VersionInstance::default()),
            &LookupModel::default(),
        )
        .unwrap();
    assert_eq!(result.result, Err(Error::UnsupportedCapability));
    assert!(result.sequence.is_some()); // valid identity, gated producer; not an invalid constructor
}
