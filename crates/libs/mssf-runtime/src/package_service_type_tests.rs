use super::tests::Fixture;
const STATEFUL: &str =
    r#"<StatefulServiceType ServiceTypeName="StatefulType" HasPersistedState="false"/>"#;

fn mixed(fixture: &Fixture, version: u32) {
    for path in [
        fixture.base.join("RustService.Package.Current.xml"),
        fixture
            .base
            .join(format!("RustService.Manifest.{version}.xml")),
    ] {
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(
            path,
            text.replace("</ServiceTypes>", &format!("{STATEFUL}</ServiceTypes>")),
        )
        .unwrap();
    }
}
#[test]
fn mixed_volatile_types_are_owned_and_configuration_only_refresh_stays_compatible() {
    let fixture = Fixture::new();
    mixed(&fixture, 1);
    let old = fixture.read_public().unwrap();
    fixture.write(2, "new");
    mixed(&fixture, 2);
    let new = fixture.read_public().unwrap();
    assert!(old.code_compatible_with(&new));
}
#[test]
fn persisted_implicit_duplicate_unknown_and_digest_mismatches_are_rejected() {
    for replacement in [
        r#"<StatefulServiceType ServiceTypeName="StatefulType" HasPersistedState="true"/>"#,
        r#"<StatefulServiceType ServiceTypeName="StatefulType" HasPersistedState="invalid"/>"#,
        r#"<StatefulServiceType ServiceTypeName="StatefulType" UseImplicitHost="false"/>"#,
        r#"<StatefulServiceType ServiceTypeName="Type" HasPersistedState="false"/>"#,
        r#"<StatefulServiceGroupType ServiceTypeName="StatefulType"/>"#,
    ] {
        let fixture = Fixture::new();
        mixed(&fixture, 1);
        for path in [
            fixture.base.join("RustService.Package.Current.xml"),
            fixture.base.join("RustService.Manifest.1.xml"),
        ] {
            let text = std::fs::read_to_string(&path).unwrap();
            std::fs::write(path, text.replace(STATEFUL, replacement)).unwrap();
        }
        assert!(fixture.read_public().is_err(), "{replacement}");
    }
    let fixture = Fixture::new();
    mixed(&fixture, 1);
    let current = fixture.base.join("RustService.Package.Current.xml");
    let text = std::fs::read_to_string(&current).unwrap();
    std::fs::write(current, text.replace(STATEFUL, "")).unwrap();
    assert!(fixture.read_public().is_err());
}
#[test]
fn changing_declared_type_identity_changes_the_code_profile_not_just_configuration() {
    let fixture = Fixture::new();
    mixed(&fixture, 1);
    let old = fixture.read_public().unwrap();
    for path in [
        fixture.base.join("RustService.Package.Current.xml"),
        fixture.base.join("RustService.Manifest.1.xml"),
    ] {
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(path, text.replace("StatefulType", "NewStatefulType")).unwrap();
    }
    let changed = fixture.read_public().unwrap();
    assert!(!old.code_compatible_with(&changed));
}
#[test]
fn native_metric_expansion_and_boolean_defaults_are_semantically_equivalent() {
    let fixture = Fixture::new();
    let manifest = fixture.base.join("RustService.Manifest.1.xml");
    let current = fixture.base.join("RustService.Package.Current.xml");
    let declared = r#"<StatelessServiceType ServiceTypeName="Type"><LoadMetrics><LoadMetric Name="Work" DefaultLoad="0" Weight="Low"/></LoadMetrics></StatelessServiceType>"#;
    let digested = r#"<StatelessServiceType ServiceTypeName="Type" UseImplicitHost="false"><LoadMetrics><LoadMetric Name="Work" DefaultLoad="0" PrimaryDefaultLoad="0" SecondaryDefaultLoad="0" Weight="Low"/></LoadMetrics></StatelessServiceType>"#;
    for (path, value) in [(&manifest, declared), (&current, digested)] {
        let text = std::fs::read_to_string(path).unwrap();
        std::fs::write(
            path,
            text.replace(r#"<StatelessServiceType ServiceTypeName="Type"/>"#, value),
        )
        .unwrap();
    }
    fixture.read_public().unwrap();
    let text = std::fs::read_to_string(&current).unwrap();
    std::fs::write(
        &current,
        text.replace(r#"PrimaryDefaultLoad="0""#, r#"PrimaryDefaultLoad="1""#),
    )
    .unwrap();
    assert!(fixture.read_public().is_err());
}
