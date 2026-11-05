use super::*;
use crate::deployment::Deployment;

const APP: &str = r#"<ApplicationManifest xmlns="http://schemas.microsoft.com/2011/01/fabric" ApplicationTypeName="OtherApp" ApplicationTypeVersion="v2">
<Parameters><Parameter Name="Count" DefaultValue="1"/><Parameter Name="Service" DefaultValue="Default"/><Parameter Name="Args" DefaultValue="--name a&amp;b"/></Parameters>
<ServiceManifestImport><ServiceManifestRef ServiceManifestName="OtherPackage" ServiceManifestVersion="v3"/></ServiceManifestImport>
<DefaultServices><Service Name="[Service]" ServicePackageActivationMode="SharedProcess"><StatelessService ServiceTypeName="OtherType" InstanceCount="[Count]"><SingletonPartition/></StatelessService></Service></DefaultServices></ApplicationManifest>"#;
const SERVICE: &str = r#"<ServiceManifest xmlns="http://schemas.microsoft.com/2011/01/fabric" Name="OtherPackage" Version="v3">
<ServiceTypes><StatelessServiceType ServiceTypeName="OtherType"/></ServiceTypes>
<CodePackage Name="Bin" Version="v4"><EntryPoint><ExeHost><Program>bin/program</Program><Arguments>[Args]</Arguments></ExeHost></EntryPoint></CodePackage>
<Resources><Endpoints><Endpoint Name="Control" Protocol="tcp" Type="Internal" Port="32123"/></Endpoints></Resources></ServiceManifest>"#;
fn stateful_manifests() -> (String, String) {
    (
        APP.replace("StatelessService", "StatefulService").replace(
            r#"InstanceCount="[Count]""#,
            r#"TargetReplicaSetSize="[Count]" MinReplicaSetSize="1""#,
        ),
        SERVICE.replace(
            r#"<StatelessServiceType ServiceTypeName="OtherType"/>"#,
            r#"<StatefulServiceType ServiceTypeName="OtherType" HasPersistedState="false"/>"#,
        ),
    )
}

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("manifest-input-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(root.join("OtherPackage/Bin/bin")).unwrap();
        fs::copy(
            std::env::current_exe().unwrap(),
            root.join("OtherPackage/Bin/bin/program"),
        )
        .unwrap();
        let fixture = Self(root);
        fixture.write(APP, SERVICE);
        fixture
    }
    fn write(&self, app: &str, service: &str) {
        fs::write(self.0.join("ApplicationManifest.xml"), app).unwrap();
        fs::write(self.0.join("OtherPackage/ServiceManifest.xml"), service).unwrap();
    }
    fn load(&self, overrides: &[(String, String)]) -> Result<Package> {
        Package::load(&self.0, "fabric:/Another/NamedApp", overrides)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn defaults_overrides_and_native_layout_are_not_fixture_constants() {
    let fixture = Fixture::new();
    fs::write(
        fixture.0.join("OtherPackage/Bin/bin/auxiliary.dat"),
        b"nested content",
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            fixture.0.join("OtherPackage/Bin/bin/auxiliary.dat"),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();
    }
    let defaults = fixture.load(&[]).unwrap();
    assert_eq!(defaults.service_name, "Default");
    assert_eq!(defaults.application_version, "v2");
    assert_eq!(defaults.working_folder, "Work");
    assert_eq!(defaults.arguments, ["--name", "a&b"]);
    let package = fixture
        .load(&[
            ("Service".into(), "Overridden".into()),
            ("Args".into(), "a<b c>d".into()),
        ])
        .unwrap();
    assert_eq!(package.service_name, "Overridden");
    assert_eq!(
        package.endpoint_file(),
        "Control;tcp;Internal;32123;;;;;127.0.0.1"
    );
    assert_eq!(package.endpoint_file().split(';').count(), 9);
    let bundle = Deployment::from_manifest(&std::env::temp_dir(), &package).unwrap();
    assert!(bundle.executable.is_file());
    assert_eq!(
        fs::read(bundle.code.join("bin/auxiliary.dat")).unwrap(),
        b"nested content"
    );
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(bundle.code.join("bin/auxiliary.dat"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o640
        );
    }
    assert_eq!(bundle.arguments, ["a<b", "c>d"]);
    assert_eq!(package.service_manifest, "OtherPackage");
    assert_eq!(package.application_name, "fabric:/Another/NamedApp");
    assert_eq!(bundle.working_directory, bundle.application.join("work"));
    assert_eq!(bundle.service_name, "fabric:/Another/NamedApp/Overridden");
    for file in [
        "App.1.0.xml",
        "OtherPackage.Package.Current.xml",
        "OtherPackage.Manifest.v3.xml",
        "OtherPackage.Endpoints.txt",
        "Application.Sids.txt",
    ] {
        assert!(bundle.application.join(file).is_file(), "{file}");
        if file.ends_with(".xml") {
            let xml = fs::read_to_string(bundle.application.join(file)).unwrap();
            let parsed = document(&xml).unwrap();
            assert_eq!(parsed.root_element().tag_name().namespace(), Some(NS));
            if file.contains("Manifest") {
                assert!(xml.contains("a&lt;b c&gt;d"));
                assert_eq!(
                    parsed
                        .descendants()
                        .find(|n| n.has_tag_name((NS, "Arguments")))
                        .unwrap()
                        .text(),
                    Some("a<b c>d")
                );
                assert_eq!(parsed.root_element().attribute("Version"), Some("v3"));
                assert_eq!(
                    parsed
                        .descendants()
                        .find(|n| n.has_tag_name((NS, "CodePackage")))
                        .unwrap()
                        .attribute("Version"),
                    Some("v4")
                );
            }
        }
    }
    fs::remove_dir_all(bundle.root).unwrap();
}

#[test]
fn volatile_stateful_registration_requires_opt_in_and_materializes_both_type_declarations() {
    let fixture = Fixture::new();
    let (app, service) = stateful_manifests();
    fixture.write(&app, &service);
    assert!(
        fixture.load(&[]).is_err(),
        "default admission stays stateless"
    );
    for profile in [
        Profile::VolatileStatefulRegistration,
        Profile::VolatileStatefulLifecycle,
    ] {
        let package =
            Package::load_with_profile(&fixture.0, "fabric:/Local", &[], profile).unwrap();
        let bundle = Deployment::from_manifest(&std::env::temp_dir(), &package).unwrap();
        for file in [
            "OtherPackage.Manifest.v3.xml",
            "OtherPackage.Package.Current.xml",
        ] {
            let xml = fs::read_to_string(bundle.application.join(file)).unwrap();
            let doc = document(&xml).unwrap();
            let declaration = doc
                .descendants()
                .find(|n| n.has_tag_name((NS, "StatefulServiceType")))
                .unwrap();
            assert_eq!(declaration.attribute("ServiceTypeName"), Some("OtherType"));
            assert_eq!(declaration.attribute("HasPersistedState"), Some("false"));
            assert!(!doc
                .descendants()
                .any(|n| n.has_tag_name((NS, "StatelessServiceType"))));
        }
        bundle.release().unwrap();
    }
}

#[test]
fn stateful_registration_rejects_persistence_mismatches_and_broader_package_shapes() {
    let fixture = Fixture::new();
    let (app, service) = stateful_manifests();
    for (app, service) in [
        (APP.into(), SERVICE.into()),
        (app.clone(), SERVICE.into()),
        (APP.into(), service.clone()),
        (
            app.clone(),
            service.replace(
                r#"HasPersistedState="false""#,
                r#"HasPersistedState="true""#,
            ),
        ),
        (
            app.clone(),
            service.replace(r#" HasPersistedState="false""#, ""),
        ),
        (
            app.replace(r#"MinReplicaSetSize="1""#, r#"MinReplicaSetSize="2""#),
            service.clone(),
        ),
        (
            app.replace(
                r#"TargetReplicaSetSize="[Count]""#,
                r#"TargetReplicaSetSize="3""#,
            ),
            service.clone(),
        ),
        (
            app.replace("<SingletonPartition/>", "<UniformInt64Partition/>"),
            service.clone(),
        ),
        (
            app.replace("SharedProcess", "ExclusiveProcess"),
            service.clone(),
        ),
        (
            app.clone(),
            service.replace(
                "ServiceTypeName=\"OtherType\"",
                "ServiceTypeName=\"WrongType\"",
            ),
        ),
        (
            app.clone(),
            service.replace(
                "</ServiceTypes>",
                "<StatelessServiceType ServiceTypeName=\"Second\"/></ServiceTypes>",
            ),
        ),
        (
            app.clone(),
            service.replace(
                "</CodePackage>",
                "</CodePackage><CodePackage Name=\"Second\" Version=\"1\"/>",
            ),
        ),
        (
            app.clone(),
            service.replace(
                "<Resources>",
                "<ConfigPackage Name=\"Config\" Version=\"1\"/><Resources>",
            ),
        ),
        (
            app.clone(),
            service.replace(
                "<Resources>",
                "<DataPackage Name=\"Data\" Version=\"1\"/><Resources>",
            ),
        ),
    ] {
        fixture.write(&app, &service);
        for profile in [
            Profile::VolatileStatefulRegistration,
            Profile::VolatileStatefulLifecycle,
        ] {
            assert!(
                Package::load_with_profile(&fixture.0, "fabric:/Local", &[], profile).is_err(),
                "{app}\n{service}"
            );
        }
    }
    fixture.write(&app, &service);
    assert!(Package::load_with_profile(
        &fixture.0,
        "fabric:/Local",
        &[("Count".into(), "2".into())],
        Profile::VolatileStatefulRegistration,
    )
    .is_err());
}

#[test]
fn deployment_reservation_rejects_source_overlap_including_symlink_aliases() {
    use std::os::unix::fs::symlink;
    let fixture = Fixture::new();
    let mut package = fixture.load(&[]).unwrap();
    let source = package.source_code.clone();
    let alias = fixture.0.join("code-alias");
    symlink(&source, &alias).unwrap();
    for source_path in [&source, &alias] {
        package.source_code = source_path.clone();
        for parent in [
            source.clone(),
            source.join("bin"),
            alias.clone(),
            alias.join("bin"),
            alias.join("bin/.."),
        ] {
            let before = fs::read_dir(&parent).unwrap().count();
            let error = Deployment::reserve(&parent, &package).unwrap_err();
            assert!(
                error.0.contains("outside the source code package"),
                "{error}"
            );
            assert_eq!(fs::read_dir(&parent).unwrap().count(), before);
        }
    }
    assert!(source.join("bin/program").is_file());
}

#[test]
fn deployment_materialization_rechecks_source_overlap_before_writing() {
    let fixture = Fixture::new();
    let mut package = fixture.load(&[]).unwrap();
    let bundle = Deployment::reserve(&fixture.0, &package).unwrap();
    for source in [fixture.0.clone(), bundle.root.clone()] {
        package.source_code = source;
        let error = bundle.materialize(&package).unwrap_err();
        assert!(
            error.0.contains("outside the source code package"),
            "{error}"
        );
        assert_eq!(fs::read_dir(&bundle.root).unwrap().count(), 0);
    }
    bundle.release().unwrap();
    assert!(fixture.0.join("OtherPackage/Bin/bin/program").is_file());
}

#[test]
fn deployment_allows_symlinked_sibling_parent_with_source_name_prefix() {
    use std::os::unix::fs::symlink;
    let fixture = Fixture::new();
    let package = fixture.load(&[]).unwrap();
    let sibling = fixture.0.join("OtherPackage/Bin-runs");
    fs::create_dir(&sibling).unwrap();
    let alias = fixture.0.join("runs-alias");
    symlink(&sibling, &alias).unwrap();
    let bundle = Deployment::from_manifest(&alias, &package).unwrap();
    assert!(bundle.root.starts_with(sibling.canonicalize().unwrap()));
    assert_eq!(
        fs::read(&bundle.executable).unwrap(),
        fs::read(package.source_code.join("bin/program")).unwrap()
    );
    bundle.release().unwrap();
    assert!(package.source_code.join("bin/program").is_file());
    assert_eq!(fs::read_dir(&sibling).unwrap().count(), 0);
}

#[test]
fn unsupported_application_syntax_is_never_stripped() {
    let fixture = Fixture::new();
    for app in [
        APP.replace("ApplicationTypeVersion=", "Unknown="),
        APP.replace("<Parameters>", "<Principals/><Parameters>"),
        APP.replace("<Parameters>", "<Policies/><Parameters>"),
        APP.replace("<Parameters>", "<Diagnostics/><Parameters>"),
        APP.replace("<Parameters>", "<ServiceTemplates/><Parameters>"),
        APP.replace("<ServiceManifestRef", "<Policies/><ServiceManifestRef"),
        APP.replace("SharedProcess", "ExclusiveProcess"),
        APP.replace("<SingletonPartition/>", "<UniformInt64Partition/>"),
        APP.replace(
            "<SingletonPartition/>",
            "<SingletonPartition/><PlacementConstraints>NodeType==x</PlacementConstraints>",
        ),
        APP.replace("InstanceCount=\"[Count]\"", "InstanceCount=\"2\""),
        APP.replace("StatelessService", "StatefulService"),
        APP.replace(
            "</DefaultServices>",
            "<Service Name=\"Second\"/></DefaultServices>",
        ),
        APP.replace(
            "</ServiceManifestImport>",
            "</ServiceManifestImport><ServiceManifestImport/>",
        ),
        APP.replace(
            "</ServiceManifestImport>",
            "<ServiceManifestRef/></ServiceManifestImport>",
        ),
        APP.replace("OtherPackage", "../OtherPackage"),
        APP.replace("v3", "wrong"),
        APP.replace("OtherType", "wrong"),
        APP.replace("http://schemas.microsoft.com/2011/01/fabric", "urn:foreign"),
        APP.replace("<Parameters>", "text<Parameters>"),
        APP.replace("<Parameters>", "<Parameters xmlns=\"urn:foreign\">"),
        APP.replace("<Parameters>", "<Parameters xmlns:unused=\"urn:foreign\">"),
        APP.replace("<Parameters>", "<Parameters foreign=\"value\">"),
        format!("<?unsupported x?>{APP}"),
        format!("<!DOCTYPE ApplicationManifest [<!ENTITY custom 'x'>]>{APP}"),
    ] {
        fixture.write(&app, SERVICE);
        assert!(fixture.load(&[]).is_err(), "{app}");
    }
}

#[test]
fn unsupported_service_syntax_is_never_stripped() {
    let fixture = Fixture::new();
    for service in [
        SERVICE.replace("Version=\"v3\"", "Version=\"wrong\""),
        SERVICE.replace("Name=\"OtherPackage\"", "Name=\"Other\""),
        SERVICE.replace("OtherType", "wrong"),
        SERVICE.replace("StatelessServiceType", "StatefulServiceType"),
        SERVICE.replace("</ServiceTypes>", "<StatelessServiceType ServiceTypeName=\"Second\"/></ServiceTypes>"),
        SERVICE.replace("</CodePackage>", "</CodePackage><CodePackage Name=\"Second\" Version=\"1\"/>"),
        SERVICE.replace("<EntryPoint>", "<SetupEntryPoint/><EntryPoint>"),
        SERVICE.replace("ExeHost>", "DllHost>"),
        SERVICE.replace("<ExeHost>", "<ExeHost UseImplicitHost=\"true\">"),
        SERVICE.replace("<Program>", "<ConsoleRedirection/><Program>"),
        SERVICE.replace("<Program>", "<WorkingFolder>CodeBase</WorkingFolder><Program>"),
        SERVICE.replace("<Program>", "<WorkingFolder>CodePackage</WorkingFolder><WorkingFolder>Work</WorkingFolder><Program>"),
        SERVICE.replace("</CodePackage>", "<EnvironmentVariables/></CodePackage>"),
        SERVICE.replace("<Resources>", "<ConfigPackage Name=\"Config\" Version=\"1\"/><Resources>"),
        SERVICE.replace("<Resources>", "<DataPackage Name=\"Data\" Version=\"1\"/><Resources>"),
        SERVICE.replace("<Resources>", "<Diagnostics/><Resources>"),
        SERVICE.replace("<Resources>", "<Extensions/><Resources>"),
        SERVICE.replace("bin/program", "/bin/true"),
        SERVICE.replace("bin/program", "../program"),
        SERVICE.replace("bin/program", "bin//program"),
        SERVICE.replace("bin/program", "bin\\program"),
        SERVICE.replace("Protocol=\"tcp\"", "Protocol=\"https\""),
        SERVICE.replace("Type=\"Internal\"", "Type=\"Input\""),
        SERVICE.replace("Port=\"32123\"", "Port=\"0\""),
        SERVICE.replace("Port=\"32123\"", "Port=\"65536\""),
        SERVICE.replace("Port=\"32123\"", "Port=\"+123\""),
        SERVICE.replace("Port=\"32123\"", "Port=\"123\" CertificateRef=\"cert\""),
        SERVICE.replace("</Endpoints>", "<Endpoint Name=\"Control\" Protocol=\"tcp\" Type=\"Internal\" Port=\"123\"/></Endpoints>"),
        SERVICE.replace("<Arguments>", "<Arguments xml:space=\"preserve\">"),
        SERVICE.replace("<Arguments>", "<Arguments><Unknown/>"),
    ] {
        fixture.write(APP, &service);
        assert!(fixture.load(&[]).is_err(), "{service}");
    }
}

#[test]
fn parameters_and_arguments_are_strict_and_bounded() {
    let fixture = Fixture::new();
    for overrides in [
        vec![("Unknown".into(), "x".into())],
        vec![("Count".into(), "1".into()), ("Count".into(), "1".into())],
        vec![("Count".into(), "0".into())],
        vec![("Service".into(), "../escape".into())],
        vec![("Args".into(), "[Count]".into())],
        vec![("Args".into(), "a \"b c\"".into())],
        vec![("Args".into(), "a\\b".into())],
        vec![("Args".into(), "a\u{00a0}b".into())],
        vec![("Args".into(), "\0".into())],
        vec![("Args".into(), "\u{c}".into())],
        vec![("Args".into(), "\u{b}".into())],
        vec![("Args".into(), "\u{fffe}".into())],
        vec![("Args".into(), "\u{ffff}".into())],
        vec![("Args".into(), "a".repeat(16385))],
        vec![("Args".into(), "a ".repeat(129))],
    ] {
        assert!(fixture.load(&overrides).is_err(), "{overrides:?}");
    }
    for app in [
        APP.replace("[Service]", "prefix[Service]"),
        APP.replace("[Service]", "[Unknown]"),
        APP.replace("DefaultValue=\"Default\"", "DefaultValue=\"[Count]\""),
        APP.replace(
            "</Parameters>",
            "<Parameter Name=\"Count\" DefaultValue=\"1\"/></Parameters>",
        ),
    ] {
        fixture.write(&app, SERVICE);
        assert!(fixture.load(&[]).is_err());
    }
}

#[test]
fn empty_optional_wrappers_and_default_shared_activation_are_valid() {
    let fixture = Fixture::new();
    fixture.write(&APP.replace(" ServicePackageActivationMode=\"SharedProcess\"", ""),
        &SERVICE.replace("<Resources><Endpoints><Endpoint Name=\"Control\" Protocol=\"tcp\" Type=\"Internal\" Port=\"32123\"/></Endpoints></Resources>", "<Resources/>"));
    assert!(fixture.load(&[]).unwrap().endpoints.is_empty());
    assert_eq!(escape("&<\"'\r\n\t"), "&amp;&lt;&quot;&apos;&#13;&#10;&#9;");
}

#[test]
fn invalid_uri_missing_files_and_oversize_xml_fail_without_native_prerequisites() {
    let fixture = Fixture::new();
    for name in [
        "fabric:/",
        "http:/app",
        "fabric:/../app",
        "fabric:/app/",
        "fabric:/a b",
    ] {
        assert!(Package::load(&fixture.0, name, &[]).is_err());
    }
    fs::write(
        fixture.0.join("ApplicationManifest.xml"),
        " ".repeat(XML_LIMIT as usize + 1),
    )
    .unwrap();
    assert!(fixture.load(&[]).is_err());
    fixture.write(APP, &SERVICE.replace("bin/program", "missing"));
    assert!(fixture.load(&[]).is_err());
}

#[test]
fn symlinks_are_rejected_in_manifests_code_and_directories() {
    use std::os::unix::fs::symlink;
    let fixture = Fixture::new();
    let link = fixture.0.join("OtherPackage/Bin/escape");
    symlink("/bin/true", &link).unwrap();
    assert!(fixture.load(&[]).is_err());
    fs::remove_file(link).unwrap();
    let manifest = fixture.0.join("OtherPackage/ServiceManifest.xml");
    fs::rename(&manifest, manifest.with_extension("real")).unwrap();
    symlink(manifest.with_extension("real"), &manifest).unwrap();
    assert!(fixture.load(&[]).is_err());
}

#[test]
fn composed_native_filenames_are_validated_before_materialization() {
    let fixture = Fixture::new();
    let long = "a".repeat(128);
    // The individual identifier is valid, but the native layout component is not.
    identifier(&long).unwrap();
    fixture.write(
        &APP.replace("OtherPackage", &long).replace("v3", &long),
        &SERVICE.replace("OtherPackage", &long).replace("v3", &long),
    );
    fs::rename(fixture.0.join("OtherPackage"), fixture.0.join(&long)).unwrap();
    assert!(fixture
        .load(&[])
        .unwrap_err()
        .0
        .contains("composed native package filename"));
}
