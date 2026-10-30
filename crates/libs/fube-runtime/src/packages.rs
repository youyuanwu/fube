use crate::{wire::CodeContext, ActivationContext};
use crate::{Error, Result};
use roxmltree::{Document, Node};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

const NS: &str = "http://schemas.microsoft.com/2011/01/fabric";
pub const MAX_ARTIFACT_BYTES: usize = 1_048_576;
pub const MAX_SNAPSHOT_BYTES: usize = 16 * MAX_ARTIFACT_BYTES;
pub const MAX_PACKAGES: usize = 64;
pub const MAX_PARAMETERS: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageDescription {
    name: String,
    version: String,
    manifest_name: String,
    manifest_version: String,
    rollout: String,
    content_checksum: String,
    shared: bool,
    path: PathBuf,
}

impl PackageDescription {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn version(&self) -> &str {
        &self.version
    }
    pub fn manifest_name(&self) -> &str {
        &self.manifest_name
    }
    pub fn manifest_version(&self) -> &str {
        &self.manifest_version
    }
    pub fn rollout(&self) -> &str {
        &self.rollout
    }
    /// Native digested metadata, not an independently verified byte digest.
    pub fn content_checksum(&self) -> &str {
        &self.content_checksum
    }
    pub fn is_shared(&self) -> bool {
        self.shared
    }
    /// This path does not pin native files or directory lifetime.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationParameter {
    name: String,
    value: String,
    must_override: bool,
    encrypted: bool,
    kind: String,
}
impl ConfigurationParameter {
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Raw deployed text. Encryption/type metadata must not be discarded.
    pub fn raw_value(&self) -> &str {
        &self.value
    }
    pub fn must_override(&self) -> bool {
        self.must_override
    }
    pub fn is_encrypted(&self) -> bool {
        self.encrypted
    }
    pub fn value_type(&self) -> &str {
        &self.kind
    }
    pub fn plaintext(&self) -> Result<&str> {
        if self.encrypted || (!self.kind.is_empty() && self.kind != "PlainText") {
            return Err(Error::InvalidInput(
                "configuration value requires unsupported secret resolution",
            ));
        }
        Ok(&self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationSection {
    name: String,
    parameters: BTreeMap<String, ConfigurationParameter>,
}
impl ConfigurationSection {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn parameters(&self) -> impl Iterator<Item = &ConfigurationParameter> {
        self.parameters.values()
    }
    pub fn parameter(&self, name: &str) -> Result<&ConfigurationParameter> {
        self.parameters
            .get(&key(name))
            .ok_or(Error::InvalidInput("configuration parameter not found"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationPackage {
    description: PackageDescription,
    sections: BTreeMap<String, ConfigurationSection>,
}
impl ConfigurationPackage {
    pub fn description(&self) -> &PackageDescription {
        &self.description
    }
    pub fn sections(&self) -> impl Iterator<Item = &ConfigurationSection> {
        self.sections.values()
    }
    pub fn section(&self, name: &str) -> Result<&ConfigurationSection> {
        self.sections
            .get(&key(name))
            .ok_or(Error::InvalidInput("configuration section not found"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPackage {
    description: PackageDescription,
}
impl DataPackage {
    pub fn description(&self) -> &PackageDescription {
        &self.description
    }
}

#[derive(Debug, Clone)]
pub struct PackageSnapshot {
    configuration: Vec<Arc<ConfigurationPackage>>,
    data: Vec<Arc<DataPackage>>,
    pub(crate) code_fingerprint: String,
    pub(crate) endpoints: BTreeMap<String, u16>,
}
impl PackageSnapshot {
    /// Read a deployment using a genuine registration's node context.
    /// No node discovery, management fallback or native directory lease occurs.
    pub fn from_deployment(
        activation: &ActivationContext,
        node: &crate::NodeContext,
    ) -> Result<Self> {
        load(activation, Path::new(&node.deployment_directory))
    }
    pub fn endpoints(&self) -> &BTreeMap<String, u16> {
        &self.endpoints
    }
    /// Compare native code/profile metadata, not independently hashed code bytes.
    pub fn code_compatible_with(&self, other: &Self) -> bool {
        self.code_fingerprint == other.code_fingerprint
    }
    pub fn configuration_packages(&self) -> &[Arc<ConfigurationPackage>] {
        &self.configuration
    }
    pub fn data_packages(&self) -> &[Arc<DataPackage>] {
        &self.data
    }
    pub fn configuration_package(&self, name: &str) -> Result<Arc<ConfigurationPackage>> {
        self.configuration
            .iter()
            .find(|p| key(p.description.name()) == key(name))
            .cloned()
            .ok_or(Error::InvalidInput("configuration package not found"))
    }
    pub fn data_package(&self, name: &str) -> Result<Arc<DataPackage>> {
        self.data
            .iter()
            .find(|p| key(p.description.name()) == key(name))
            .cloned()
            .ok_or(Error::InvalidInput("data package not found"))
    }
}

pub(crate) fn key(value: &str) -> String {
    value.to_lowercase()
}
fn invalid<T>() -> Result<T> {
    Err(Error::Protocol(
        "invalid or unsupported native package artifact",
    ))
}

fn text(value: &str) -> Result<String> {
    if value.contains('\0') || value.encode_utf16().count() > 32768 {
        return invalid();
    }
    Ok(value.to_owned())
}
fn component(value: &str) -> Result<String> {
    if value.is_empty() || value == "." || value == ".." || value.contains(['/', '\\', ':']) {
        return invalid();
    }
    text(value)
}
fn attr(node: Node<'_, '_>, name: &str) -> Result<String> {
    text(
        node.attribute(name)
            .ok_or(Error::Protocol("missing native package attribute"))?,
    )
}
fn name(node: Node<'_, '_>) -> Result<String> {
    component(&attr(node, "Name")?)
}
fn setting_name(node: Node<'_, '_>) -> Result<String> {
    let value = attr(node, "Name")?;
    if value.is_empty() {
        return invalid();
    }
    Ok(value)
}
fn boolean(node: Node<'_, '_>, name: &str) -> Result<bool> {
    match node.attribute(name).map(str::to_ascii_lowercase).as_deref() {
        None | Some("false" | "0") => Ok(false),
        Some("true" | "1") => Ok(true),
        _ => invalid(),
    }
}
fn rollout(value: &str) -> Result<String> {
    let (a, b) = value
        .split_once('.')
        .ok_or(Error::Protocol("invalid native rollout"))?;
    let a: u32 = a
        .parse()
        .map_err(|_| Error::Protocol("invalid native rollout"))?;
    let b: u32 = b
        .parse()
        .map_err(|_| Error::Protocol("invalid native rollout"))?;
    if value != format!("{a}.{b}") {
        return invalid();
    }
    Ok(value.to_owned())
}
fn child<'a, 'input>(node: Node<'a, 'input>, tag: &str) -> Result<Node<'a, 'input>> {
    let mut found = node.children().filter(|n| n.has_tag_name((NS, tag)));
    let first = found
        .next()
        .ok_or(Error::Protocol("missing native package element"))?;
    if found.next().is_some() {
        return invalid();
    }
    Ok(first)
}
fn parse<'a>(xml: &'a str, root: &str) -> Result<Document<'a>> {
    let doc = Document::parse(xml).map_err(|_| Error::Protocol("malformed native package XML"))?;
    if !doc.root_element().has_tag_name((NS, root)) {
        return invalid();
    }
    for n in doc.descendants() {
        if n.is_pi() || (n.is_element() && n.tag_name().namespace() != Some(NS)) {
            return invalid();
        }
        if n.ancestors().count() > 32 {
            return Err(Error::ResourceLimit("artifact XML depth"));
        }
        if n.is_element() {
            for a in n.attributes() {
                text(a.value())?;
            }
        } else if n.is_text() {
            text(n.text().unwrap_or(""))?;
        }
    }
    Ok(doc)
}
fn shape(node: Node<'_, '_>, attributes: &[&str], children: &[&str]) -> Result<()> {
    if node
        .attributes()
        .any(|a| a.namespace().is_some() || !attributes.contains(&a.name()))
        || node.children().any(|n| {
            (n.is_element() && !children.contains(&n.tag_name().name()))
                || (n.is_text() && !n.text().unwrap_or("").trim().is_empty())
        })
    {
        return invalid();
    }
    Ok(())
}

struct Reader {
    bytes: usize,
}
impl Reader {
    fn read(&mut self, path: &Path) -> Result<String> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NONBLOCK);
        }
        let file: File = options.open(path)?;
        let meta = file.metadata()?;
        if !meta.is_file() || meta.len() > MAX_ARTIFACT_BYTES as u64 {
            return Err(Error::ResourceLimit("native package artifact file"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_ARTIFACT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or(Error::ResourceLimit("snapshot bytes"))?;
        if bytes.len() > MAX_ARTIFACT_BYTES || self.bytes > MAX_SNAPSHOT_BYTES {
            return Err(Error::ResourceLimit("snapshot bytes"));
        }
        String::from_utf8(bytes).map_err(|_| Error::Protocol("native artifact is not UTF-8"))
    }
}

fn settings(xml: &str, count: &mut usize) -> Result<BTreeMap<String, ConfigurationSection>> {
    let doc = parse(xml, "Settings")?;
    shape(doc.root_element(), &[], &["Section"])?;
    let mut sections = BTreeMap::new();
    for s in doc.root_element().children().filter(Node::is_element) {
        shape(s, &["Name"], &["Parameter"])?;
        let section_name = setting_name(s)?;
        let mut parameters = BTreeMap::new();
        for p in s.children().filter(Node::is_element) {
            shape(
                p,
                &["Name", "Value", "MustOverride", "IsEncrypted", "Type"],
                &[],
            )?;
            *count += 1;
            if *count > MAX_PARAMETERS {
                return Err(Error::ResourceLimit("configuration parameters"));
            }
            let parameter = ConfigurationParameter {
                name: setting_name(p)?,
                value: attr(p, "Value")?,
                must_override: boolean(p, "MustOverride")?,
                encrypted: boolean(p, "IsEncrypted")?,
                kind: text(p.attribute("Type").unwrap_or(""))?,
            };
            if parameters.insert(key(&parameter.name), parameter).is_some() {
                return invalid();
            }
        }
        if sections
            .insert(
                key(&section_name),
                ConfigurationSection {
                    name: section_name,
                    parameters,
                },
            )
            .is_some()
            || sections.len() > MAX_PARAMETERS
        {
            return invalid();
        }
    }
    Ok(sections)
}

fn overrides(node: Node<'_, '_>, package: &mut ConfigurationPackage) -> Result<()> {
    shape(node, &["Name"], &["Settings"])?;
    let override_name = attr(node, "Name")?;
    if !override_name.is_empty() && key(&override_name) != key(package.description.name()) {
        return invalid();
    }
    let settings = child(node, "Settings")?;
    shape(settings, &[], &["Section"])?;
    let mut seen_sections = BTreeSet::new();
    for section in settings.children().filter(Node::is_element) {
        shape(section, &["Name"], &["Parameter"])?;
        let section_name = setting_name(section)?;
        if !seen_sections.insert(key(&section_name)) {
            return invalid();
        }
        let target = package
            .sections
            .get_mut(&key(&section_name))
            .ok_or(Error::Protocol("configuration override section not found"))?;
        let mut seen_parameters = BTreeSet::new();
        for p in section.children().filter(Node::is_element) {
            shape(p, &["Name", "Value", "IsEncrypted", "Type"], &[])?;
            let parameter_name = setting_name(p)?;
            if !seen_parameters.insert(key(&parameter_name)) {
                return invalid();
            }
            let parameter =
                target
                    .parameters
                    .get_mut(&key(&parameter_name))
                    .ok_or(Error::Protocol(
                        "configuration override parameter not found",
                    ))?;
            // Native ApplyOverrides copies Name/Value/nonempty Type, NOT IsEncrypted/MustOverride.
            boolean(p, "IsEncrypted")?;
            parameter.name = parameter_name;
            parameter.value = attr(p, "Value")?;
            if let Some(kind) = p.attribute("Type").filter(|v| !v.is_empty()) {
                parameter.kind = text(kind)?;
            }
        }
    }
    Ok(())
}

fn service_types(node: Node<'_, '_>) -> Result<BTreeMap<String, String>> {
    shape(node, &[], &["StatelessServiceType", "StatefulServiceType"])?;
    let mut result = BTreeMap::new();
    for service in node.children().filter(Node::is_element) {
        let stateful = service.has_tag_name((NS, "StatefulServiceType"));
        shape(
            service,
            if stateful {
                &["ServiceTypeName", "HasPersistedState"]
            } else {
                &["ServiceTypeName", "UseImplicitHost"]
            },
            &[
                "LoadMetrics",
                "PlacementConstraints",
                "ServicePlacementPolicies",
                "Extensions",
            ],
        )?;
        let name = attr(service, "ServiceTypeName")?;
        if name.is_empty()
            || (stateful && boolean(service, "HasPersistedState")?)
            || (!stateful && boolean(service, "UseImplicitHost")?)
        {
            return invalid();
        }
        let mut metrics = BTreeMap::new();
        if let Some(loads) = optional(service, "LoadMetrics")? {
            shape(loads, &[], &["LoadMetric"])?;
            for metric in loads.children().filter(Node::is_element) {
                shape(
                    metric,
                    &[
                        "Name",
                        "Weight",
                        "DefaultLoad",
                        "PrimaryDefaultLoad",
                        "SecondaryDefaultLoad",
                    ],
                    &[],
                )?;
                let metric_name = setting_name(metric)?;
                let number = |name| -> Result<Option<u32>> {
                    metric
                        .attribute(name)
                        .map(|value| {
                            value
                                .parse::<u32>()
                                .map_err(|_| Error::Protocol("invalid type metric load"))
                        })
                        .transpose()
                };
                let default = number("DefaultLoad")?;
                let primary = number("PrimaryDefaultLoad")?;
                if default.is_some() && primary.is_some() && default != primary {
                    return invalid();
                }
                let primary = primary.or(default).unwrap_or(0);
                let secondary = number("SecondaryDefaultLoad")?.unwrap_or(0);
                let weight = metric
                    .attribute("Weight")
                    .unwrap_or("Low")
                    .to_ascii_lowercase();
                if !matches!(weight.as_str(), "zero" | "low" | "medium" | "high")
                    || metrics
                        .insert(
                            metric_name.to_lowercase(),
                            (metric_name, primary, secondary, weight),
                        )
                        .is_some()
                {
                    return invalid();
                }
            }
        }
        let constraint = if let Some(constraint) = optional(service, "PlacementConstraints")? {
            if constraint.attributes().len() != 0
                || constraint
                    .children()
                    .any(|child| child.is_element() || child.is_pi())
            {
                return invalid();
            }
            let content: String = constraint
                .children()
                .filter(Node::is_text)
                .filter_map(|child| child.text())
                .collect();
            text(&content)?
        } else {
            String::new()
        };
        for tag in ["ServicePlacementPolicies", "Extensions"] {
            if let Some(empty) = optional(service, tag)? {
                shape(empty, &[], &[])?;
            }
        }
        let declaration = format!("{name:?}:{stateful}:{metrics:?}:{constraint:?}");
        if result.insert(name.to_lowercase(), declaration).is_some() || result.len() > 32 {
            return invalid();
        }
    }
    if result.is_empty() {
        return invalid();
    }
    Ok(result)
}

fn canonical(node: Node<'_, '_>, output: &mut String) {
    if node.is_element() {
        output.push('<');
        output.push_str(node.tag_name().name());
        output.push(' ');
        let mut attributes: Vec<_> = node.attributes().map(|a| (a.name(), a.value())).collect();
        attributes.sort();
        for (k, v) in attributes {
            output.push_str(&format!("{k}={v:?};"));
        }
        output.push('>');
        for n in node.children() {
            canonical(n, output);
        }
        output.push_str("</");
        output.push_str(node.tag_name().name());
        output.push('>');
    } else if node.is_text() {
        let t = node.text().unwrap_or("").trim();
        if !t.is_empty() {
            output.push_str(&format!("{t:?}"));
        }
    }
}

fn scalar(node: Node<'_, '_>) -> Result<String> {
    if node.attributes().len() != 0 || node.children().any(|n| n.is_element()) {
        return invalid();
    }
    let value: String = node
        .children()
        .filter(Node::is_text)
        .filter_map(|n| n.text())
        .collect();
    text(value.trim())
}
fn optional<'a, 'b>(node: Node<'a, 'b>, tag: &str) -> Result<Option<Node<'a, 'b>>> {
    let mut found = node.children().filter(|n| n.has_tag_name((NS, tag)));
    let result = found.next();
    if found.next().is_some() {
        return invalid();
    }
    Ok(result)
}
fn code_semantics(node: Node<'_, '_>) -> Result<String> {
    shape(
        node,
        &["Name", "Version", "IsShared", "IsActivator"],
        &["EntryPoint", "EnvironmentVariables"],
    )?;
    if boolean(node, "IsActivator")? {
        return invalid();
    }
    let package_name = name(node)?;
    let version = component(&attr(node, "Version")?)?;
    let shared = boolean(node, "IsShared")?;
    let entry = child(node, "EntryPoint")?;
    shape(entry, &[], &["ExeHost"])?;
    let exe = child(entry, "ExeHost")?;
    shape(
        exe,
        &["IsExternalExecutable"],
        &[
            "Program",
            "Arguments",
            "WorkingFolder",
            "ConsoleRedirection",
            "RunFrequency",
        ],
    )?;
    if boolean(exe, "IsExternalExecutable")? {
        return invalid();
    }
    let program = scalar(child(exe, "Program")?)?;
    if program.is_empty() {
        return invalid();
    }
    let arguments = optional(exe, "Arguments")?
        .map(scalar)
        .transpose()?
        .unwrap_or_default();
    let working = optional(exe, "WorkingFolder")?
        .map(scalar)
        .transpose()?
        .unwrap_or_else(|| "Work".into());
    if !matches!(working.as_str(), "Work" | "CodePackage" | "CodeBase") {
        return invalid();
    }
    let mut fingerprint =
        format!("{package_name:?}:{version:?}:{shared}:{program:?}:{arguments:?}:{working:?}");
    if let Some(console) = optional(exe, "ConsoleRedirection")? {
        shape(console, &["FileRetentionCount", "FileMaxSizeInKb"], &[])?;
        // Native defaults from ExeEntryPointDescription::clear.
        let retention = console
            .attribute("FileRetentionCount")
            .unwrap_or("2")
            .parse::<u32>()
            .map_err(|_| Error::Protocol("invalid console retention"))?;
        let size = console
            .attribute("FileMaxSizeInKb")
            .unwrap_or("20480")
            .parse::<u32>()
            .map_err(|_| Error::Protocol("invalid console size"))?;
        fingerprint.push_str(&format!(":console:{retention}:{size}"));
    }
    if let Some(frequency) = optional(exe, "RunFrequency")? {
        shape(frequency, &["IntervalInSeconds"], &[])?;
        let interval = attr(frequency, "IntervalInSeconds")?
            .parse::<u32>()
            .map_err(|_| Error::Protocol("invalid entry point frequency"))?;
        if interval != 0 {
            return Err(Error::InvalidInput("periodic entry point unsupported"));
        }
    }
    if let Some(env) = optional(node, "EnvironmentVariables")? {
        shape(env, &[], &[])?;
    }
    Ok(fingerprint)
}

fn description(
    digest: Node<'_, '_>,
    declaration: Node<'_, '_>,
    manifest: Node<'_, '_>,
    base: &Path,
    service: &str,
) -> Result<PackageDescription> {
    let package_name = name(declaration)?;
    shape(declaration, &["Name", "Version"], &["Description"])?;
    let version = component(&attr(declaration, "Version")?)?;
    let declared = manifest
        .children()
        .find(|n| {
            n.has_tag_name((NS, declaration.tag_name().name()))
                && n.attribute("Name").map(key) == Some(key(&package_name))
        })
        .ok_or(Error::Protocol("digested package missing from manifest"))?;
    shape(declared, &["Name", "Version"], &["Description"])?;
    if attr(declared, "Version")? != version {
        return invalid();
    }
    let shared = boolean(digest, "IsShared")?;
    let path = base.join(format!(
        "{service}.{package_name}.{version}{}",
        if shared { ".lk" } else { "" }
    ));
    if !path.is_dir() {
        return Err(Error::Protocol("native package directory not found"));
    }
    Ok(PackageDescription {
        name: package_name,
        version,
        manifest_name: name(manifest)?,
        manifest_version: attr(manifest, "Version")?,
        rollout: rollout(&attr(digest, "RolloutVersion")?)?,
        content_checksum: text(digest.attribute("ContentChecksum").unwrap_or(""))?,
        shared,
        path,
    })
}

/// Read only native-authorized deployment artifacts, never management guesses.
pub(crate) fn load(activation: &ActivationContext, deployment: &Path) -> Result<PackageSnapshot> {
    let code: &CodeContext = &activation.runtime.code;
    if !deployment.is_absolute()
        || code.id.package.activation.mode != 0
        || code.id.package.activation.guid.0 != [0; 16]
        || !code.id.package.public_id.is_empty()
    {
        return Err(Error::InvalidInput(
            "requires native SharedProcess deployment",
        ));
    }
    let application = &code.id.package.package.application;
    let app_id = format!(
        "{}_App{}",
        component(&application.type_name)?,
        application.number
    );
    let service = component(&code.id.package.package.name)?;
    let base = deployment.join(&app_id);
    let mut reader = Reader { bytes: 0 };
    let app_rollout = &code.version.version.application.value;
    let app = reader.read(&base.join(format!(
        "App.{}.{}.xml",
        app_rollout.major, app_rollout.minor
    )))?;
    let app_doc = parse(&app, "ApplicationPackage")?;
    let app_node = app_doc.root_element();
    shape(
        app_node,
        &[
            "ApplicationId",
            "ApplicationTypeName",
            "NameUri",
            "RolloutVersion",
            "ContentChecksum",
        ],
        &["DigestedEnvironment", "DigestedCertificates"],
    )?;
    child(app_node, "DigestedEnvironment")?;
    child(app_node, "DigestedCertificates")?;
    if attr(app_node, "ApplicationId")? != app_id
        || attr(app_node, "NameUri")? != activation.application_name
        || attr(app_node, "ApplicationTypeName")? != application.type_name
        || rollout(&attr(app_node, "RolloutVersion")?)?
            != format!("{}.{}", app_rollout.major, app_rollout.minor)
    {
        return invalid();
    }
    let current_path = base.join(format!("{service}.Package.Current.xml"));
    let current = reader.read(&current_path)?;
    let current_doc = parse(&current, "ServicePackage")?;
    let root = current_doc.root_element();
    shape(
        root,
        &[
            "Name",
            "ManifestVersion",
            "RolloutVersion",
            "ManifestChecksum",
            "ContentChecksum",
        ],
        &[
            "Description",
            "ServicePackageResourceGovernancePolicy",
            "ServicePackageContainerPolicy",
            "ServiceFabricRuntimeAccessPolicy",
            "DigestedServiceTypes",
            "DigestedCodePackage",
            "DigestedConfigPackage",
            "DigestedDataPackage",
            "DigestedResources",
            "NetworkPolicies",
            "Diagnostics",
        ],
    )?;
    for required in ["DigestedServiceTypes", "DigestedResources", "Diagnostics"] {
        child(root, required)?;
    }
    for tag in [
        "Description",
        "ServicePackageResourceGovernancePolicy",
        "ServicePackageContainerPolicy",
        "ServiceFabricRuntimeAccessPolicy",
        "NetworkPolicies",
    ] {
        optional(root, tag)?;
    }
    if let Some(access) = optional(root, "ServiceFabricRuntimeAccessPolicy")? {
        shape(
            access,
            &[
                "RemoveServiceFabricRuntimeAccess",
                "UseServiceFabricReplicatedStore",
            ],
            &[],
        )?;
        if boolean(access, "RemoveServiceFabricRuntimeAccess")?
            || boolean(access, "UseServiceFabricReplicatedStore")?
        {
            return invalid();
        }
    }
    if optional(root, "ServicePackageContainerPolicy")?.is_some()
        || optional(root, "NetworkPolicies")?.is_some()
    {
        return Err(Error::InvalidInput(
            "unsupported native package host policy",
        ));
    }
    let service_rollout = &code.version.version.value;
    if name(root)? != service
        || rollout(&attr(root, "RolloutVersion")?)?
            != format!("{}.{}", service_rollout.major, service_rollout.minor)
    {
        return invalid();
    }
    let manifest_version = component(&attr(root, "ManifestVersion")?)?;
    let manifest_xml =
        reader.read(&base.join(format!("{service}.Manifest.{manifest_version}.xml")))?;
    let manifest_doc = parse(&manifest_xml, "ServiceManifest")?;
    let manifest = manifest_doc.root_element();
    shape(
        manifest,
        &["Name", "Version", "ManifestId"],
        &[
            "Description",
            "ServiceTypes",
            "CodePackage",
            "ConfigPackage",
            "DataPackage",
            "Resources",
            "Diagnostics",
        ],
    )?;
    let types = service_types(child(manifest, "ServiceTypes")?)?;
    let digested_types = child(root, "DigestedServiceTypes")?;
    shape(digested_types, &["RolloutVersion"], &["ServiceTypes"])?;
    rollout(&attr(digested_types, "RolloutVersion")?)?;
    if service_types(child(digested_types, "ServiceTypes")?)? != types {
        return invalid();
    }
    if name(manifest)? != service || attr(manifest, "Version")? != manifest_version {
        return invalid();
    }
    let digested_code = child(root, "DigestedCodePackage")?;
    shape(
        digested_code,
        &["RolloutVersion", "ContentChecksum", "IsShared"],
        &[
            "CodePackage",
            "RunAsPolicy",
            "ResourceGovernancePolicy",
            "ConfigPackagePolicies",
        ],
    )?;
    let code_rollout = rollout(&attr(digested_code, "RolloutVersion")?)?;
    let code_shared = boolean(digested_code, "IsShared")?;
    let declared_code = child(digested_code, "CodePackage")?;
    let manifest_code = child(manifest, "CodePackage")?;
    if name(declared_code)? != code.id.name {
        return invalid();
    }
    let code_decl = code_semantics(declared_code)?;
    let manifest_decl = code_semantics(manifest_code)?;
    if code_decl != manifest_decl {
        return invalid();
    }
    let mut fingerprint = format!(
        "{code_decl}:{code_rollout}:{code_shared}:{types:?}:{:?}",
        digested_code.attribute("ContentChecksum").unwrap_or("")
    );
    for n in digested_code
        .children()
        .filter(Node::is_element)
        .filter(|n| !n.has_tag_name((NS, "CodePackage")))
    {
        canonical(n, &mut fingerprint);
    }
    for n in root.children().filter(Node::is_element) {
        if !matches!(
            n.tag_name().name(),
            "Description"
                | "DigestedCodePackage"
                | "DigestedConfigPackage"
                | "DigestedDataPackage"
                | "DigestedServiceTypes"
                | "DigestedResources"
                | "Diagnostics"
        ) {
            canonical(n, &mut fingerprint);
        }
    }
    let mut configuration = Vec::new();
    let mut data = Vec::new();
    let mut config_names = BTreeSet::new();
    let mut data_names = BTreeSet::new();
    let mut parameter_count = 0;
    for n in root.children().filter(Node::is_element) {
        match n.tag_name().name() {
            "DigestedConfigPackage" => {
                shape(
                    n,
                    &["RolloutVersion", "ContentChecksum", "IsShared"],
                    &["ConfigPackage", "ConfigOverride"],
                )?;
                let d = description(n, child(n, "ConfigPackage")?, manifest, &base, &service)?;
                if !config_names.insert(key(&d.name)) || configuration.len() == MAX_PACKAGES {
                    return invalid();
                }
                let xml = reader.read(&d.path.join("Settings.xml"))?;
                let mut package = ConfigurationPackage {
                    description: d,
                    sections: settings(&xml, &mut parameter_count)?,
                };
                let entries: Vec<_> = n
                    .children()
                    .filter(|n| n.has_tag_name((NS, "ConfigOverride")))
                    .collect();
                if entries.len() > 1 {
                    return invalid();
                }
                if let Some(entry) = entries.first() {
                    overrides(*entry, &mut package)?;
                }
                configuration.push(Arc::new(package));
            }
            "DigestedDataPackage" => {
                shape(
                    n,
                    &["RolloutVersion", "ContentChecksum", "IsShared"],
                    &["DataPackage"],
                )?;
                let d = description(n, child(n, "DataPackage")?, manifest, &base, &service)?;
                if !data_names.insert(key(&d.name)) || data.len() == MAX_PACKAGES {
                    return invalid();
                }
                data.push(Arc::new(DataPackage { description: d }));
            }
            _ => {}
        }
    }
    for (tag, names) in [
        ("ConfigPackage", &config_names),
        ("DataPackage", &data_names),
    ] {
        let mut declared_names = BTreeSet::new();
        for n in manifest.children().filter(|n| n.has_tag_name((NS, tag))) {
            if !declared_names.insert(key(&name(n)?)) {
                return invalid();
            }
        }
        if &declared_names != names {
            return invalid();
        }
    }
    let endpoints_xml = reader.read(&base.join(format!("{service}.Endpoints.txt")))?;
    let mut endpoints = BTreeMap::new();
    let mut endpoint_names = BTreeSet::new();
    for entry in endpoints_xml.split(',').filter(|e| !e.is_empty()) {
        let fields: Vec<_> = entry.split(';').collect();
        if fields.len() != 9 || !endpoint_names.insert(key(fields[0])) {
            return invalid();
        }
        let port = fields[3]
            .parse::<u16>()
            .map_err(|_| Error::Protocol("invalid deployed endpoint port"))?;
        if !matches!(
            fields[1].to_ascii_lowercase().as_str(),
            "http" | "https" | "tcp" | "udp"
        ) || !matches!(
            fields[2].to_ascii_lowercase().as_str(),
            "input" | "internal"
        ) || (!fields[7].is_empty() && fields[7] != code.id.name)
            || endpoints.len() >= 4096
        {
            return invalid();
        }
        if !fields[4].is_empty() || fields[1].eq_ignore_ascii_case("https") {
            return Err(Error::InvalidInput(
                "unsupported deployed endpoint security",
            ));
        }
        endpoints.insert(component(fields[0])?, port);
    }
    #[cfg(test)]
    {
        let hook = STABILITY_HOOK.with(|hook| hook.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
    }
    if reader.read(&current_path)? != current {
        return Err(Error::Protocol("native package changed while loading"));
    }
    Ok(PackageSnapshot {
        configuration,
        data,
        code_fingerprint: fingerprint,
        endpoints,
    })
}

#[cfg(test)]
thread_local! {
    pub(crate) static STABILITY_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
#[path = "package_service_type_tests.rs"]
mod service_type_tests;
#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) struct Fixture {
        pub root: PathBuf,
        pub base: PathBuf,
        pub activation: ActivationContext,
    }
    impl Fixture {
        pub fn new() -> Self {
            let root =
                std::env::temp_dir().join(format!("package-context-{}", uuid::Uuid::new_v4()));
            let base = root.join("RustApp_App0");
            std::fs::create_dir_all(&base).unwrap();
            let activation = ActivationContext::parse(&crate::context::tests::env()).unwrap();
            let value = Self {
                root,
                base,
                activation,
            };
            value.write(1, "old");
            value
        }
        pub fn write(&self, version: u32, setting: &str) {
            let code = r#"<CodePackage Name="Code" Version="1"><EntryPoint><ExeHost><Program>app</Program></ExeHost></EntryPoint></CodePackage>"#;
            let config = format!(r#"<ConfigPackage Name="Config" Version="{version}"/>"#);
            let data = r#"<DataPackage Name="Data" Version="1"/>"#;
            std::fs::write(self.base.join("App.1.0.xml"),
                format!(r#"<ApplicationPackage xmlns="{NS}" ApplicationId="RustApp_App0" ApplicationTypeName="RustApp" NameUri="fabric:/RustApp" RolloutVersion="1.0"><DigestedEnvironment RolloutVersion="1.0"><Principals/><Policies/><Diagnostics/></DigestedEnvironment><DigestedCertificates RolloutVersion="1.0"/></ApplicationPackage>"#)).unwrap();
            std::fs::write(self.base.join(format!("RustService.Manifest.{version}.xml")),
                format!(r#"<ServiceManifest xmlns="{NS}" Name="RustService" Version="{version}"><ServiceTypes><StatelessServiceType ServiceTypeName="Type"/></ServiceTypes>{code}{config}{data}</ServiceManifest>"#)).unwrap();
            let digested_code = code
                .replace("Version=\"1\"", "Version=\"1\" IsShared=\"false\"")
                .replace("<ExeHost>", "<ExeHost IsExternalExecutable=\"false\">")
                .replace(
                    "</ExeHost>",
                    "<WorkingFolder>Work</WorkingFolder></ExeHost>",
                );
            let root = format!(
                r#"<ServicePackage xmlns="{NS}" Name="RustService" ManifestVersion="{version}" RolloutVersion="2.0"><DigestedServiceTypes RolloutVersion="1.0"><ServiceTypes><StatelessServiceType ServiceTypeName="Type"/></ServiceTypes></DigestedServiceTypes><DigestedCodePackage RolloutVersion="1.0" ContentChecksum="code" IsShared="false">{digested_code}</DigestedCodePackage><DigestedConfigPackage RolloutVersion="{version}.0" ContentChecksum="config">{config}</DigestedConfigPackage><DigestedDataPackage RolloutVersion="1.0" IsShared="true">{data}</DigestedDataPackage><DigestedResources RolloutVersion="1.0"><DigestedEndpoints/></DigestedResources><Diagnostics/></ServicePackage>"#
            );
            std::fs::write(self.base.join("RustService.Package.Current.xml"), root).unwrap();
            let config_dir = self.base.join(format!("RustService.Config.{version}"));
            std::fs::create_dir_all(&config_dir).unwrap();
            std::fs::create_dir_all(self.base.join("RustService.Data.1.lk")).unwrap();
            std::fs::write(config_dir.join("Settings.xml"),
                format!(r#"<Settings xmlns="{NS}"><Section Name="Main"><Parameter Name="Value" Value="{setting}"/></Section></Settings>"#)).unwrap();
            std::fs::write(
                self.base.join("RustService.Endpoints.txt"),
                "Rpc;http;Input;1234;;;;;127.0.0.1",
            )
            .unwrap();
        }
    }
    impl Fixture {
        pub(super) fn read_public(&self) -> Result<PackageSnapshot> {
            let node = crate::NodeContext {
                id: "node".into(),
                instance: 1,
                name: "node".into(),
                node_type: "type".into(),
                ip_or_fqdn: "127.0.0.1".into(),
                client_address: "127.0.0.1:19000".into(),
                deployment_directory: self.root.to_str().unwrap().into(),
                work_directory: "work".into(),
                host_process_id: 1,
                initial_lease_ttl_ticks: 1,
                lease_duration_ticks: 1,
                fresh_poll_ttl_ticks: 1,
                fresh_poll_elapsed_micros: 0,
            };
            PackageSnapshot::from_deployment(&self.activation, &node)
        }
        pub(crate) fn config_count(&self, count: usize, parameters: usize, value: &str) {
            self.write(1, "old");
            let current_path = self.base.join("RustService.Package.Current.xml");
            let manifest_path = self.base.join("RustService.Manifest.1.xml");
            let mut digests = String::new();
            let mut declarations = String::new();
            for i in 0..count {
                let declaration = format!(r#"<ConfigPackage Name="Config{i}" Version="1"/>"#);
                declarations.push_str(&declaration);
                digests.push_str(&format!(r#"<DigestedConfigPackage RolloutVersion="1.0">{declaration}</DigestedConfigPackage>"#));
                let path = self.base.join(format!("RustService.Config{i}.1"));
                std::fs::create_dir_all(&path).unwrap();
                let mut body = String::new();
                for j in 0..parameters {
                    body.push_str(&format!(r#"<Parameter Name="P{j}" Value="{value}"/>"#));
                }
                std::fs::write(
                    path.join("Settings.xml"),
                    format!(
                        r#"<Settings xmlns="{NS}"><Section Name="Main">{body}</Section></Settings>"#
                    ),
                )
                .unwrap();
            }
            let current = std::fs::read_to_string(&current_path).unwrap();
            let start = current.find("<DigestedConfigPackage").unwrap();
            let end = current.find("</DigestedConfigPackage>").unwrap()
                + "</DigestedConfigPackage>".len();
            std::fs::write(
                &current_path,
                format!("{}{}{}", &current[..start], digests, &current[end..]),
            )
            .unwrap();
            let manifest = std::fs::read_to_string(&manifest_path).unwrap();
            std::fs::write(
                &manifest_path,
                manifest.replace(
                    r#"<ConfigPackage Name="Config" Version="1"/>"#,
                    &declarations,
                ),
            )
            .unwrap();
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[test]
    fn deployed_snapshots_override_and_retained_values() {
        let fixture = Fixture::new();
        let current_path = fixture.base.join("RustService.Package.Current.xml");
        let current = std::fs::read_to_string(&current_path).unwrap();
        let override_xml = r#"<ConfigOverride Name="config"><Settings><Section Name="main"><Parameter Name="VALUE" Value="overridden" Type="PlainText"/></Section></Settings></ConfigOverride>"#;
        std::fs::write(
            &current_path,
            current.replace(
                "</DigestedConfigPackage>",
                &format!("{override_xml}</DigestedConfigPackage>"),
            ),
        )
        .unwrap();
        let old = load(&fixture.activation, &fixture.root).unwrap();
        let package = old.configuration_package("CONFIG").unwrap();
        assert_eq!(
            package
                .section("MAIN")
                .unwrap()
                .parameter("value")
                .unwrap()
                .plaintext()
                .unwrap(),
            "overridden"
        );
        assert_eq!(package.description().manifest_version(), "1");
        assert_eq!(
            old.data_package("data").unwrap().description().path(),
            fixture.base.join("RustService.Data.1.lk")
        );
        assert!(old.data_package("data").unwrap().description().is_shared());
        assert!(old.configuration_package("missing").is_err());
        fixture.write(2, "new");
        let new = load(&fixture.activation, &fixture.root).unwrap();
        assert_eq!(
            new.configuration_package("Config")
                .unwrap()
                .description()
                .version(),
            "2"
        );
        assert_eq!(
            package
                .section("Main")
                .unwrap()
                .parameter("Value")
                .unwrap()
                .raw_value(),
            "overridden"
        );
        assert_eq!(old.code_fingerprint, new.code_fingerprint);
    }

    #[test]
    fn artifact_mismatch_and_missing_inputs_fail() {
        let fixture = Fixture::new();
        let path = fixture.base.join("RustService.Package.Current.xml");
        let original = std::fs::read_to_string(&path).unwrap();
        for bad in [
            original.replace("RolloutVersion=\"2.0\"","RolloutVersion=\"9.0\""),
            original.replace("Name=\"RustService\"","Name=\"Wrong\""),
            original.replace("ManifestVersion=\"1\"","ManifestVersion=\"missing\""),
            original.replace("Name=\"Config\"","Name=\"../Config\""),
            original.replace("</DigestedConfigPackage>",r#"<ConfigOverride Name="Wrong"><Settings/></ConfigOverride></DigestedConfigPackage>"#),
            original.replace("</DigestedConfigPackage>",r#"<ConfigOverride Name="Config"><Settings><Section Name="unknown"/></Settings></ConfigOverride></DigestedConfigPackage>"#),
            "<broken".into(),
        ] {
            std::fs::write(&path,bad).unwrap();
            assert!(load(&fixture.activation,&fixture.root).is_err());
        }
        std::fs::write(&path, original).unwrap();
        std::fs::remove_file(fixture.base.join("RustService.Config.1/Settings.xml")).unwrap();
        assert!(load(&fixture.activation, &fixture.root).is_err());
        assert!(load(&fixture.activation, Path::new("relative")).is_err());
        let large = fixture.base.join("too-large");
        std::fs::write(&large, vec![b'a'; MAX_ARTIFACT_BYTES + 1]).unwrap();
        assert!(Reader { bytes: 0 }.read(&large).is_err());
    }

    #[test]
    fn public_reader_rejects_malformed_profiles_and_declarations() {
        let fixture = Fixture::new();
        assert!(fixture.read_public().is_ok());
        let path = fixture.base.join("RustService.Package.Current.xml");
        let original = std::fs::read_to_string(&path).unwrap();
        for bad in [
            original.replace(
                "IsExternalExecutable=\"false\"",
                "IsExternalExecutable=\"true\"",
            ),
            original.replace(
                "<CodePackage Name=\"Code\"",
                "<CodePackage IsActivator=\"true\" Name=\"Code\"",
            ),
            original.replace(
                "<CodePackage Name=\"Code\" Version=\"1\"",
                "<CodePackage Name=\"Code\"",
            ),
            original.replace("<Program>app</Program>", ""),
            original.replace(
                "RolloutVersion=\"1.0\" ContentChecksum=\"code\"",
                "RolloutVersion=\"bad\" ContentChecksum=\"code\"",
            ),
            original.replace(
                "ContentChecksum=\"code\" IsShared=\"false\"",
                "ContentChecksum=\"code\" IsShared=\"invalid\"",
            ),
            original.replace("DigestedConfigPackage", "DigestedConfigPackag"),
        ] {
            std::fs::write(&path, bad).unwrap();
            assert!(fixture.read_public().is_err());
        }
        fixture.write(1, "old");
        let manifest = fixture.base.join("RustService.Manifest.1.xml");
        let original = std::fs::read_to_string(&manifest).unwrap();
        std::fs::write(
            &manifest,
            original.replace("Name=\"Config\"", "Other=\"x\" Name=\"Config\""),
        )
        .unwrap();
        assert!(fixture.read_public().is_err());
    }

    #[test]
    fn empty_packages_duplicates_shared_paths_and_overrides() {
        let fixture = Fixture::new();
        fixture.config_count(0, 0, "");
        assert!(fixture
            .read_public()
            .unwrap()
            .configuration_packages()
            .is_empty());
        let current_path = fixture.base.join("RustService.Package.Current.xml");
        for tag in ["Config", "Data"] {
            fixture.write(1, "old");
            let current = std::fs::read_to_string(&current_path).unwrap();
            let declaration = format!(r#"<{tag}Package Name="{tag}" Version="1"/>"#);
            let duplicate = format!(
                r#"<Digested{tag}Package RolloutVersion="1.0">{}</Digested{tag}Package>"#,
                declaration.replace(
                    &format!("Name=\"{tag}\""),
                    &format!("Name=\"{}\"", tag.to_lowercase())
                )
            );
            std::fs::write(
                &current_path,
                current.replace(
                    "</ServicePackage>",
                    &format!("{duplicate}</ServicePackage>"),
                ),
            )
            .unwrap();
            assert!(fixture.read_public().is_err());
        }
        fixture.write(1, "old");
        let current = std::fs::read_to_string(&current_path).unwrap();
        std::fs::write(
            &current_path,
            current.replace(
                "<DigestedConfigPackage RolloutVersion=",
                "<DigestedConfigPackage IsShared=\"true\" RolloutVersion=",
            ),
        )
        .unwrap();
        let source = fixture.base.join("RustService.Config.1");
        let shared = fixture.base.join("RustService.Config.1.lk");
        std::os::unix::fs::symlink(&source, &shared).unwrap();
        assert!(fixture
            .read_public()
            .unwrap()
            .configuration_package("Config")
            .unwrap()
            .description()
            .is_shared());
        std::fs::remove_file(&shared).unwrap();
        for target in [
            r#"<Section Name="Main"><Parameter Name="unknown" Value="x"/></Section>"#,
            r#"<Section Name="Main"><Parameter Name="Value" Value="x"/><Parameter Name="VALUE" Value="y"/></Section>"#,
            r#"<Section Name="Main"/><Section Name="MAIN"/>"#,
        ] {
            fixture.write(1, "old");
            let current = std::fs::read_to_string(&current_path).unwrap();
            let xml = format!(
                r#"<ConfigOverride Name="Config"><Settings>{target}</Settings></ConfigOverride></DigestedConfigPackage>"#
            );
            std::fs::write(
                &current_path,
                current.replace("</DigestedConfigPackage>", &xml),
            )
            .unwrap();
            assert!(fixture.read_public().is_err());
        }
        fixture.write(1, "old");
        let settings_path = fixture.base.join("RustService.Config.1/Settings.xml");
        std::fs::write(&settings_path,format!(r#"<Settings xmlns="{NS}"><Section Name="Main"><Parameter Name="Value" Value="base" MustOverride="TRUE" IsEncrypted="1" Type="Encrypted"/></Section></Settings>"#)).unwrap();
        let current = std::fs::read_to_string(&current_path).unwrap();
        let xml = r#"<ConfigOverride Name="Config"><Settings><Section Name="Main"><Parameter Name="Value" Value="cipher" IsEncrypted="0" Type="PlainText"/></Section></Settings></ConfigOverride></DigestedConfigPackage>"#;
        std::fs::write(
            &current_path,
            current.replace("</DigestedConfigPackage>", xml),
        )
        .unwrap();
        let package = fixture
            .read_public()
            .unwrap()
            .configuration_package("Config")
            .unwrap();
        let p = package.section("Main").unwrap().parameter("Value").unwrap();
        assert!(p.is_encrypted() && p.must_override() && p.plaintext().is_err());
        assert_eq!((p.raw_value(), p.value_type()), ("cipher", "PlainText"));
        std::fs::remove_file(fixture.base.join("App.1.0.xml")).unwrap();
        assert!(fixture.read_public().is_err());
    }

    #[test]
    fn endpoints_booleans_strings_depth_and_aggregate_bounds() {
        let fixture = Fixture::new();
        let path = fixture.base.join("RustService.Endpoints.txt");
        for bad in [
            "Rpc;wrong;Input;12;;;;;ip",
            "Rpc;http;wrong;12;;;;;ip",
            "Rpc;;Input;12;;;;;ip",
            "Rpc;http;Input;12;;;;",
            "Rpc;https;Input;12;;;;;ip",
            "Rpc;http;Input;12;cert;;;;ip",
        ] {
            std::fs::write(&path, bad).unwrap();
            assert!(fixture.read_public().is_err(), "{bad}");
        }
        for flag in ["0", "FALSE", "false", "1", "TRUE", "true"] {
            let xml = format!(
                r#"<Settings xmlns="{NS}"><Section Name="s"><Parameter Name="p" Value="v" IsEncrypted="{flag}"/></Section></Settings>"#
            );
            let p = settings(&xml, &mut 0).unwrap();
            assert_eq!(
                p["s"].parameter("p").unwrap().is_encrypted(),
                matches!(flag, "1" | "TRUE" | "true")
            );
        }
        let field = |value: &str| {
            format!(
                r#"<Settings xmlns="{NS}"><Section Name="s"><Parameter Name="p" Value="{value}"/></Section></Settings>"#
            )
        };
        assert!(settings(&field(&"😀".repeat(16384)), &mut 0).is_ok());
        assert!(settings(&field(&format!("{}x", "😀".repeat(16384))), &mut 0).is_err());
        let split = format!(
            "<Program>{}<!--split-->{}</Program>",
            "x".repeat(20000),
            "x".repeat(20000)
        );
        let doc = Document::parse(&split).unwrap();
        assert!(scalar(doc.root_element()).is_err());
        let doc = Document::parse("<Program>a<!--not program text-->pp</Program>").unwrap();
        assert_eq!(scalar(doc.root_element()).unwrap(), "app");
        let code = format!(
            r#"<CodePackage xmlns="{NS}" Name="Code" Version="1"><EntryPoint><ExeHost><Program>app</Program></ExeHost></EntryPoint></CodePackage>"#
        );
        let plain = Document::parse(&code).unwrap();
        let commented_xml = code.replace("app", "a<!--misleading-->pp");
        let commented = Document::parse(&commented_xml).unwrap();
        assert_eq!(
            code_semantics(plain.root_element()).unwrap(),
            code_semantics(commented.root_element()).unwrap()
        );
        let differing_xml = code.replace("app", "amisleadingpp");
        let differing = Document::parse(&differing_xml).unwrap();
        assert_ne!(
            code_semantics(differing.root_element()).unwrap(),
            code_semantics(commented.root_element()).unwrap()
        );
        assert!(parse(
            &format!(
                r#"<Settings xmlns="{NS}">{}{}</Settings>"#,
                "<n>".repeat(32),
                "</n>".repeat(32)
            ),
            "Settings"
        )
        .is_err());
        fixture.config_count(64, 0, "");
        assert_eq!(
            fixture
                .read_public()
                .unwrap()
                .configuration_packages()
                .len(),
            64
        );
        fixture.config_count(65, 0, "");
        assert!(fixture.read_public().is_err());
        fixture.config_count(2, 2048, "v");
        assert!(fixture.read_public().is_ok());
        fixture.config_count(2, 2049, "v");
        assert!(fixture.read_public().is_err());
        fixture.config_count(30, 20, &"x".repeat(30000));
        assert!(matches!(
            fixture.read_public(),
            Err(Error::ResourceLimit("snapshot bytes"))
        ));
    }

    #[test]
    fn settings_are_owned_case_insensitive_and_security_explicit() {
        let xml = format!(
            r#"<Settings xmlns="{NS}"><Section Name="Main"><Parameter Name="Text" Value="a&amp;b"/><Parameter Name="Secret" Value="cipher" IsEncrypted="true" MustOverride="true"/><Parameter Name="Ref" Value="uri" Type="SecretsStoreRef"/></Section></Settings>"#
        );
        let parsed = settings(&xml, &mut 0).unwrap();
        drop(xml);
        let section = &parsed["main"];
        assert_eq!(
            section.parameter("TEXT").unwrap().plaintext().unwrap(),
            "a&b"
        );
        let secret = section.parameter("secret").unwrap();
        assert!(secret.must_override() && secret.is_encrypted());
        assert_eq!(secret.raw_value(), "cipher");
        assert!(secret.plaintext().is_err());
        assert!(section.parameter("ref").unwrap().plaintext().is_err());
        assert!(section.parameter("missing").is_err());
    }

    #[test]
    fn malformed_settings_never_become_empty() {
        for body in [
            "<Section Name='a'/><Section Name='A'/>",
            "<Section Name='a'><Parameter Name='p' Value='1'/><Parameter Name='P' Value='2'/></Section>",
            "<Section Name='a'><Parameter Name='p'/></Section>",
            "<Section Name='a'><Parameter Name='p' Value='1' IsEncrypted='invalid'/></Section>",
            "<Section Name='a'><Parameter Name='p' Value='1' Other='x'/></Section>",
            "<Section Name='a'>invalid</Section>", "<Unknown/>",
            "<Section xmlns='foreign' Name='a'/>",
        ] {
            assert!(settings(&format!(r#"<Settings xmlns="{NS}">{body}</Settings>"#),&mut 0).is_err(),"{body}");
        }
        assert!(settings("", &mut 0).is_err());
        assert!(settings(
            &format!(r#"<!DOCTYPE Settings [<!ENTITY x "x">]><Settings xmlns="{NS}"/>"#),
            &mut 0
        )
        .is_err());
        assert!(settings(&format!(r#"<?bad value?><Settings xmlns="{NS}"/>"#), &mut 0).is_err());
        let mut count = MAX_PARAMETERS;
        assert!(settings(&format!(r#"<Settings xmlns="{NS}"><Section Name="a"><Parameter Name="p" Value="1"/></Section></Settings>"#),&mut count).is_err());
        for value in ["../x", "a/b", "a\\b", "..", "", "C:x"] {
            assert!(component(value).is_err());
        }
    }
}
