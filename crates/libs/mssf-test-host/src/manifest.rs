//! Explicitly supported XML, not a partial deserializer of the full SF schema.
use crate::{require, HostError, Result};
use roxmltree::{Document, Node};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

pub(crate) const NS: &str = "http://schemas.microsoft.com/2011/01/fabric";
const XML_LIMIT: u64 = 1024 * 1024;
type Parameters = BTreeMap<String, String>;

#[derive(Debug)]
pub(crate) struct Endpoint {
    pub name: String,
    pub port: u16,
}

#[derive(Debug)]
pub(crate) struct Package {
    pub application_type: String,
    pub application_version: String,
    pub application_name: String,
    pub service_manifest: String,
    pub manifest_version: String,
    pub code_name: String,
    pub code_version: String,
    pub service_type: String,
    pub service_name: String,
    pub program: String,
    pub arguments: Vec<String>,
    pub argument_text: String,
    pub working_folder: String,
    pub endpoints: Vec<Endpoint>,
    pub source_code: PathBuf,
}

fn invalid(message: impl std::fmt::Display) -> HostError {
    HostError(format!("manifest validation: {message}"))
}

pub(crate) fn identifier(value: &str) -> Result<()> {
    require(
        !value.is_empty()
            && value.len() <= 128
            && value != "."
            && value != ".."
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b)),
        &format!("manifest validation: unsafe identifier {value:?}"),
    )
}

fn program_path(value: &str) -> Result<()> {
    for part in value.split('/') {
        identifier(part)?;
    }
    Ok(())
}

fn read_xml(path: &Path) -> Result<String> {
    let metadata = fs::symlink_metadata(path)?;
    require(
        metadata.is_file() && metadata.len() <= XML_LIMIT,
        "manifest must be a regular file of at most 1MiB",
    )?;
    // The metadata limit also bounds normal inputs before allocation. Recheck the
    // read for a concurrently replaced file; trusted inputs must remain immutable.
    use std::io::Read;
    let mut text = String::new();
    fs::File::open(path)?
        .take(XML_LIMIT + 1)
        .read_to_string(&mut text)?;
    require(text.len() as u64 <= XML_LIMIT, "manifest exceeds 1MiB")?;
    Ok(text)
}

fn document(text: &str) -> Result<Document<'_>> {
    // roxmltree defaults to allow_dtd=false: no external or custom entity input.
    let doc = Document::parse(text).map_err(invalid)?;
    require(
        !doc.descendants().any(|n| n.is_pi()),
        "manifest processing instructions are unsupported",
    )?;
    Ok(doc)
}

fn shape(node: Node<'_, '_>, name: &str, attrs: &[&str], children: &[&str]) -> Result<()> {
    require(
        node.tag_name().name() == name && node.tag_name().namespace() == Some(NS),
        &format!("manifest validation: expected Fabric {name}"),
    )?;
    for namespace in node.namespaces() {
        require(
            namespace.uri() == NS,
            "unsupported XML namespace declaration",
        )?;
    }
    for attr in node.attributes() {
        require(
            attr.namespace().is_none() && attrs.contains(&attr.name()),
            &format!("unsupported {name} attribute {}", attr.name()),
        )?;
    }
    for child in node.children() {
        if child.is_element() {
            require(
                child.tag_name().namespace() == Some(NS)
                    && children.contains(&child.tag_name().name()),
                &format!("unsupported {name} child {}", child.tag_name().name()),
            )?;
        } else if child.is_text() {
            require(
                child.text().unwrap_or("").trim().is_empty(),
                &format!("unsupported text in {name}"),
            )?;
        }
    }
    Ok(())
}

fn optional<'a, 'input>(node: Node<'a, 'input>, name: &str) -> Result<Option<Node<'a, 'input>>> {
    let mut matches = node.children().filter(|n| n.has_tag_name((NS, name)));
    let first = matches.next();
    require(matches.next().is_none(), &format!("duplicate {name}"))?;
    Ok(first)
}

fn one<'a, 'input>(node: Node<'a, 'input>, name: &str) -> Result<Node<'a, 'input>> {
    optional(node, name)?.ok_or_else(|| invalid(format!("missing {name}")))
}

fn resolve(value: &str, params: &Parameters) -> Result<String> {
    let resolved = if let Some(name) = value.strip_prefix('[').and_then(|v| v.strip_suffix(']')) {
        params
            .get(name)
            .ok_or_else(|| invalid(format!("unknown parameter {name:?}")))?
    } else {
        value
    };
    require(
        !resolved.contains(['[', ']']),
        "embedded or recursive parameter references are unsupported",
    )?;
    Ok(resolved.to_owned())
}

fn attr(node: Node<'_, '_>, name: &str, params: &Parameters) -> Result<String> {
    resolve(
        node.attribute(name).ok_or_else(|| {
            invalid(format!(
                "missing {} attribute {name}",
                node.tag_name().name()
            ))
        })?,
        params,
    )
}

fn id_attr(node: Node<'_, '_>, name: &str, params: &Parameters) -> Result<String> {
    let value = attr(node, name, params)?;
    identifier(&value)?;
    Ok(value)
}

fn text(node: Node<'_, '_>, params: &Parameters) -> Result<String> {
    require(
        node.tag_name().namespace() == Some(NS)
            && node.attributes().len() == 0
            && node.namespaces().all(|ns| ns.uri() == NS)
            && node.children().all(|n| n.is_text() || n.is_comment()),
        "unsupported text element syntax",
    )?;
    let content: String = node
        .children()
        .filter_map(|n| n.text().filter(|_| n.is_text()))
        .collect();
    resolve(&content, params)
}

fn arguments(value: &str) -> Result<Vec<String>> {
    require(
        value.len() <= 16 * 1024
            && !value.chars().any(|c| {
                matches!(c, '\'' | '"' | '\\')
                    || (c.is_control() && !c.is_ascii_whitespace())
                    || (c.is_whitespace() && !c.is_ascii_whitespace())
            }),
        "unsupported Arguments: at most 16KiB, ASCII whitespace tokens, no quotes/escapes/controls",
    )?;
    let args: Vec<_> = value.split_ascii_whitespace().map(str::to_owned).collect();
    require(args.len() <= 128, "Arguments exceeds 128 tokens")?;
    Ok(args)
}

impl Package {
    pub fn load(
        directory: &Path,
        application_name: &str,
        overrides: &[(String, String)],
    ) -> Result<Self> {
        let suffix = application_name
            .strip_prefix("fabric:/")
            .ok_or_else(|| invalid("application name must start with fabric:/"))?;
        program_path(suffix)?;
        require(
            fs::symlink_metadata(directory)?.is_dir(),
            "package must be a real directory",
        )?;
        let directory = directory.canonicalize()?;
        let app_text = read_xml(&directory.join("ApplicationManifest.xml"))?;
        let app_doc = document(&app_text)?;
        let app = app_doc.root_element();
        shape(
            app,
            "ApplicationManifest",
            &["ApplicationTypeName", "ApplicationTypeVersion"],
            &["Parameters", "ServiceManifestImport", "DefaultServices"],
        )?;
        let mut params = Parameters::new();
        if let Some(parameters) = optional(app, "Parameters")? {
            shape(parameters, "Parameters", &[], &["Parameter"])?;
            for parameter in parameters.children().filter(Node::is_element) {
                shape(parameter, "Parameter", &["Name", "DefaultValue"], &[])?;
                let name = parameter
                    .attribute("Name")
                    .ok_or_else(|| invalid("missing parameter Name"))?;
                identifier(name)?;
                let value = parameter
                    .attribute("DefaultValue")
                    .ok_or_else(|| invalid("missing parameter DefaultValue"))?;
                require(
                    !value.contains(['[', ']']),
                    "parameter defaults must be nonrecursive literal values",
                )?;
                require(
                    params.insert(name.to_owned(), value.to_owned()).is_none(),
                    "duplicate parameter",
                )?;
            }
        }
        let mut seen = BTreeSet::new();
        for (name, value) in overrides {
            require(seen.insert(name), "duplicate parameter override")?;
            require(
                params.contains_key(name),
                &format!("unknown parameter override {name}"),
            )?;
            require(
                !value.contains(['[', ']']),
                "override must be a nonrecursive literal",
            )?;
            require(
                value.chars().all(|c| {
                    matches!(c,
                    '\u{9}' | '\u{a}' | '\u{d}' | '\u{20}'..='\u{d7ff}'
                    | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}')
                }),
                "override contains an XML 1.0-invalid character",
            )?;
            params.insert(name.clone(), value.clone());
        }
        let application_type = id_attr(app, "ApplicationTypeName", &params)?;
        let application_version = id_attr(app, "ApplicationTypeVersion", &params)?;
        let import = one(app, "ServiceManifestImport")?;
        shape(
            import,
            "ServiceManifestImport",
            &[],
            &["ServiceManifestRef"],
        )?;
        let reference = one(import, "ServiceManifestRef")?;
        shape(
            reference,
            "ServiceManifestRef",
            &["ServiceManifestName", "ServiceManifestVersion"],
            &[],
        )?;
        let service_manifest = id_attr(reference, "ServiceManifestName", &params)?;
        let manifest_version = id_attr(reference, "ServiceManifestVersion", &params)?;
        let defaults = one(app, "DefaultServices")?;
        shape(defaults, "DefaultServices", &[], &["Service"])?;
        let service = one(defaults, "Service")?;
        shape(
            service,
            "Service",
            &["Name", "ServicePackageActivationMode"],
            &["StatelessService"],
        )?;
        let service_name = id_attr(service, "Name", &params)?;
        if let Some(activation) = service.attribute("ServicePackageActivationMode") {
            require(
                resolve(activation, &params)? == "SharedProcess",
                "only SharedProcess activation is supported",
            )?;
        }
        let stateless = one(service, "StatelessService")?;
        shape(
            stateless,
            "StatelessService",
            &["ServiceTypeName", "InstanceCount"],
            &["SingletonPartition"],
        )?;
        let service_type = id_attr(stateless, "ServiceTypeName", &params)?;
        require(
            attr(stateless, "InstanceCount", &params)? == "1",
            "only InstanceCount=1 is supported",
        )?;
        shape(
            one(stateless, "SingletonPartition")?,
            "SingletonPartition",
            &[],
            &[],
        )?;

        let service_directory = directory.join(&service_manifest);
        require(
            fs::symlink_metadata(&service_directory)?.is_dir(),
            "service package directory must not be a symlink",
        )?;
        let service_text = read_xml(&service_directory.join("ServiceManifest.xml"))?;
        let service_doc = document(&service_text)?;
        let manifest = service_doc.root_element();
        shape(
            manifest,
            "ServiceManifest",
            &["Name", "Version"],
            &["ServiceTypes", "CodePackage", "Resources"],
        )?;
        require(
            id_attr(manifest, "Name", &params)? == service_manifest
                && id_attr(manifest, "Version", &params)? == manifest_version,
            "service manifest import identity/version mismatch",
        )?;
        let types = one(manifest, "ServiceTypes")?;
        shape(types, "ServiceTypes", &[], &["StatelessServiceType"])?;
        let declared = one(types, "StatelessServiceType")?;
        shape(declared, "StatelessServiceType", &["ServiceTypeName"], &[])?;
        require(
            id_attr(declared, "ServiceTypeName", &params)? == service_type,
            "default service type is not the declared stateless type",
        )?;
        let code = one(manifest, "CodePackage")?;
        shape(code, "CodePackage", &["Name", "Version"], &["EntryPoint"])?;
        let code_name = id_attr(code, "Name", &params)?;
        let code_version = id_attr(code, "Version", &params)?;
        for component in [
            format!("{application_type}_App1"),
            format!("{service_manifest}.{code_name}.{code_version}"),
            format!("{service_manifest}.Manifest.{manifest_version}.xml"),
            format!("{service_manifest}.Package.Current.xml"),
            format!("{service_manifest}.Endpoints.txt"),
        ] {
            require(
                component.len() <= 255,
                "composed native package filename exceeds 255 bytes",
            )?;
        }
        let entry = one(code, "EntryPoint")?;
        shape(entry, "EntryPoint", &[], &["ExeHost"])?;
        let exe = one(entry, "ExeHost")?;
        shape(
            exe,
            "ExeHost",
            &[],
            &["Program", "Arguments", "WorkingFolder"],
        )?;
        let program = text(one(exe, "Program")?, &params)?;
        program_path(&program)?;
        let argument_text = optional(exe, "Arguments")?
            .map(|n| text(n, &params))
            .transpose()?
            .unwrap_or_default();
        let arguments = arguments(&argument_text)?;
        let working_folder = optional(exe, "WorkingFolder")?
            .map(|n| text(n, &params))
            .transpose()?
            .unwrap_or_else(|| "Work".into());
        require(
            ["Work", "CodePackage"].contains(&working_folder.as_str()),
            "unsupported WorkingFolder",
        )?;
        let mut endpoints = Vec::new();
        let mut names = BTreeSet::new();
        if let Some(resources) = optional(manifest, "Resources")? {
            shape(resources, "Resources", &[], &["Endpoints"])?;
            if let Some(list) = optional(resources, "Endpoints")? {
                shape(list, "Endpoints", &[], &["Endpoint"])?;
                for endpoint in list.children().filter(Node::is_element) {
                    shape(
                        endpoint,
                        "Endpoint",
                        &["Name", "Protocol", "Type", "Port"],
                        &[],
                    )?;
                    let name = id_attr(endpoint, "Name", &params)?;
                    require(names.insert(name.clone()), "duplicate endpoint name")?;
                    require(
                        attr(endpoint, "Protocol", &params)? == "tcp"
                            && attr(endpoint, "Type", &params)? == "Internal",
                        "only static internal TCP endpoints are supported",
                    )?;
                    let port_text = attr(endpoint, "Port", &params)?;
                    require(
                        port_text.bytes().all(|b| b.is_ascii_digit()),
                        "invalid endpoint Port",
                    )?;
                    let port: u16 = port_text.parse().map_err(invalid)?;
                    require(port != 0, "dynamic endpoint ports are unsupported")?;
                    endpoints.push(Endpoint { name, port });
                }
            }
        }
        let source_code = service_directory.join(&code_name);
        validate_tree(&source_code)?;
        let executable = source_code.join(&program);
        require(
            fs::symlink_metadata(&executable)?.is_file(),
            "Program must be a regular code-package file",
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            require(
                fs::metadata(&executable)?.permissions().mode() & 0o111 != 0,
                "Program is not executable",
            )?;
        }
        Ok(Self {
            application_type,
            application_version,
            application_name: application_name.into(),
            service_manifest,
            manifest_version,
            code_name,
            code_version,
            service_type,
            service_name,
            program,
            arguments,
            argument_text,
            working_folder,
            endpoints,
            source_code,
        })
    }

    pub fn application_id(&self) -> String {
        format!("{}_App1", self.application_type)
    }

    pub fn types_xml(&self) -> String {
        format!(
            r#"<ServiceTypes><StatelessServiceType ServiceTypeName="{}" /></ServiceTypes>"#,
            escape(&self.service_type)
        )
    }

    pub fn code_xml(&self) -> String {
        format!(
            r#"<CodePackage Name="{}" Version="{}"><EntryPoint><ExeHost><Program>{}</Program><Arguments>{}</Arguments><WorkingFolder>{}</WorkingFolder></ExeHost></EntryPoint></CodePackage>"#,
            escape(&self.code_name),
            escape(&self.code_version),
            escape(&self.program),
            escape(&self.argument_text),
            escape(&self.working_folder)
        )
    }

    pub fn endpoints_xml(&self, digested: bool) -> String {
        self.endpoints
            .iter()
            .map(|endpoint| {
                let xml = format!(
                    r#"<Endpoint Name="{}" Protocol="tcp" Type="Internal" Port="{}" />"#,
                    escape(&endpoint.name),
                    endpoint.port
                );
                if digested {
                    format!("<DigestedEndpoint>{xml}</DigestedEndpoint>")
                } else {
                    xml
                }
            })
            .collect()
    }

    pub fn endpoint_file(&self) -> String {
        self.endpoints
            .iter()
            .map(|endpoint| {
                format!(
                    "{};tcp;Internal;{};;;;;127.0.0.1",
                    endpoint.name, endpoint.port
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

pub(crate) fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
        .replace('\r', "&#13;")
        .replace('\n', "&#10;")
        .replace('\t', "&#9;")
}

pub(crate) fn validate_tree(root: &Path) -> Result<()> {
    fn visit(path: &Path, depth: usize, entries: &mut usize, bytes: &mut u64) -> Result<()> {
        *entries += 1;
        require(
            *entries <= 4096 && depth <= 64,
            "code tree exceeds 4096 entries or 64 levels",
        )?;
        let meta = fs::symlink_metadata(path)?;
        if meta.is_dir() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let name = entry.file_name();
                identifier(
                    name.to_str()
                        .ok_or_else(|| invalid("non-UTF8 code filename"))?,
                )?;
                visit(&entry.path(), depth + 1, entries, bytes)?;
            }
        } else {
            require(
                meta.is_file(),
                "code package contains symlink or nonregular file",
            )?;
            *bytes = bytes
                .checked_add(meta.len())
                .ok_or_else(|| invalid("code tree size overflow"))?;
            require(*bytes <= 512 * 1024 * 1024, "code tree exceeds 512MiB")?;
        }
        Ok(())
    }
    require(
        fs::symlink_metadata(root)?.is_dir(),
        "code package must be a real directory",
    )?;
    visit(root, 0, &mut 0, &mut 0)
}

#[cfg(test)]
#[path = "manifest_tests.rs"]
mod tests;
