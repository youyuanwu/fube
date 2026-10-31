//! ServiceModel identifiers and PAL/wc16.c:131-150 (startup C locale).
use std::cmp::Ordering;

/// Native PAL signed-short comparison operand; non-ASCII input is accepted.
pub fn native_upper_unit(unit: u16) -> i16 {
    if (u16::from(b'a')..=u16::from(b'z')).contains(&unit) {
        (unit - 32) as i16
    } else {
        unit as i16
    }
}

/// No Unicode folding or output normalization. NUL terminates native comparison.
pub fn compare_names(left: &str, right: &str) -> Ordering {
    let mut left = left.encode_utf16().chain(std::iter::once(0));
    let mut right = right.encode_utf16().chain(std::iter::once(0));
    loop {
        let l = native_upper_unit(left.next().expect("terminator"));
        let r = native_upper_unit(right.next().expect("terminator"));
        match l.cmp(&r) {
            Ordering::Equal if l != 0 => {}
            result => return result,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplicationId {
    pub type_name: String,
    pub number: u32,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackageId {
    pub application: ApplicationId,
    pub name: String,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceTypeId {
    pub package: PackageId,
    pub name: String,
}

pub fn compare_applications(a: &ApplicationId, b: &ApplicationId) -> Ordering {
    a.number
        .cmp(&b.number)
        .then_with(|| compare_names(&a.type_name, &b.type_name))
}
pub fn compare_packages(a: &PackageId, b: &PackageId) -> Ordering {
    compare_applications(&a.application, &b.application)
        .then_with(|| compare_names(&a.name, &b.name))
}
/// ServiceTypeIdentifier.cpp:78-118. Intentionally not Rust `Ord`: malformed
/// raw NUL application triples need not form a native total order.
pub fn compare_service_types(a: &ServiceTypeId, b: &ServiceTypeId) -> Ordering {
    let empty = |a: &ApplicationId| a.number == 0 && a.type_name.is_empty();
    let package = if empty(&a.package.application) && empty(&b.package.application) {
        Ordering::Equal
    } else {
        compare_packages(&a.package, &b.package)
    };
    package.then_with(|| compare_names(&a.name, &b.name))
}

/// GUID bytes in canonical textual/network order, NOT native wire byte order.
/// The zero GUID is SharedProcess. Identity data does not enable exclusive execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Activation(pub [u8; 16]);
impl Activation {
    pub const SHARED: Self = Self([0; 16]);
    pub fn from_uuid(value: uuid::Uuid) -> Self {
        Self(*value.as_bytes())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackageInstance {
    pub package: PackageId,
    pub activation: Activation,
    pub public_activation: String,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TypeInstance {
    pub service_type: ServiceTypeId,
    pub activation: Activation,
    pub public_activation: String,
}
impl TypeInstance {
    pub fn package_instance(&self) -> PackageInstance {
        PackageInstance {
            package: self.service_type.package.clone(),
            activation: self.activation,
            public_activation: self.public_activation.clone(),
        }
    }
}
pub fn compare_package_instances(a: &PackageInstance, b: &PackageInstance) -> Ordering {
    compare_packages(&a.package, &b.package).then(a.activation.cmp(&b.activation))
}
pub fn compare_type_instances(a: &TypeInstance, b: &TypeInstance) -> Ordering {
    compare_packages(&a.service_type.package, &b.service_type.package)
        .then(a.activation.cmp(&b.activation))
        .then_with(|| compare_names(&a.service_type.name, &b.service_type.name))
}

/// Raw comparison data is unrestricted; admitted/consumed identities must obey
/// ApplicationIdentifier.cpp:27-34 and ServicePackageIdentifier.cpp:42-49.
/// These local failures model native constructor assertions, not wire errors.
pub(crate) fn validate_package(package: &PackageId) -> crate::Result<()> {
    let app = &package.application;
    if app.type_name.is_empty() && app.number != 0 {
        return Err(crate::Error::NativeInvariant(
            "empty application name with nonzero number",
        ));
    }
    if *app != ApplicationId::default() && package.name.is_empty() {
        return Err(crate::Error::NativeInvariant(
            "nonempty application with empty package name",
        ));
    }
    Ok(())
}

/// HostingSubsystem.cpp:165-180 uses a CI STRING map, not a structured key:
/// `<ServicePackageIdentifier.ToString()>_<activation GUID text>`.
/// Preserve raw string length (including NUL and delimiters) before comparison.
pub fn public_activation_map_key(
    package: &PackageId,
    activation: Activation,
) -> crate::Result<String> {
    validate_package(package)?;
    let rendered = if package.application == ApplicationId::default() {
        package.name.clone()
    } else {
        format!(
            "{}_App{}:{}",
            package.application.type_name, package.application.number, package.name
        )
    };
    Ok(format!(
        "{}_{}",
        rendered,
        uuid::Uuid::from_bytes(activation.0)
    ))
}

/// Public activation is associated provenance, never an extra map dimension.
pub(crate) fn validate_public(activation: Activation, public: &str) -> crate::Result<()> {
    // Common/Guid.cpp:161-175, stricter than UUID's simple/braced/URN parser.
    let native_guid = public.len() == 36
        && public.bytes().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                c == b'-'
            } else {
                c.is_ascii_hexdigit()
            }
        });
    if (activation != Activation::SHARED && public.is_empty())
        || (!public.is_empty() && !native_guid)
    {
        return Err(crate::Error::InvalidInput);
    }
    Ok(())
}

impl PackageId {
    pub(crate) fn bytes(&self) -> usize {
        self.application.type_name.len() + self.name.len()
    }
}
impl TypeInstance {
    pub(crate) fn bytes(&self) -> usize {
        self.service_type.package.bytes()
            + self.service_type.name.len()
            + self.public_activation.len()
    }
}
