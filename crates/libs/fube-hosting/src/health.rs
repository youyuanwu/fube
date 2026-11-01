//! Managed source ownership for explicitly construction-time disabled reporting.
//! This is not enabled report processing or an HM acknowledgment.
use crate::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisabledReport {
    pub sequence: u64,
    pub node_id: String,
    pub node_name: String,
    pub node_instance: u64,
    pub package: PackageInstance,
    pub application_name: String,
    pub property: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSource {
    pub id: TypeInstance,
    pub application_name: String,
    pub component: String,
    pub entity_instance: u64,
    pub last_disabled_report: Option<DisabledReport>,
}

#[derive(Debug, Clone)]
pub(crate) struct DisabledHealth {
    pub sources: Vec<HealthSource>,
    sequence: u64,
    max_sources: usize,
    max_bytes: usize,
}
impl DisabledHealth {
    pub fn new(limits: &Limits) -> Self {
        Self {
            sources: Vec::new(),
            sequence: 0,
            max_sources: limits.sources,
            max_bytes: limits.state_bytes,
        }
    }
    fn next(&mut self) -> Result<u64> {
        self.sequence = self.sequence.checked_add(1).ok_or(Error::Capacity)?;
        Ok(self.sequence)
    }
    fn check(&self) -> Result<()> {
        let bytes = self.sources.iter().fold(0usize, |n, s| {
            n.saturating_add(256)
                .saturating_add(s.id.service_type.package.bytes())
                .saturating_add(s.id.service_type.name.len())
                .saturating_add(s.id.public_activation.len())
                .saturating_add(s.application_name.len())
                .saturating_add(s.component.len())
                .saturating_add(s.last_disabled_report.as_ref().map_or(0, |r| {
                    256 + r.node_id.len()
                        + r.node_name.len()
                        + r.package.package.bytes()
                        + r.package.public_activation.len()
                        + r.application_name.len()
                        + r.property.len()
                }))
        });
        if self.sources.len() > self.max_sources || bytes > self.max_bytes {
            Err(Error::Capacity)
        } else {
            Ok(())
        }
    }
    pub fn declare(&mut self, id: TypeInstance, application_name: String) -> Result<()> {
        if application_name.is_empty() {
            return Err(Error::InvalidInput);
        }
        let component = format!("ServiceTypeRegistration:{}", id.service_type.name);
        if self
            .sources
            .iter()
            .any(|s| compare_type_instances(&s.id, &id).is_eq() && s.component == component)
        {
            return Err(Error::NativeInvariant("duplicate health component source"));
        }
        let package = PackageInstance {
            package: id.service_type.package.clone(),
            activation: id.activation,
            public_activation: id.public_activation.clone(),
        };
        let existing = self.sources.iter().find(|s| {
            compare_packages(&s.id.service_type.package, &package.package).is_eq()
                && s.id.activation == package.activation
        });
        let instance = match existing {
            Some(s) if s.application_name != application_name => return Err(Error::InvalidInput),
            Some(s) => s.entity_instance,
            None => self.next()?,
        };
        self.sources.push(HealthSource {
            id,
            application_name,
            component,
            entity_instance: instance,
            last_disabled_report: None,
        });
        self.check()
    }
    pub fn registered(&mut self, id: &TypeInstance, node: &crate::wire::StartReply) -> Result<()> {
        let i = self
            .sources
            .iter()
            .position(|s| compare_type_instances(&s.id, id).is_eq())
            .ok_or(Error::InvalidServiceType)?;
        let sequence = self.next()?;
        let s = &mut self.sources[i];
        s.last_disabled_report = Some(DisabledReport {
            sequence,
            node_id: node.node_id.clone(),
            node_name: node.node_name.clone(),
            node_instance: node.node_instance,
            package: PackageInstance {
                package: s.id.service_type.package.clone(),
                activation: s.id.activation,
                public_activation: s.id.public_activation.clone(),
            },
            application_name: s.application_name.clone(),
            property: format!("ServiceTypeRegistration:{}", s.id.service_type.name),
        });
        self.check()
    }
    pub fn remove(&mut self, id: &TypeInstance, component: &str) -> Result<HealthSource> {
        let i = self
            .sources
            .iter()
            .position(|s| compare_type_instances(&s.id, id).is_eq() && s.component == component)
            .ok_or(Error::NotFound)?;
        // Reporting-disabled removal has no transport work or HM ACK. Returning
        // the retained report flags makes that lifecycle boundary inspectable.
        Ok(self.sources.remove(i))
    }
}
