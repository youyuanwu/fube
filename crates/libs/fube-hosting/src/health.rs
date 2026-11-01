//! Managed source ownership for explicitly construction-time disabled reporting.
//! This is not enabled report processing or an HM acknowledgment.
use crate::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisabledReportKind {
    Registered,
    DeleteEvent,
    DeleteEntity,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisabledReport {
    pub kind: DisabledReportKind,
    pub source: &'static str,
    pub sequence: u64,
    pub entity_instance: u64,
    pub node_id: String,
    pub node_name: String,
    pub node_instance: u64,
    pub package: PackageInstance,
    pub application_name: String,
    pub property: String,
    /// None is native TimeSpan::MaxValue, not an unbounded work deadline.
    pub ttl_ticks: Option<i64>,
    pub remove_when_expired: bool,
}
impl DisabledReport {
    fn bytes(&self) -> usize {
        256 + self.node_id.len()
            + self.node_name.len()
            + self.package.package.bytes()
            + self.package.public_activation.len()
            + self.application_name.len()
            + self.property.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSource {
    pub id: TypeInstance,
    pub application_name: String,
    pub component: String,
    pub entity_instance: u64,
    pub last_disabled_report: Option<DisabledReport>,
}
impl HealthSource {
    fn bytes(&self) -> usize {
        256 + self.id.service_type.package.bytes()
            + self.id.service_type.name.len()
            + self.id.public_activation.len()
            + self.application_name.len()
            + self.component.len()
            + self
                .last_disabled_report
                .as_ref()
                .map_or(0, DisabledReport::bytes)
    }
    fn package(&self) -> PackageInstance {
        PackageInstance {
            package: self.id.service_type.package.clone(),
            activation: self.id.activation,
            public_activation: self.id.public_activation.clone(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthRemoval {
    pub source: HealthSource,
    pub sequence: u64,
    pub entity_deleted: bool,
    pub disabled_report: Option<DisabledReport>,
}
#[derive(Debug, Clone)]
struct Entity {
    package: PackageInstance,
    application_name: String,
    instance: u64,
    has_sent_report: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DisabledHealth {
    pub sources: Vec<HealthSource>,
    entities: Vec<Entity>,
    sequence: u64,
    max_sources: usize,
    max_bytes: usize,
}
impl DisabledHealth {
    #[cfg(test)]
    pub(crate) fn bound_to_retained(&mut self) {
        self.max_bytes = self.bytes();
    }
    #[cfg(test)]
    pub(crate) fn restore_budget(&mut self, bytes: usize) {
        self.max_bytes = bytes;
    }
    pub fn new(limits: &Limits) -> Self {
        Self {
            sources: Vec::new(),
            entities: Vec::new(),
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
        let bytes = self.bytes();
        if self.sources.len() > self.max_sources || bytes > self.max_bytes {
            Err(Error::Capacity)
        } else {
            Ok(())
        }
    }
    fn bytes(&self) -> usize {
        self.sources.iter().map(HealthSource::bytes).sum::<usize>()
            + self
                .entities
                .iter()
                .map(|e| {
                    128 + e.package.package.bytes()
                        + e.package.public_activation.len()
                        + e.application_name.len()
                })
                .sum::<usize>()
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
        let existing = self
            .entities
            .iter()
            .find(|e| compare_package_instances(&e.package, &package).is_eq());
        let instance = match existing {
            Some(e) if e.application_name != application_name => return Err(Error::InvalidInput),
            Some(e) => e.instance,
            None => {
                let instance = self.next()?;
                self.entities.push(Entity {
                    package,
                    application_name: application_name.clone(),
                    instance,
                    has_sent_report: false,
                });
                instance
            }
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
        let package = self.sources[i].package();
        let entity = self
            .entities
            .iter_mut()
            .find(|e| compare_package_instances(&e.package, &package).is_eq())
            .ok_or(Error::NativeInvariant("health entity missing"))?;
        entity.has_sent_report = true;
        let s = &mut self.sources[i];
        s.last_disabled_report = Some(DisabledReport {
            kind: DisabledReportKind::Registered,
            source: "System.Hosting",
            sequence,
            entity_instance: s.entity_instance,
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
            ttl_ticks: None,
            remove_when_expired: false,
        });
        self.check()
    }
    pub fn remove(
        &mut self,
        id: &TypeInstance,
        component: &str,
        node: &crate::wire::StartReply,
    ) -> Result<HealthRemoval> {
        let i = self
            .sources
            .iter()
            .position(|s| compare_type_instances(&s.id, id).is_eq() && s.component == component)
            .ok_or(Error::NotFound)?;
        let source = self.sources.remove(i);
        let package = source.package();
        let entity_deleted = !self
            .sources
            .iter()
            .any(|s| compare_package_instances(&s.package(), &package).is_eq());
        let ei = self
            .entities
            .iter()
            .position(|e| compare_package_instances(&e.package, &package).is_eq())
            .ok_or(Error::NativeInvariant("health entity missing"))?;
        let reported = if entity_deleted {
            self.entities.remove(ei).has_sent_report
        } else {
            source.last_disabled_report.is_some()
        };
        let sequence = self.next()?;
        let disabled_report = reported.then(|| DisabledReport {
            kind: if entity_deleted {
                DisabledReportKind::DeleteEntity
            } else {
                DisabledReportKind::DeleteEvent
            },
            source: "System.Hosting",
            sequence,
            entity_instance: source.entity_instance,
            node_id: node.node_id.clone(),
            node_name: node.node_name.clone(),
            node_instance: node.node_instance,
            package,
            application_name: source.application_name.clone(),
            property: if entity_deleted {
                "DeleteProperty".into()
            } else {
                source.component.clone()
            },
            ttl_ticks: Some(if entity_deleted {
                72_000_000_000
            } else {
                1_000_000
            }),
            remove_when_expired: !entity_deleted,
        });
        let removal = HealthRemoval {
            source,
            sequence,
            entity_deleted,
            disabled_report,
        };
        // Projected result and retained state share a finite logical budget.
        if self
            .bytes()
            .saturating_add(removal.source.bytes())
            .saturating_add(
                removal
                    .disabled_report
                    .as_ref()
                    .map_or(0, DisabledReport::bytes),
            )
            > self.max_bytes
        {
            return Err(Error::Capacity);
        }
        Ok(removal)
    }
}
