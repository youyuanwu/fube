//! Volatile node-wide service-type reporting. Native authorities:
//! Hosting.ServiceTypeMap.cpp:20-145, Hosting.ServiceTypeMapEntity.cpp:28-80.
//! Timers are trusted monotonic inputs, not transport delivery/acknowledgment.
use super::*;
use fube_transport::serialization::serde::ObjectArray;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TypeKey(u32, String, String, String);
fn folded(value: &str) -> Result<String> {
    // Pinned PAL wc16.cpp:137-155 compares towupper per UTF-16 code unit,
    // explicitly describing its ASCII profile. Do not invent Unicode folding.
    if !value.is_ascii() || value.contains('\0') {
        return Err(Failure::Unsupported(
            "service-type execution keys require non-NUL ASCII",
        ));
    }
    Ok(value.to_ascii_uppercase())
}
pub(super) fn key(id: &ServiceType) -> Result<TypeKey> {
    let app = &id.package.application;
    let empty = app.number == 0 && app.type_name.is_empty();
    Ok(TypeKey(
        app.number,
        folded(&app.type_name)?,
        if empty {
            String::new()
        } else {
            folded(&id.package.name)?
        },
        folded(&id.name)?,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceTypeEntry {
    pub info: ServiceTypeInfo,
    pub references: usize,
    pub pending: bool,
    pub last_sent_ms: Option<u64>,
}
#[derive(Default)]
pub(super) struct Coordinator {
    entries: BTreeMap<TypeKey, ServiceTypeEntry>,
    // Conservative largest identity/payload combination among in-flight
    // reservations. Retain the bound until the last owner resolves: any one
    // of these candidates may become the first identifier if others fail.
    reserved: BTreeMap<TypeKey, (usize, ServiceTypeInfo)>,
    last_work_ms: Option<u64>,
    observed_ms: u64,
}
impl Coordinator {
    pub fn snapshot(&self) -> Vec<ServiceTypeEntry> {
        self.entries.values().cloned().collect()
    }
    pub fn reserve(&mut self, info: &ServiceTypeInfo, settings: &Settings) -> Result<TypeKey> {
        let key = key(&info.versioned.id)?;
        let mut projected = info.clone();
        let payload_len = |i: &ServiceTypeInfo| {
            let mut payload = i.clone();
            payload.versioned.id = ServiceType::default();
            to_vec(&payload)
                .map(|b| b.len())
                .map_err(|_| Failure::Capacity)
        };
        for prior in self
            .entries
            .get(&key)
            .map(|e| &e.info)
            .into_iter()
            .chain(self.reserved.get(&key).map(|r| &r.1))
        {
            if to_vec(&prior.versioned.id)
                .map_err(|_| Failure::Capacity)?
                .len()
                > to_vec(&projected.versioned.id)
                    .map_err(|_| Failure::Capacity)?
                    .len()
            {
                projected.versioned.id = prior.versioned.id.clone();
            }
            if payload_len(prior)? > payload_len(&projected)? {
                projected.versioned.version = prior.versioned.version.clone();
                projected.code_package = prior.code_package.clone();
            }
        }
        let singleton = ServiceTypeNotification {
            infos: ObjectArray(vec![projected.clone()]),
        };
        if to_vec(&singleton).map_err(|_| Failure::Capacity)?.len()
            > settings.notification_batch_bytes
        {
            return Err(Failure::Capacity);
        }
        let slots = self
            .entries
            .keys()
            .chain(self.reserved.keys())
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        if !self.entries.contains_key(&key)
            && !self.reserved.contains_key(&key)
            && slots >= settings.max_service_types
        {
            return Err(Failure::Capacity);
        }
        let entry = self
            .reserved
            .entry(key.clone())
            .or_insert((0, projected.clone()));
        entry.0 += 1;
        entry.1 = projected;
        Ok(key)
    }
    pub fn unreserve(&mut self, key: &TypeKey) {
        let count = self.reserved.get_mut(key).expect("owned type reservation");
        count.0 -= 1;
        if count.0 == 0 {
            self.reserved.remove(key);
        }
    }
    pub fn add(&mut self, key: TypeKey, info: ServiceTypeInfo) -> bool {
        let entry = self.entries.entry(key).or_insert_with(|| ServiceTypeEntry {
            info: info.clone(),
            references: 0,
            pending: false,
            last_sent_ms: None,
        });
        entry.references += 1;
        if entry.references == 1 || entry.info.versioned.version != info.versioned.version {
            // Native keeps the first id spelling, replaces version/name on ANY
            // different full version, not only an upgrade.
            entry.info.versioned.version = info.versioned.version;
            entry.info.code_package = info.code_package;
            entry.pending = true;
            entry.last_sent_ms = None;
            return true;
        }
        false
    }
    pub fn remove(&mut self, id: &ServiceType) {
        let key = key(id).expect("admitted attached key");
        let entry = self.entries.get_mut(&key).expect("attached map reference");
        entry.references -= 1;
        if entry.references == 0 {
            self.entries.remove(&key);
        }
    }
    pub fn acknowledge(&mut self, body: &ServiceTypeNotification) -> Result<()> {
        // Validate every key before making even a partial mutation.
        let keys = body
            .infos
            .0
            .iter()
            .map(|i| key(&i.versioned.id))
            .collect::<Result<Vec<_>>>()?;
        for (info, key) in body.infos.0.iter().zip(keys) {
            if let Some(entry) = self.entries.get_mut(&key) {
                if entry.info.versioned.version.instance == info.versioned.version.instance {
                    entry.pending = false;
                }
            }
        }
        Ok(())
    }
    pub fn retry(&mut self, now: u64, settings: &Settings) -> Result<Vec<Effect>> {
        if now < self.observed_ms {
            return Err(Failure::Admission("notification clock moved backwards"));
        }
        self.observed_ms = now;
        if !self.entries.values().any(|e| e.pending) {
            return Ok(vec![]);
        }
        let mut effects = Vec::new();
        let node_due = self
            .last_work_ms
            .is_none_or(|t| now.saturating_sub(t) >= settings.notification_node_ms);
        if node_due {
            let mut selected = Vec::new();
            let mut infos = Vec::new();
            for (key, entry) in &self.entries {
                if !entry.pending
                    || entry
                        .last_sent_ms
                        .is_some_and(|t| now.saturating_sub(t) < settings.notification_type_ms)
                {
                    continue;
                }
                if infos.len() == settings.notification_batch_count {
                    break;
                }
                let mut candidate = infos.clone();
                candidate.push(entry.info.clone());
                if to_vec(&ServiceTypeNotification {
                    infos: ObjectArray(candidate),
                })
                .map_or(true, |bytes| {
                    bytes.len() > settings.notification_batch_bytes
                }) {
                    break;
                }
                infos.push(entry.info.clone());
                selected.push(key.clone());
            }
            self.last_work_ms = Some(now);
            if !infos.is_empty() {
                for key in selected {
                    self.entries.get_mut(&key).unwrap().last_sent_ms = Some(now);
                }
                effects.push(Effect::ServiceTypeNotification(Box::new(
                    ServiceTypeNotification {
                        infos: ObjectArray(infos),
                    },
                )));
            }
        }
        effects.push(Effect::RequestServiceTypeRetry {
            after_ms: settings.notification_retry_ms,
        });
        Ok(effects)
    }
}

pub(super) fn info(state: &EntityState) -> ServiceTypeInfo {
    ServiceTypeInfo {
        versioned: VersionedServiceType {
            id: state.body.service.service_type.clone(),
            version: state.body.service.version.clone(),
        },
        code_package: state
            .runtime
            .as_ref()
            .expect("attached registration")
            .code_package
            .clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn projected_retained_identifier_and_pending_candidates_must_fit() {
        let mut a = info("Type", 1);
        a.versioned.id.package.name = "P".repeat(200);
        let mut b = info("Type", 2);
        b.code_package = "C".repeat(200);
        let size = |i| {
            to_vec(&ServiceTypeNotification {
                infos: ObjectArray(vec![i]),
            })
            .unwrap()
            .len()
        };
        let s = Settings {
            notification_batch_bytes: size(a.clone()).max(size(b.clone())),
            ..Settings::default()
        };
        let mut map = Coordinator::default();
        let r = map.reserve(&a, &s).unwrap();
        assert_eq!(map.reserve(&b, &s), Err(Failure::Capacity));
        map.add(r.clone(), a);
        map.unreserve(&r);
        assert_eq!(map.reserve(&b, &s), Err(Failure::Capacity));
        assert_eq!(map.retry(0, &s).unwrap().len(), 2);
    }
    #[test]
    fn codec_ceiling_emits_valid_prefix_and_retains_remaining_due_work() {
        let mut map = Coordinator::default();
        let s = Settings {
            max_service_types: 128,
            notification_batch_bytes: 4 * 1024 * 1024,
            ..Settings::default()
        };
        for n in 0..65 {
            let mut i = info(&format!("Type{n:02}"), 1);
            i.code_package = "C".repeat(32768);
            attach(&mut map, i, &s);
        }
        let effects = map.retry(0, &s).unwrap();
        let Effect::ServiceTypeNotification(body) = &effects[0] else {
            panic!()
        };
        assert!(!body.infos.0.is_empty() && body.infos.0.len() < 65);
        assert_eq!(effects.len(), 2);
        assert!(map.snapshot().iter().all(|e| e.pending));
        let effects = map.retry(5000, &s).unwrap();
        assert!(
            matches!(&effects[0], Effect::ServiceTypeNotification(body) if !body.infos.0.is_empty())
        );
    }
    fn info(name: &str, instance: u64) -> ServiceTypeInfo {
        ServiceTypeInfo {
            versioned: VersionedServiceType {
                id: ServiceType {
                    name: name.into(),
                    ..ServiceType::default()
                },
                version: VersionInstance {
                    instance,
                    ..VersionInstance::default()
                },
            },
            code_package: format!("Code{instance}"),
        }
    }
    fn attach(map: &mut Coordinator, i: ServiceTypeInfo, s: &Settings) -> bool {
        let k = map.reserve(&i, s).unwrap();
        let send = map.add(k.clone(), i);
        map.unreserve(&k);
        send
    }
    #[test]
    fn native_shared_count_last_different_version_and_no_removal_fallback() {
        // Hosting.ServiceTypeMapEntity.cpp:28-80; unit tests :150-243.
        let mut map = Coordinator::default();
        let s = Settings::default();
        let a = info("Type", 2);
        assert!(attach(&mut map, a.clone(), &s));
        let mut same = a.clone();
        same.code_package = "Ignored".into();
        assert!(!attach(&mut map, same, &s));
        assert_eq!(map.snapshot()[0].info, a);
        let downgrade = info("TYPE", 1);
        assert!(attach(&mut map, downgrade.clone(), &s));
        assert_eq!(map.snapshot()[0].references, 3);
        map.remove(&downgrade.versioned.id);
        let entry = &map.snapshot()[0];
        assert_eq!(entry.references, 2);
        assert_eq!(entry.info.versioned.version.instance, 1);
        assert_eq!(entry.info.versioned.id.name, "Type");
        for _ in 0..2 {
            map.remove(&a.versioned.id);
        }
        assert!(map.snapshot().is_empty());
        assert!(attach(&mut map, a, &s)); // a new round after last removal
    }
    #[test]
    fn native_instance_only_partial_ack_and_throttled_bounded_batches() {
        // ServiceTypeNotification.cpp:222-309; same-instance differing fields
        // is handler-derived, not the misleading "ExactMatch" test name.
        let mut map = Coordinator::default();
        let s = Settings {
            notification_batch_count: 1,
            ..Settings::default()
        };
        let a = info("A", 7);
        let b = info("B", 8);
        attach(&mut map, a.clone(), &s);
        attach(&mut map, b.clone(), &s);
        let sent = map.retry(0, &s).unwrap();
        assert!(
            matches!(&sent[0], Effect::ServiceTypeNotification(body) if body.infos.0 == [a.clone()])
        );
        assert!(map.snapshot().iter().all(|e| e.pending));
        assert_eq!(map.retry(4999, &s).unwrap().len(), 1);
        assert!(
            matches!(&map.retry(5000, &s).unwrap()[0], Effect::ServiceTypeNotification(body) if body.infos.0 == [b.clone()])
        );
        let mut ack = a.clone();
        ack.versioned.version.version.value.major += 1;
        ack.code_package = "NotMatched".into();
        map.acknowledge(&ServiceTypeNotification {
            infos: ObjectArray(vec![ack]),
        })
        .unwrap();
        assert!(!map.snapshot()[0].pending);
        assert!(map.snapshot()[1].pending);
        let mut wrong = b.clone();
        wrong.versioned.version.instance += 1;
        map.acknowledge(&ServiceTypeNotification {
            infos: ObjectArray(vec![wrong]),
        })
        .unwrap();
        assert!(map.snapshot()[1].pending);
        assert_eq!(map.retry(10000, &s).unwrap().len(), 1);
        assert_eq!(map.retry(50000, &s).unwrap().len(), 2);
        map.acknowledge(&ServiceTypeNotification {
            infos: ObjectArray(vec![b]),
        })
        .unwrap();
        assert!(map.retry(50001, &s).unwrap().is_empty());
        assert!(map.retry(1, &s).is_err());
    }
    #[test]
    fn identity_profile_and_capacity_reservations_are_explicit() {
        // ServiceTypeIdentifier.cpp:78-118; ApplicationIdentifier.cpp:86-123.
        let mut a = info("Type", 1);
        a.versioned.id.package.name = "Ignored".into();
        let mut b = a.clone();
        b.versioned.id.name = "type".into();
        b.versioned.id.package.name = "Other".into();
        assert_eq!(key(&a.versioned.id), key(&b.versioned.id));
        b.versioned.id.package.application.number = 1;
        assert_ne!(key(&a.versioned.id), key(&b.versioned.id));
        let mut map = Coordinator::default();
        let s = Settings {
            max_service_types: 1,
            ..Settings::default()
        };
        let r = map.reserve(&a, &s).unwrap();
        assert_eq!(map.reserve(&b, &s), Err(Failure::Capacity));
        let again = map.reserve(&a, &s).unwrap();
        map.unreserve(&r);
        assert_eq!(map.reserve(&b, &s), Err(Failure::Capacity));
        map.unreserve(&again);
        assert!(map.reserve(&b, &s).is_ok());
        a.versioned.id.name = "π".into();
        assert!(matches!(key(&a.versioned.id), Err(Failure::Unsupported(_))));
        let s = Settings {
            notification_batch_bytes: 1,
            ..Settings::default()
        };
        assert_eq!(map.reserve(&b, &s), Err(Failure::Capacity));
    }
}
