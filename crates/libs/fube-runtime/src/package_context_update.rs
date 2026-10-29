use super::*;

impl Updater {
    pub fn admit(&self) -> Result<UpdatePermit> {
        if self.handle.registry.closed.load(Ordering::Acquire)
            || self
                .handle
                .registry
                .active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return Err(Error::Closed);
        }
        Ok(UpdatePermit(self.handle.registry.clone()))
    }
    pub fn apply(
        &self,
        update: UpdateContext,
        authority: &Authority,
        deadline: Instant,
    ) -> Result<()> {
        let check = || {
            authority.check()?;
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            if self.handle.registry.closed.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            Ok(())
        };
        check()?;
        let old = self.handle.snapshot();
        let old_code = &old.runtime.code;
        if update.activation.host != old.host_id
            || update.activation.instance <= 0
            || old
                .activation_instance
                .is_some_and(|id| id != update.activation.instance)
            || update.code.id != old_code.id
            || update.code.code_sequence != old_code.code_sequence
            || update.code.package_sequence != old_code.package_sequence
            || update.code.application_name != old_code.application_name
            || update.code.version.instance < old_code.version.instance
        {
            return Err(Error::Protocol(
                "stale or conflicting native package context",
            ));
        }
        if update.code.version.instance == old_code.version.instance {
            if update.code != *old_code {
                return Err(Error::Protocol("conflicting equal package version"));
            }
            check()?;
            if old.activation_instance.is_none() {
                let mut bound = (*old).clone();
                bound.activation_instance = Some(update.activation.instance);
                let mut current = self.handle.current.write().unwrap();
                check()?;
                if !Arc::ptr_eq(&current, &old) {
                    return Err(Error::Protocol("concurrent package update"));
                }
                *current = Arc::new(bound);
            }
            return Ok(());
        }
        let mut candidate = (*old).clone();
        candidate.runtime.code = update.code;
        candidate.activation_instance = Some(update.activation.instance);
        candidate.generation = old
            .generation
            .checked_add(1)
            .ok_or(Error::ResourceLimit("snapshot generation"))?;
        let packages = Arc::new(crate::packages::load(&candidate, &self.deployment)?);
        let old_packages = old.packages()?;
        if !old_packages.code_compatible_with(&packages) {
            return Err(Error::InvalidInput(
                "native context update changed unsupported code/profile",
            ));
        }
        let configs = changes(
            old_packages.configuration_packages(),
            packages.configuration_packages(),
            ConfigurationPackage::description,
        );
        let data = changes(
            old_packages.data_packages(),
            packages.data_packages(),
            DataPackage::description,
        );
        candidate.endpoints = packages.endpoints().clone();
        candidate.packages = Some(packages);
        check()?;
        {
            let mut current = self.handle.current.write().unwrap();
            check()?;
            if !Arc::ptr_eq(&current, &old) {
                return Err(Error::Protocol("concurrent package update"));
            }
            *current = Arc::new(candidate);
        }
        let handlers = self
            .handle
            .registry
            .handlers
            .lock()
            .unwrap()
            .configuration
            .clone();
        for (_, callback) in handlers {
            for event in &configs {
                check()?;
                callback(event.clone());
            }
        }
        let handlers = self.handle.registry.handlers.lock().unwrap().data.clone();
        for (_, callback) in handlers {
            for event in &data {
                check()?;
                callback(event.clone());
            }
        }
        check()
    }
}
