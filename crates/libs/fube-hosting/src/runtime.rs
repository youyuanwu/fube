//! RuntimeRegistrationTable.cpp:53-266; FabricRuntimeManager.cpp:501-679.
use crate::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRecord {
    pub context: RuntimeContext,
    pub valid: bool,
}
impl RuntimeRecord {
    pub(crate) fn bytes(&self) -> usize {
        16 + self.context.bytes()
    }
}
impl State {
    pub(crate) fn runtime_index(&self, id: &str) -> Result<usize> {
        if self.runtimes_closed {
            return Err(Error::ObjectClosed);
        }
        self.runtimes
            .iter()
            .position(|r| compare_names(&r.context.runtime_id, id).is_eq())
            .ok_or(Error::RuntimeNotRegistered)
    }
    pub(crate) fn runtime(&self, id: &str) -> Result<&RuntimeContext> {
        let index = self.runtime_index(id)?;
        if !self.runtimes[index].valid {
            return Err(Error::RuntimeNotRegistered);
        }
        Ok(&self.runtimes[index].context)
    }
    pub(crate) fn remove_runtime(&mut self, id: &str) -> Result<Option<RuntimeContext>> {
        let index = self.runtime_index(id)?;
        let record = self.runtimes.remove(index);
        Ok(record.valid.then_some(record.context))
    }
    pub(crate) fn remove_runtimes_for_host(&mut self, host: &str) -> Result<usize> {
        if self.runtimes_closed {
            return Err(Error::ObjectClosed);
        }
        let prior = self.runtimes.len();
        self.runtimes
            .retain(|r| !compare_names(&r.context.host_id, host).is_eq());
        Ok(prior - self.runtimes.len())
    }
}
impl Hosting {
    pub fn add_runtime(&mut self, context: RuntimeContext) -> Result<()> {
        validate_package(&context.package.package)?;
        validate_public(
            context.package.activation,
            &context.package.public_activation,
        )?;
        self.change(|s| {
            match s.runtime_index(&context.runtime_id) {
                Ok(index) => {
                    return Err(if s.runtimes[index].valid {
                        Error::RuntimeAlreadyRegistered
                    } else {
                        Error::OperationFailed
                    })
                }
                Err(Error::RuntimeNotRegistered) => {}
                Err(e) => return Err(e),
            }
            s.runtimes.push(RuntimeRecord {
                context,
                valid: false,
            });
            Ok(())
        })
    }
    pub fn check_runtime_host(&self, context: &RuntimeContext) -> Result<()> {
        validate_package(&context.package.package)?;
        if self.state.host_valid(&context.host_id)? {
            Ok(())
        } else {
            Err(Error::HostNotRegistered)
        }
    }
    /// Table Validate deliberately has no second host check.
    pub fn validate_runtime(&mut self, id: &str) -> Result<RuntimeContext> {
        self.change(|s| {
            let index = s.runtime_index(id)?;
            s.runtimes[index].valid = true;
            Ok(s.runtimes[index].context.clone())
        })
    }
    pub fn find_runtime(&self, id: &str) -> Result<RuntimeContext> {
        self.state.runtime(id).cloned()
    }
    pub fn runtime_records(&self) -> &[RuntimeRecord] {
        &self.state.runtimes
    }
    pub fn close_runtime_table(&mut self) -> Result<()> {
        self.change(|s| {
            s.runtimes_closed = true;
            s.runtimes.clear();
            Ok(())
        })
    }
    /// Preflight for the public unregister path. Native Remove returns
    /// `Success + null` on tentative rows, but its public callback dereferences null
    /// (FabricRuntimeManager.cpp:657-663,896-900). No safe native success exists.
    /// H1 rejects that source-invariant branch before destructive removal.
    pub fn check_public_runtime_unregister(&self, id: &str) -> Result<RuntimeContext> {
        let index = self.state.runtime_index(id)?;
        if !self.state.runtimes[index].valid {
            return Err(Error::NativeInvariant(
                "tentative public runtime unregister",
            ));
        }
        Ok(self.state.runtimes[index].context.clone())
    }
    /// Safe table cleanup: Success + None is the native tentative-row result.
    /// This does NOT claim completed public runtime-unregister processing.
    pub fn remove_runtime_entry(&mut self, id: &str) -> Result<Option<RuntimeContext>> {
        self.change(|s| s.remove_runtime(id))
    }
    pub fn remove_host_runtime_entries(&mut self, host: &str) -> Result<Vec<RuntimeContext>> {
        self.change(|s| {
            if s.runtimes_closed {
                return Err(Error::ObjectClosed);
            }
            let mut valid = Vec::new();
            s.runtimes.retain(|r| {
                if compare_names(&r.context.host_id, host).is_eq() {
                    if r.valid {
                        valid.push(r.context.clone());
                    }
                    false
                } else {
                    true
                }
            });
            Ok(valid)
        })
    }
    pub fn cleanup_runtime(&mut self, id: &str, original: Error) -> ProcessorResult {
        ProcessorResult {
            result: Err(original),
            cleanup: Some(self.remove_runtime_entry(id).map(|_| ())),
        }
    }
    pub fn register_runtime(&mut self, context: RuntimeContext) -> ProcessorResult {
        let id = context.runtime_id.clone();
        if let Err(e) = self.add_runtime(context.clone()) {
            return ProcessorResult {
                result: Err(e),
                cleanup: None,
            };
        }
        let result = self
            .check_runtime_host(&context)
            .and_then(|_| self.validate_runtime(&id).map(|_| ()));
        match result {
            Ok(()) => ProcessorResult {
                result: Ok(()),
                cleanup: None,
            },
            Err(e) => self.cleanup_runtime(&id, e),
        }
    }
}
