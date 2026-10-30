//! Explicit non-SDK stateless identity and per-record reporting observations.
use crate::{api::Partition, extensions::ExecutionRuntime, *};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

#[derive(Clone)]
pub struct InstanceIntegrationProvider(
    Arc<dyn Fn(uuid::Uuid, i64) -> Result<InstanceRuntimeIntegration> + Send + Sync>,
);
impl InstanceIntegrationProvider {
    pub(crate) fn new(
        lookup: impl Fn(uuid::Uuid, i64) -> Result<InstanceRuntimeIntegration> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(lookup))
    }
    pub fn capture(
        &self,
        partition: uuid::Uuid,
        instance: i64,
    ) -> Result<InstanceRuntimeIntegration> {
        (self.0)(partition, instance)
    }
}
#[derive(Clone)]
pub struct InstanceRuntimeIntegration {
    pub(crate) partition: Partition,
    pub(crate) execution: ExecutionRuntime,
}
impl InstanceRuntimeIntegration {
    pub fn partition(&self) -> Arc<dyn IStatelessServicePartition> {
        Arc::new(self.partition.clone())
    }
    pub fn identity(&self) -> &crate::api::ServiceIdentity {
        &self.partition.identity
    }
    pub fn validate(&self) -> Result<()> {
        self.partition.ensure_valid()
    }
    pub fn authority_remaining(&self) -> Result<std::time::Duration> {
        self.partition.authority_remaining()
    }
    pub fn execution(&self) -> ExecutionRuntime {
        self.execution.clone()
    }
    pub fn fault_delivery(&self) -> Option<crate::reporting::FaultDelivery> {
        self.partition.fault_delivery()
    }
    /// One immutable observation record, associated before emission. It is not
    /// a mutable "last report" slot and conveys no remote durability guarantee.
    pub fn observe_report(&self) -> (Arc<dyn IStatelessServicePartition>, ReportingObservation) {
        let observation = ReportingObservation::new();
        (
            Arc::new(ObservedPartition {
                partition: self.partition.clone(),
                observation: observation.clone(),
            }),
            observation,
        )
    }
}
#[derive(Clone, Debug)]
pub struct ReportCompletion {
    pub health_sequence: Option<i64>,
    pub fault: Option<crate::reporting::FaultDelivery>,
}
#[derive(Clone)]
pub struct ReportingObservation(Arc<Observation>);
struct Observation {
    started: AtomicBool,
    result: Mutex<Option<Result<ReportCompletion>>>,
}
impl ReportingObservation {
    fn new() -> Self {
        Self(Arc::new(Observation {
            started: AtomicBool::new(false),
            result: Mutex::new(None),
        }))
    }
    pub fn result(&self) -> Option<Result<ReportCompletion>> {
        self.0.result.lock().unwrap().clone()
    }
    fn complete(&self, call: impl FnOnce() -> Result<ReportCompletion>) -> Result<()> {
        if self.0.started.swap(true, Ordering::AcqRel) {
            return Err(Error::new(
                ErrorKind::InvalidState,
                "a report observation can be submitted only once",
            ));
        }
        let result = call();
        *self.0.result.lock().unwrap() = Some(result.clone());
        result.map(|_| ())
    }
}
struct ObservedPartition {
    partition: Partition,
    observation: ReportingObservation,
}
impl IStatelessServicePartition for ObservedPartition {
    fn get_partition_info(&self) -> Result<ServicePartitionInformation> {
        self.partition.get_partition_info()
    }
    fn report_load(&self, metrics: &[LoadMetric]) -> Result<()> {
        self.observation.complete(|| {
            IStatelessServicePartition::report_load(&self.partition, metrics).map(|_| {
                ReportCompletion {
                    health_sequence: None,
                    fault: None,
                }
            })
        })
    }
    fn report_move_cost(&self, cost: MoveCost) -> Result<()> {
        self.observation.complete(|| {
            IStatelessServicePartition::report_move_cost(&self.partition, cost).map(|_| {
                ReportCompletion {
                    health_sequence: None,
                    fault: None,
                }
            })
        })
    }
    fn report_fault(&self, fault: FaultType) -> Result<()> {
        self.observation.complete(|| {
            self.partition.ensure_valid()?;
            let reporting = self.partition.reporting.clone();
            let worker = reporting.clone();
            let fault = reporting.complete(async move { worker.fault(fault).await })?;
            Ok(ReportCompletion {
                health_sequence: None,
                fault: Some(fault),
            })
        })
    }
    fn report_instance_health(&self, health: &HealthInformation) -> Result<()> {
        self.health(health, crate::reporting::HealthTarget::Instance)
    }
    fn report_partition_health(&self, health: &HealthInformation) -> Result<()> {
        self.health(health, crate::reporting::HealthTarget::Partition)
    }
    fn report_partition_health2(
        &self,
        health: &HealthInformation,
        _: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        self.report_partition_health(health)
    }
    fn report_instance_health2(
        &self,
        health: &HealthInformation,
        _: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        self.report_instance_health(health)
    }
}
impl ObservedPartition {
    fn health(
        &self,
        health: &HealthInformation,
        target: crate::reporting::HealthTarget,
    ) -> Result<()> {
        self.observation.complete(|| {
            let receipt = self.partition.health(health, target)?;
            Ok(ReportCompletion {
                health_sequence: Some(receipt.sequence),
                fault: None,
            })
        })
    }
}

pub(crate) fn observe_stateful(
    partition: crate::stateful_api::StatefulPartition,
) -> (Arc<dyn IStatefulServicePartition>, ReportingObservation) {
    let observation = ReportingObservation::new();
    (
        Arc::new(ObservedStatefulPartition {
            partition,
            observation: observation.clone(),
        }),
        observation,
    )
}
struct ObservedStatefulPartition {
    partition: crate::stateful_api::StatefulPartition,
    observation: ReportingObservation,
}
impl ObservedStatefulPartition {
    fn health(
        &self,
        health: &HealthInformation,
        target: crate::reporting::HealthTarget,
    ) -> Result<()> {
        self.observation.complete(|| {
            let receipt = self.partition.health(health, target)?;
            Ok(ReportCompletion {
                health_sequence: Some(receipt.sequence),
                fault: None,
            })
        })
    }
}
impl IStatefulServicePartition for ObservedStatefulPartition {
    fn get_partition_information(&self) -> Result<ServicePartitionInformation> {
        self.partition.get_partition_information()
    }
    fn get_read_status(&self) -> Result<ServicePartitionAccessStatus> {
        self.partition.get_read_status()
    }
    fn get_write_status(&self) -> Result<ServicePartitionAccessStatus> {
        self.partition.get_write_status()
    }
    fn report_load(&self, metrics: &[LoadMetric]) -> Result<()> {
        self.observation.complete(|| {
            self.partition.report_load(metrics)?;
            Ok(ReportCompletion {
                health_sequence: None,
                fault: None,
            })
        })
    }
    fn report_move_cost(&self, cost: MoveCost) -> Result<()> {
        self.observation.complete(|| {
            self.partition.report_move_cost(cost)?;
            Ok(ReportCompletion {
                health_sequence: None,
                fault: None,
            })
        })
    }
    fn report_fault(&self, fault: FaultType) -> Result<()> {
        self.observation.complete(|| {
            self.partition.ensure_valid()?;
            let reporting = self
                .partition
                .reporting
                .as_ref()
                .ok_or(Error::Closed)?
                .clone();
            let worker = reporting.clone();
            let fault = reporting.complete(async move { worker.fault(fault).await })?;
            Ok(ReportCompletion {
                health_sequence: None,
                fault: Some(fault),
            })
        })
    }
    fn report_partition_health(&self, health: &HealthInformation) -> Result<()> {
        self.health(health, crate::reporting::HealthTarget::Partition)
    }
    fn report_replica_health(&self, health: &HealthInformation) -> Result<()> {
        self.health(health, crate::reporting::HealthTarget::Replica)
    }
    fn report_partition_health2(
        &self,
        health: &HealthInformation,
        _: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        self.report_partition_health(health)
    }
    fn report_replica_health2(
        &self,
        health: &HealthInformation,
        _: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        self.report_replica_health(health)
    }
}
