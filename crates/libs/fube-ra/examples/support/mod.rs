#![allow(dead_code)]
use fube_ra::protocol::*;
use fube_transport::serialization::serde::{
    ByteArray, Derived, Guid, NativeEnum, ObjectArray, Pointer,
};

pub trait Rich {
    fn rich(&mut self, counter: &mut i32);
}
fn next(counter: &mut i32) -> i32 {
    *counter += 1;
    *counter
}
impl Rich for i64 {
    fn rich(&mut self, c: &mut i32) {
        *self = -0x1234_5678_0000 - i64::from(next(c));
    }
}
impl Rich for u64 {
    fn rich(&mut self, c: &mut i32) {
        *self = 0xfedc_ba98_0000_0000 + next(c) as u64;
    }
}
impl Rich for i32 {
    fn rich(&mut self, c: &mut i32) {
        *self = 1000 + next(c);
    }
}
impl Rich for u32 {
    fn rich(&mut self, c: &mut i32) {
        *self = 4_000_000_000 + next(c) as u32;
    }
}
impl Rich for bool {
    fn rich(&mut self, c: &mut i32) {
        *self = next(c) % 2 == 0;
    }
}
impl Rich for f64 {
    fn rich(&mut self, c: &mut i32) {
        *self = 1.25 + f64::from(next(c));
    }
}
impl Rich for String {
    fn rich(&mut self, c: &mut i32) {
        *self = format!("v{}π🦀", next(c));
    }
}
impl Rich for [u8; 16] {
    fn rich(&mut self, c: &mut i32) {
        let n = next(c);
        for (i, b) in self.iter_mut().enumerate() {
            *b = (n as usize + i) as u8;
        }
    }
}
impl Rich for Guid {
    fn rich(&mut self, c: &mut i32) {
        self.0.rich(c);
    }
}
impl Rich for ByteArray {
    fn rich(&mut self, c: &mut i32) {
        self.0 = (0..2).map(|_| next(c) as u8).collect();
    }
}
impl<T: Rich + Default> Rich for ObjectArray<Vec<T>> {
    fn rich(&mut self, c: &mut i32) {
        self.0 = (0..2)
            .map(|_| {
                let mut value = T::default();
                value.rich(c);
                value
            })
            .collect();
    }
}
macro_rules! field {
    ($v:ident, $c:ident, [$f:ident]) => {
        $v.$f = i64::from(next($c) % 5);
    };
    ($v:ident, $c:ident, $f:ident) => {
        $v.$f.rich($c);
    };
}
macro_rules! rich {
    ($t:ty: $($f:tt),* $(,)?) => {
        impl Rich for $t {
            fn rich(&mut self, c: &mut i32) { $(field!(self, c, $f);)* }
        }
    };
}
rich!(PartitionId: guid);
rich!(ConsistencyUnit: id, low, high, name, [kind]);
rich!(Epoch: configuration, data_loss);
rich!(FailoverUnit: id, consistency, current, previous, target, minimum);
rich!(ApplicationId: type_name, number);
rich!(PackageId: application, name);
rich!(ServiceType: package, name);
rich!(Rollout: major, minor);
rich!(ApplicationVersion: value);
rich!(PackageVersion: application, value);
rich!(VersionInstance: version, instance);
rich!(NodeInstance: id, instance);
rich!(Replica: node, id, instance, [current_role], [previous_role], up, last_lsn,
    first_lsn, [state], version, service_endpoint, replication_endpoint);
rich!(Correlation: service, [scheme]);
rich!(Metric: name, [weight], primary, secondary);
rich!(Placement: domain, [kind]);
rich!(PartitionLoad: metric, lower, upper, interval);
rich!(ServiceLoad: metric, lower, upper, interval, primary_only);
rich!(ScaleCount: maximum, minimum, increment);
rich!(AddInstance: unit, replica, service);
rich!(AddInstanceReply: unit, replica, [error]);
rich!(GenerationNumber: generation, owner);
rich!(GenerationHeader: number, for_fm_replica);
rich!(ReplicaInfo: description, [intermediate_role]);
rich!(FailoverUnitInfo: service, unit, intermediate, report_from_primary, replicas, local_deleted);
rich!(ReplicaUp: replicas, dropped, last, from_fmm);

fn filled<T: Default + Rich>(c: &mut i32) -> T {
    let mut value = T::default();
    value.rich(c);
    value
}
pub fn rich_request() -> AddInstance {
    filled(&mut 0)
}
pub fn rich_remove(force: bool) -> RemoveInstance {
    RemoveInstance::new(rich_request(), force)
}
pub fn rich_replica_up(last: bool, from_fmm: bool) -> ReplicaUp {
    let mut value: ReplicaUp = filled(&mut 0);
    value.last = last;
    value.from_fmm = from_fmm;
    value
}
pub fn rich_reply(error: i64) -> AddInstanceReply {
    let mut value: AddInstanceReply = filled(&mut 0);
    value.error = error;
    value
}
pub fn rich_generation(for_fm_replica: bool) -> GenerationHeader {
    let mut value: GenerationHeader = filled(&mut 0);
    value.for_fm_replica = for_fm_replica;
    value
}
impl Rich for Service {
    fn rich(&mut self, c: &mut i32) {
        self.name.rich(c);
        self.instance.rich(c);
        self.service_type.rich(c);
        self.application.rich(c);
        self.version.rich(c);
        self.partitions.rich(c);
        self.target.rich(c);
        self.minimum.rich(c);
        self.stateful.rich(c);
        self.persisted.rich(c);
        self.initialization.rich(c);
        self.group.rich(c);
        self.correlations.rich(c);
        self.constraints.rich(c);
        self.scaleout.rich(c);
        self.metrics.rich(c);
        self.move_cost.rich(c);
        self.restart_wait.rich(c);
        self.quorum_wait.rich(c);
        self.update_version.rich(c);
        self.placement.rich(c);
        self.standby_keep.rich(c);
        self.activation_mode = i64::from(next(c) % 5);
        self.dns.rich(c);
        let null = ScalingPolicy {
            trigger: Pointer(None),
            mechanism: Pointer(None),
        };
        let invalid = ScalingPolicy {
            trigger: Pointer(Some(NativeEnum(ScalingTrigger::Invalid))),
            mechanism: Pointer(Some(NativeEnum(ScalingMechanism::Invalid))),
        };
        let partition = ScalingPolicy {
            trigger: Pointer(Some(NativeEnum(ScalingTrigger::Partition {
                fields: Derived(filled(c)),
            }))),
            mechanism: Pointer(Some(NativeEnum(ScalingMechanism::Instance {
                fields: Derived(filled(c)),
            }))),
        };
        let service = ScalingPolicy {
            trigger: Pointer(Some(NativeEnum(ScalingTrigger::Service {
                fields: Derived(filled(c)),
            }))),
            mechanism: Pointer(Some(NativeEnum(ScalingMechanism::Partition {
                fields: Derived(filled(c)),
            }))),
        };
        self.scaling = ObjectArray(vec![null, invalid, partition, service]);
    }
}
