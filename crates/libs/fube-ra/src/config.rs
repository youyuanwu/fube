//! Native dynamic properties, separate from finite executor resource budgets.
//! GetThreshold reads four independently mutable properties in native order.
use super::*;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};

#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub enum RetryState {
    Opening,
    Registration,
    Closing,
    Deleting,
    RegistrationAtDrop,
}
#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub enum Threshold {
    Warning,
    Drop,
    Restart,
    Error,
}
#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub enum NotificationProperty {
    BatchCount,
    NodeIntervalTicks,
    TypeIntervalTicks,
    RetryIntervalTicks,
}

pub struct NativeConfig {
    thresholds: [[AtomicI32; 4]; 5],
    notification: [AtomicI64; 4],
}
impl NativeConfig {
    pub(super) fn new(settings: &Settings) -> Self {
        let close = RetryPolicy {
            warning: settings.close_warning as i32,
            drop: settings.close_drop as i32,
            ..RetryPolicy::registration()
        };
        let delete = RetryPolicy {
            warning: settings.close_warning as i32,
            drop: settings.delete_drop as i32,
            ..RetryPolicy::registration()
        };
        let drop = RetryPolicy {
            warning: settings.registration_warning as i32,
            drop: settings.registration_drop as i32,
            ..RetryPolicy::registration()
        };
        let policies = [
            settings.open_retry,
            settings.open_registration,
            close,
            delete,
            drop,
        ];
        Self {
            thresholds: std::array::from_fn(|s| {
                let p = policies[s];
                [p.warning, p.drop, p.restart, p.error].map(AtomicI32::new)
            }),
            notification: [
                settings.notification_batch_count as i64,
                (settings.notification_node_ms as i64).saturating_mul(10_000),
                (settings.notification_type_ms as i64).saturating_mul(10_000),
                (settings.notification_retry_ms as i64).saturating_mul(10_000),
            ]
            .map(AtomicI64::new),
        }
    }
    fn slot(&self, state: RetryState, field: Threshold) -> &AtomicI32 {
        let state = if matches!(state, RetryState::RegistrationAtDrop)
            && matches!(field, Threshold::Restart | Threshold::Error)
        {
            RetryState::Registration
        } else {
            state
        };
        &self.thresholds[state as usize][field as usize]
    }
    pub fn set(&self, state: RetryState, field: Threshold, value: i32) {
        self.slot(state, field).store(value, Ordering::SeqCst);
    }
    pub fn get(&self, state: RetryState, field: Threshold) -> i32 {
        self.slot(state, field).load(Ordering::SeqCst)
    }
    pub fn policy(&self, state: RetryState) -> RetryPolicy {
        RetryPolicy::read(|field| self.get(state, field))
    }
    pub fn set_notification(&self, property: NotificationProperty, value: i64) {
        let value = if matches!(property, NotificationProperty::BatchCount) {
            i64::from(value as i32)
        } else {
            value
        };
        self.notification[property as usize].store(value, Ordering::SeqCst);
    }
    pub fn notification(&self, property: NotificationProperty) -> i64 {
        self.notification[property as usize].load(Ordering::SeqCst)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn enabled_native_retry_families_are_separate_with_shared_registration_restart_error() {
        let cfg = NativeConfig::new(&Settings::default());
        assert_eq!(cfg.policy(RetryState::Closing).drop, 40320);
        assert_eq!(cfg.policy(RetryState::Deleting).drop, 40);
        assert_eq!(cfg.policy(RetryState::RegistrationAtDrop).warning, 240);
        cfg.set(RetryState::Closing, Threshold::Warning, -1);
        cfg.set(RetryState::Deleting, Threshold::Warning, 0);
        assert!(cfg.policy(RetryState::Closing).clear(0));
        assert!(cfg.policy(RetryState::Deleting).clear(0));
        assert_eq!(cfg.policy(RetryState::Registration).warning, i32::MAX);
        cfg.set(RetryState::RegistrationAtDrop, Threshold::Error, 0);
        assert_eq!(cfg.policy(RetryState::Registration).error, 0);
        let mut count = 0;
        assert!(matches!(
            cfg.policy(RetryState::Deleting)
                .failure(&mut count)
                .unwrap(),
            opening::Action::None
        ));
    }
    #[test]
    fn native_policy_reads_are_independent_properties_not_a_transaction() {
        let cfg = NativeConfig::new(&Settings::default());
        cfg.set(RetryState::Opening, Threshold::Warning, 10);
        let mixed = RetryPolicy::read(|field| {
            let value = cfg.get(RetryState::Opening, field);
            if matches!(field, Threshold::Warning) {
                cfg.set(RetryState::Opening, Threshold::Warning, 2);
                cfg.set(RetryState::Opening, Threshold::Drop, 2);
            }
            value
        });
        assert_eq!((mixed.warning, mixed.drop), (10, 2));
        let mut count = 1;
        assert!(matches!(
            mixed.failure(&mut count).unwrap(),
            opening::Action::Drop
        ));
        let mut count = 1;
        assert!(matches!(
            cfg.policy(RetryState::Opening).failure(&mut count).unwrap(),
            opening::Action::Warning
        ));
    }
}
