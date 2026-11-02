/// A custom metric declared in the current native service description.
#[derive(Debug, Clone)]
pub struct LoadMetric {
    pub name: String,
    /// Native application reporting requires a value strictly less than INT_MAX.
    pub value: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Pinned native costs 0–3. Newer mssf VeryHigh is intentionally unavailable.
pub enum MoveCost {
    Zero,
    Low,
    Medium,
    High,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    Invalid,
    Ok,
    Warning,
    Error,
}
/// UTF-8 health values. TTL is in seconds; zero is infinite. Sequence zero is
/// process-auto; negative values are invalid. This profile directly sends both
/// Immediate values and awaits an exact HM result, not SDK batching admission.
/// Only Ok/Warning/Error are accepted. Source/property are nonempty bounded
/// UTF-16-compatible text without NUL; System* and DeleteProperty are reserved.
/// Descriptions above 4096 UTF-16 units are rejected, not truncated.
#[derive(Debug, Clone)]
pub struct HealthInformation {
    pub source_id: String,
    pub property: String,
    pub time_to_live_seconds: u32,
    pub state: HealthState,
    pub description: String,
    pub sequence_number: i64,
    pub remove_when_expired: bool,
}
#[derive(Debug, Clone)]
/// Both values directly send, with no intentional reporting-interval delay.
/// This is not SDK default batching/retry parity or guaranteed HM delivery.
pub struct HealthReportSendOption {
    pub immediate: bool,
}
pub const MAX_REPORT_METRICS: usize = 64;
pub const MAX_REPORT_NAME_UNITS: usize = 4096;
