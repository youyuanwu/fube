//! Independent management observations shared by setup and native assertions.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ObservedNode {
    pub name: String,
    #[serde(rename = "IpAddressOrFQDN")]
    pub ip_address_or_fqdn: String,
    #[serde(rename = "Type")]
    pub node_type: String,
    pub code_version: String,
    pub config_version: String,
    pub node_status: String,
    pub node_up_time_in_seconds: String,
    pub health_state: String,
    pub is_seed_node: bool,
    pub upgrade_domain: String,
    pub fault_domain: String,
    pub id: ObservedNodeId,
    pub instance_id: String,
    pub node_deactivation_info: ObservedDeactivation,
    pub is_stopped: bool,
    pub node_down_time_in_seconds: String,
    pub node_up_at: String,
    pub node_down_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ObservedNodeId {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ObservedDeactivation {
    pub node_deactivation_intent: String,
    pub node_deactivation_status: String,
    pub node_deactivation_task: Vec<serde_json::Value>,
    pub pending_safety_checks: Vec<serde_json::Value>,
}

pub fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after Unix epoch")
        .as_millis()
        .try_into()
        .expect("clock fits u64 milliseconds")
}
