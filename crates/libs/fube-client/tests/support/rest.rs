//! Test-only REST configuration and generated node observations.
use std::{error::Error, time::Duration};

use openapi_service_fabric::apis::{configuration::Configuration, node_api};
use reqwest::{redirect::Policy, Client};
use serde_json::Value;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
pub const MANAGEMENT: &str = "http://127.0.0.1:19080";

pub fn configuration(timeout: Duration) -> Result<Configuration> {
    Ok(Configuration {
        base_path: MANAGEMENT.into(),
        client: Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .timeout(timeout)
            .build()?,
        ..Configuration::default()
    })
}

pub async fn nodes(config: &Configuration, token: Option<&str>) -> Result<Value> {
    let page = node_api::get_node_info_list(config, "6.3", token, None, None, None).await?;
    // Keep the fixtures' required-field checks and evidence format, rather than
    // defaulting missing fields in the generated optional response models.
    Ok(serde_json::to_value(page)?)
}
