//! Native furpc observations shared by the existing onebox scenarios.
#![allow(dead_code)] // Individual scenarios use different observation helpers.
use std::time::Duration;

use fube_client::{FurpcClient, FurpcClientError};
use tokio::time::Instant;

use super::rest::Result;

pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

pub async fn connect(gateway: &str) -> Result<(FurpcClient, u64)> {
    let channel = FurpcClient::endpoint_clear(gateway.parse()?)?
        .connect()
        .await?;
    let mut client = FurpcClient::new(channel);
    let response = client.ping(furpc::Request::new(()), deadline()).await?;
    let generation = response.source().expect("native Ping source").generation();
    Ok((client, generation))
}

pub fn body<T>(response: furpc::Response<T>, generation: u64) -> T {
    assert_eq!(
        response.source().expect("native query source").generation(),
        generation,
        "healthy reuse must not hide a reconnect"
    );
    response.into_body()
}

pub fn failure(error: FurpcClientError) -> (i64, &'static str) {
    match error {
        FurpcClientError::Domain(failure) => (failure.code, "domain"),
        FurpcClientError::Native(furpc::Error::RemoteFault(fault)) => {
            (fault.code(), "native_fault")
        }
        other => panic!("expected native domain/fault error, got {other:?}"),
    }
}
