//! Independent native resolution and application RPC oracle, not runtime state.
use mssf_client::{
    DeployedCodePackageQueryDescription, DeployedCodePackageQueryParameters, FabricClient,
    NamingUri, PartitionKey,
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(2..=4).contains(&args.len()) {
        return Err(
            "usage: runtime_observer GATEWAY_IP:PORT fabric:/APP/SERVICE [NAMED_PARTITION|int:I64|-] [REPORT_QUERY]"
                .into(),
        );
    }
    let client = FabricClient::connect_clear(args[0].parse()?, Duration::from_secs(10)).await?;
    let name: NamingUri = args[1].parse()?;
    let key = match args.get(2) {
        None => PartitionKey::Singleton,
        Some(value) if value == "-" => PartitionKey::Singleton,
        Some(value) if value.starts_with("int:") => PartitionKey::Int64(value[4..].parse()?),
        Some(value) => PartitionKey::Named(value.clone()),
    };
    let query = args.get(3).map_or("", String::as_str);
    if query.len() > 1024
        || query
            .bytes()
            .any(|b| !b.is_ascii_alphanumeric() && !b"-_=&".contains(&b))
    {
        return Err("unsupported bounded sample report query".into());
    }
    let resolved = client
        .resolve_service_partition(&name, &key, None, Some(Duration::from_secs(10)))
        .await?;
    if resolved.endpoints.is_empty() {
        return Err("no resolved endpoints".into());
    }
    for endpoint in resolved.endpoints {
        let (authority, path) = endpoint
            .address
            .strip_prefix("http://")
            .and_then(|v| v.split_once('/'))
            .ok_or("sample endpoint is not HTTP")?;
        let mut stream =
            tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(authority)).await??;
        stream
            .write_all(
                format!(
                    "GET /{path}{}{} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n",
                    if query.is_empty() { "" } else { "?" },
                    query
                )
                .as_bytes(),
            )
            .await?;
        let mut response = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(35),
            (&mut stream).take(16_385).read_to_end(&mut response),
        )
        .await??;
        if response.len() > 16_384 {
            return Err("RPC response limit".into());
        }
        let response = String::from_utf8(response)?;
        if !response.starts_with("HTTP/1.1 200 ") {
            return Err(format!("RPC did not succeed: {response}").into());
        }
        let (_, body) = response.split_once("\r\n\r\n").ok_or("missing RPC body")?;
        let rpc: serde_json::Value = serde_json::from_str(body)?;
        let application = args[1]
            .rsplit_once('/')
            .ok_or("service must belong to app")?
            .0;
        let packages = client
            .get_deployed_code_package_list(
                &DeployedCodePackageQueryDescription::new(DeployedCodePackageQueryParameters::new(
                    rpc["node"].as_str().ok_or("missing node")?,
                    application.parse()?,
                )),
                None,
            )
            .await?;
        let package = packages
            .iter()
            .find(|p| {
                p.service_manifest_name == "RustRuntimeService"
                    && p.name == "Code"
                    && p.main_entry_point.process_id == rpc["pid"].as_i64().unwrap_or(-1)
            })
            .ok_or("native code package does not confirm serving PID")?;
        println!(
            "{}",
            serde_json::json!({
                "endpoint":endpoint.address,"rpc":rpc,"native_pid":package.main_entry_point.process_id,
                "native_entry_point_instance":package.main_entry_point.instance_id,
                "native_activation_count":package.main_entry_point.statistics.activation_count,
                "native_entry_point":package.main_entry_point.entry_point_location,
                "native_code_version":package.version,
                "resolver":"mssf-client",
            })
        );
    }
    client.close().await?;
    Ok(())
}
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1)
    }
}
