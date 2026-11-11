//! Test-only native SDK reference for application-package replacement.
use clap::Parser;
use mssf_com::FabricClient::IFabricApplicationManagementClient3;
use mssf_core::{client::FabricClientBuilder, WString};

#[derive(Parser)]
#[command(about = "Copy an application package through the native SDK")]
struct Arguments {
    native_endpoint: String,
    local_package: String,
    imagestore_path: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse();
    let client = FabricClientBuilder::new()
        .with_connection_strings(vec![WString::from(arguments.native_endpoint.as_str())])
        .build_interface::<IFabricApplicationManagementClient3>()?;
    let scheme = WString::from("fabric:ImageStore");
    let source = WString::from(arguments.local_package.as_str());
    let destination = WString::from(arguments.imagestore_path.as_str());
    unsafe {
        client
            .CopyApplicationPackage(
                scheme.as_pcwstr(),
                source.as_pcwstr(),
                destination.as_pcwstr(),
            )
            .ok()?;
    }
    println!(
        "Native SDK copied application package to {}.",
        arguments.imagestore_path
    );
    Ok(())
}
