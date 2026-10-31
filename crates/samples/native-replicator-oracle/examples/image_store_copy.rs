//! Test-only native SDK reference for application-package replacement.
use mssf_com::FabricClient::IFabricApplicationManagementClient3;
use mssf_core::{client::FabricClientBuilder, WString};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments.len() != 3 {
        return Err("usage: image_store_copy NATIVE_ENDPOINT LOCAL_PACKAGE IMAGESTORE_PATH".into());
    }
    let client = FabricClientBuilder::new()
        .with_connection_strings(vec![WString::from(arguments[0].as_str())])
        .build_interface::<IFabricApplicationManagementClient3>()?;
    let scheme = WString::from("fabric:ImageStore");
    let source = WString::from(arguments[1].as_str());
    let destination = WString::from(arguments[2].as_str());
    unsafe {
        client
            .CopyApplicationPackage(
                scheme.as_pcwstr(),
                source.as_pcwstr(),
                destination.as_pcwstr(),
            )
            .ok()?;
    }
    println!("Native SDK copied application package to {}.", arguments[2]);
    Ok(())
}
