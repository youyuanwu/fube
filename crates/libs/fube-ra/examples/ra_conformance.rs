//! Compiler-free consumer of independently native-produced fixtures.
mod support;

use fube_ra::protocol::*;
use fube_transport::serialization::serde::{from_slice, to_vec};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{fmt::Debug, fs, path::Path};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Scalar<T> {
    value: T,
}

fn check<T: Serialize + DeserializeOwned + Debug + PartialEq>(
    input: &Path,
    output: &Path,
    name: &str,
    expected: T,
) {
    let bytes = fs::read(input.join(format!("{name}.bin"))).expect("prebuilt native fixture");
    let decoded: T = from_slice(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
    assert_eq!(decoded, expected, "native -> Rust fields: {name}");
    // Rust produces independently constructed values, not a byte echo.
    fs::write(
        output.join(format!("{name}.bin")),
        to_vec(&expected).unwrap(),
    )
    .unwrap();
    println!("{name} native->Rust OK; Rust output produced");
}

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() == 2 && args[1] == "--identity" {
        println!(
            "{}",
            option_env!("FUBE_RA_SOURCE_DIGEST").unwrap_or("unbound")
        );
        return;
    }
    assert_eq!(
        args.len(),
        3,
        "usage: ra_conformance NATIVE_INPUT RUST_OUTPUT"
    );
    let input = Path::new(&args[1]);
    let output = Path::new(&args[2]);
    assert!(input.is_dir() && output.is_dir());
    check(input, output, "default-request", AddInstance::default());
    check(input, output, "rich-request", support::rich_request());
    check(input, output, "default-reply", AddInstanceReply::default());
    for error in [0, -2_147_017_398, -2_147_017_441] {
        check(
            input,
            output,
            &format!("rich-reply-{error}"),
            support::rich_reply(error),
        );
    }
    check(
        input,
        output,
        "default-generation",
        GenerationHeader::default(),
    );
    check(
        input,
        output,
        "fm-generation",
        support::rich_generation(false),
    );
    check(
        input,
        output,
        "fmm-generation",
        support::rich_generation(true),
    );
    for value in [10_i64, 11, 12] {
        check(input, output, &format!("actor-{value}"), Scalar { value });
    }
    check(
        input,
        output,
        "action-request",
        Scalar {
            value: "AddInstance".to_string(),
        },
    );
    check(
        input,
        output,
        "action-reply",
        Scalar {
            value: "AddInstanceReply".to_string(),
        },
    );
    check(input, output, "priority", Scalar { value: true });
    for name in ["rich-request", "rich-reply-0"] {
        let bytes = fs::read(input.join(format!("{name}.bin"))).unwrap();
        for end in 0..bytes.len() {
            if name == "rich-request" {
                assert!(from_slice::<AddInstance>(&bytes[..end]).is_err());
            } else {
                assert!(from_slice::<AddInstanceReply>(&bytes[..end]).is_err());
            }
        }
    }
    println!("15 cases verified; all selected-body proper prefixes rejected");
}
