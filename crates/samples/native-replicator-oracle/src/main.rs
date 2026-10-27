// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

use native_replicator_oracle::{
    data::{operation_data, read_segments},
    provider::{get_copy_context, snapshot_provider},
    stream::{end_get_next, operation_data_stream, StreamItem},
};
use serde_json::json;

fn self_test() -> windows_core::Result<()> {
    let expected = vec![vec![1, 2, 3], Vec::new(), vec![4, 5]];
    let data = operation_data(expected.clone())?;
    let provider = snapshot_provider(0, vec![data.clone()])?;
    if get_copy_context(&provider)?.is_some() {
        return Err(windows_core::HRESULT(0x8000_ffffu32 as i32).into());
    }
    let stream = operation_data_stream(vec![StreamItem::Data(data)])?;
    let context = unsafe { stream.BeginGetNext(None)? };
    let data =
        end_get_next(&stream, &context)?.ok_or(windows_core::HRESULT(0x8000_ffffu32 as i32))?;
    if unsafe { read_segments(&data)? } != expected {
        return Err(windows_core::HRESULT(0x8000_ffffu32 as i32).into());
    }
    let context = unsafe { stream.BeginGetNext(None)? };
    if end_get_next(&stream, &context)?.is_some() {
        return Err(windows_core::HRESULT(0x8000_ffffu32 as i32).into());
    }
    println!(
        "{}",
        json!({
            "profile": "native-replicator-oracle-phase1-abi",
            "abi_self_test": "passed",
            "native_engine_created": false,
            "native_interoperability_qualified": false
        })
    );
    Ok(())
}

fn main() -> std::process::ExitCode {
    if std::env::args().skip(1).collect::<Vec<_>>() != ["--abi-self-test"] {
        eprintln!(
            "usage: native-replicator-oracle --abi-self-test\n\
             Phase 1 provides local ABI checks only; native service registration \
             and replication are not implemented."
        );
        return std::process::ExitCode::FAILURE;
    }
    match self_test() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ABI self-test failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
