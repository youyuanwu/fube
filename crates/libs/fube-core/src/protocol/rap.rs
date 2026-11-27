//! RAP and query scalar codes from ProxyMessageFlags, ApiMonitoring and
//! FabricTypes.idl at native revision 3988b451. All use signed LONG64 encoding.
use serde::{Deserialize, Serialize};

bitflags::bitflags! {
    /// Unknown wire bits are retained. Admission must use difference(allowed),
    /// not a bitflags complement, which truncates unknown bits.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
    pub struct NativeProxyFlags: i64 {
        const NONE = 0;
        const END_RECONFIGURATION = 0x01;
        const CATCH_UP = 0x02;
        const ABORT = 0x04;
        const DROP = 0x08;
    }

    /// Native ReplicatorOperationManager ORs concurrently active query operations.
    /// NONE is the idle marker, not an active operation or the empty mask.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
    pub struct NativeReplicatorOperation: i64 {
        const INVALID = 0;
        const NONE = 0x001;
        const OPEN = 0x002;
        const CHANGE_ROLE = 0x004;
        const UPDATE_EPOCH = 0x008;
        const CLOSE = 0x010;
        const ABORT = 0x020;
        const ON_DATA_LOSS = 0x040;
        const WAIT_FOR_CATCH_UP = 0x080;
        const BUILD = 0x100;
    }
}
native_scalar!(NativeProxyFlags, bits, from_bits_retain);
native_scalar!(NativeReplicatorOperation, bits, from_bits_retain);

native_enum!(NativeServiceOperation {
    Invalid = 0,
    None = 0x01,
    Open = 0x02,
    ChangeRole = 0x04,
    Close = 0x08,
    Abort = 0x10,
});
native_enum!(NativeApiName {
    Invalid = 0,
    Open = 1,
    Close = 2,
    Abort = 3,
    GetNextCopyState = 4,
    GetNextCopyContext = 5,
    UpdateEpoch = 6,
    OnDataLoss = 7,
    ChangeRole = 8,
    CatchupReplicaSet = 9,
    BuildReplica = 10,
    RemoveReplica = 11,
    GetStatus = 12,
    UpdateCatchupConfiguration = 13,
    UpdateCurrentConfiguration = 14,
    GetQuery = 15,
});
native_enum!(NativeInterfaceName {
    Invalid = 0,
    StatefulServiceReplica = 1,
    StatelessServiceInstance = 2,
    StateProvider = 3,
    Replicator = 4,
});

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeApi {
    pub api: NativeApiName,
    pub interface: NativeInterfaceName,
}
impl NativeApi {
    pub const fn new(interface: NativeInterfaceName, api: NativeApiName) -> Self {
        Self { api, interface }
    }
}

/// Native ApiNameDescription field order; metadata remains opaque text.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeApiDescription {
    pub api: NativeApiName,
    pub interface: NativeInterfaceName,
    pub metadata: String,
}
impl From<NativeApi> for NativeApiDescription {
    fn from(value: NativeApi) -> Self {
        Self {
            api: value.api,
            interface: value.interface,
            metadata: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::check_scalar;

    #[test]
    fn rap_scalars_preserve_signed_long64_and_numeric_json_including_unknowns() {
        for code in [
            i64::MIN,
            -1,
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            31,
            128,
            256,
            i64::MAX,
        ] {
            check_scalar(NativeProxyFlags::from_bits_retain(code), code);
            check_scalar(NativeReplicatorOperation::from_bits_retain(code), code);
            check_scalar(NativeServiceOperation::from(code), code);
            check_scalar(NativeApiName::from(code), code);
            check_scalar(NativeInterfaceName::from(code), code);
        }
    }

    #[test]
    fn api_description_keeps_native_order_and_unknown_codes_and_metadata() {
        #[derive(Serialize, Deserialize)]
        struct Raw {
            api: i64,
            interface: i64,
            metadata: String,
        }
        let raw = Raw {
            api: -99,
            interface: i64::MAX,
            metadata: "opaque metadata".into(),
        };
        let bytes = fubst::serde::to_vec(&raw).unwrap();
        let typed: NativeApiDescription = fubst::serde::from_slice(&bytes).unwrap();
        assert_eq!(typed.api, NativeApiName::Unrecognized(-99));
        assert_eq!(typed.interface, NativeInterfaceName::Unrecognized(i64::MAX));
        assert_eq!(typed.metadata, raw.metadata);
        assert_eq!(fubst::serde::to_vec(&typed).unwrap(), bytes);
        assert_eq!(
            serde_json::to_value(&typed).unwrap(),
            serde_json::to_value(&raw).unwrap()
        );
        let changed = NativeApi::new(NativeInterfaceName::Replicator, NativeApiName::ChangeRole);
        let description = NativeApiDescription::from(changed);
        assert_eq!(
            fubst::serde::to_vec(&description).unwrap(),
            fubst::serde::to_vec(&Raw {
                api: 8,
                interface: 4,
                metadata: String::new()
            })
            .unwrap()
        );
    }
}
