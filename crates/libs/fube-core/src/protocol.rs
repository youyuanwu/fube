//! Shared Service Fabric wire identifiers, not transport or subsystem policy.

macro_rules! native_scalar {
    ($name:ident, $code:ident, $from:ident) => {
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_i64(self.$code())
            }
        }
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                <i64 as serde::Deserialize>::deserialize(deserializer).map(Self::$from)
            }
        }
    };
}

mod replica;
pub use replica::{NativeReplicaRole, NativeReplicaState};
mod rap;
pub use rap::{
    NativeApi, NativeApiDescription, NativeApiName, NativeInterfaceName, NativeProxyFlags,
    NativeReplicatorOperation, NativeServiceOperation,
};

pub mod actor {
    //! Selected native `Transport::Actor` codes from
    //! [the pinned enumeration](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Actor.h).
    //! Naming a subsystem does not authorize its traffic or qualify its protocol.

    pub const EMPTY: i64 = 0;
    pub const APPLICATION_HOST_MANAGER: i64 = 7;
    pub const APPLICATION_HOST: i64 = 8;
    pub const FABRIC_RUNTIME_MANAGER: i64 = 9;
    /// Reconfiguration Agent; also used by the app-host RAP lifecycle protocol.
    pub const RA: i64 = 12;
    pub const HOSTING: i64 = 15;
    /// Health Manager.
    pub const HM: i64 = 16;
    pub const FILE_SENDER: i64 = 25;
    pub const FILE_TRANSFER_CLIENT: i64 = 27;
}
