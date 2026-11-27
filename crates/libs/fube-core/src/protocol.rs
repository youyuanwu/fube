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

macro_rules! native_enum {
    ($name:ident { $default:ident = $zero:literal, $($variant:ident = $code:literal),* $(,)? }) => {
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
        pub enum $name {
            #[default]
            $default,
            $($variant,)*
            Unrecognized(i64),
        }
        impl $name {
            pub const fn code(self) -> i64 {
                match self {
                    Self::$default => $zero,
                    $(Self::$variant => $code,)*
                    Self::Unrecognized(code) => code,
                }
            }
            pub const fn is_known(self) -> bool {
                !matches!(self, Self::Unrecognized(_))
            }
        }
        impl From<i64> for $name {
            fn from(code: i64) -> Self {
                match code {
                    $zero => Self::$default,
                    $($code => Self::$variant,)*
                    code => Self::Unrecognized(code),
                }
            }
        }
        native_scalar!($name, code, from);
    };
}

mod fault;
pub use fault::NativeFaultType;
mod health;
pub use health::{NativeHealthReportKind, NativeHealthState};
mod hosting;
pub use hosting::{
    NativeActivityType, NativeApplicationHostType, NativeServicePackageActivationMode,
};
mod partition;
pub use partition::NativePartitionKind;
mod replica;
pub use replica::{NativeReplicaRole, NativeReplicaState};
mod service;
pub use service::{
    NativeMoveCost, NativePlacementPolicyType, NativeServiceCorrelationScheme, NativeServiceKind,
    NativeServiceLoadMetricWeight, NativeServicePartitionAccessStatus,
};
mod rap;
pub use rap::{
    NativeApi, NativeApiDescription, NativeApiName, NativeInterfaceName, NativeProxyFlags,
    NativeReplicatorOperation, NativeServiceOperation,
};
mod uri;
pub use uri::{NativeUriHostType, NativeUriType};

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

#[cfg(test)]
fn check_scalar<T>(value: T, code: i64)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = fubst::serde::to_vec(&code).unwrap();
    assert_eq!(fubst::serde::to_vec(&value).unwrap(), bytes);
    assert_eq!(fubst::serde::from_slice::<T>(&bytes).unwrap(), value);
    assert_eq!(
        serde_json::to_value(&value).unwrap(),
        serde_json::json!(code)
    );
    assert_eq!(
        serde_json::from_value::<T>(serde_json::json!(code)).unwrap(),
        value
    );
}
