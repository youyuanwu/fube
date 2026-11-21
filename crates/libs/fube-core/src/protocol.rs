//! Shared Service Fabric wire identifiers, not transport or subsystem policy.

pub mod actor {
    //! Selected native `Transport::Actor` codes from
    //! [the pinned enumeration](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Actor.h).
    //! Naming a subsystem does not authorize its traffic or qualify its protocol.

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
