//! Reliability scalar codes, distinct from public SDK role numbers.
//! Native scalar enums serialize as LONG64; unknown codes remain wire data.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum NativeReplicaRole {
    #[default]
    Unknown,
    None,
    Idle,
    Secondary,
    Primary,
    Unrecognized(i64),
}

impl NativeReplicaRole {
    pub const fn code(self) -> i64 {
        match self {
            Self::Unknown => 0,
            Self::None => 1,
            Self::Idle => 2,
            Self::Secondary => 3,
            Self::Primary => 4,
            Self::Unrecognized(code) => code,
        }
    }

    pub const fn is_known(self) -> bool {
        !matches!(self, Self::Unrecognized(_))
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Secondary | Self::Primary)
    }
}

impl From<i64> for NativeReplicaRole {
    fn from(code: i64) -> Self {
        match code {
            0 => Self::Unknown,
            1 => Self::None,
            2 => Self::Idle,
            3 => Self::Secondary,
            4 => Self::Primary,
            code => Self::Unrecognized(code),
        }
    }
}

impl From<crate::ReplicaRole> for NativeReplicaRole {
    fn from(role: crate::ReplicaRole) -> Self {
        match role {
            crate::ReplicaRole::Unknown => Self::Unknown,
            crate::ReplicaRole::None => Self::None,
            crate::ReplicaRole::IdleSecondary => Self::Idle,
            crate::ReplicaRole::ActiveSecondary => Self::Secondary,
            crate::ReplicaRole::Primary => Self::Primary,
        }
    }
}

impl TryFrom<NativeReplicaRole> for crate::ReplicaRole {
    type Error = crate::Error;

    fn try_from(role: NativeReplicaRole) -> crate::Result<Self> {
        match role {
            NativeReplicaRole::Unknown => Ok(Self::Unknown),
            NativeReplicaRole::None => Ok(Self::None),
            NativeReplicaRole::Idle => Ok(Self::IdleSecondary),
            NativeReplicaRole::Secondary => Ok(Self::ActiveSecondary),
            NativeReplicaRole::Primary => Ok(Self::Primary),
            NativeReplicaRole::Unrecognized(_) => {
                Err(crate::Error::Protocol("invalid native replica role"))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum NativeReplicaState {
    #[default]
    StandBy,
    InBuild,
    Ready,
    Dropped,
    Unrecognized(i64),
}

impl NativeReplicaState {
    pub const fn code(self) -> i64 {
        match self {
            Self::StandBy => 0,
            Self::InBuild => 1,
            Self::Ready => 2,
            Self::Dropped => 3,
            Self::Unrecognized(code) => code,
        }
    }

    pub const fn is_known(self) -> bool {
        !matches!(self, Self::Unrecognized(_))
    }
}

impl From<i64> for NativeReplicaState {
    fn from(code: i64) -> Self {
        match code {
            0 => Self::StandBy,
            1 => Self::InBuild,
            2 => Self::Ready,
            3 => Self::Dropped,
            code => Self::Unrecognized(code),
        }
    }
}

native_scalar!(NativeReplicaRole, code, from);
native_scalar!(NativeReplicaState, code, from);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_scalars_preserve_signed_wire_and_json_codes_including_unknowns() {
        for code in [i64::MIN, -1, 0, 1, 2, 3, 4, 5, i64::MAX] {
            let role = NativeReplicaRole::from(code);
            let state = NativeReplicaState::from(code);
            let expected = fubst::serde::to_vec(&code).unwrap();
            assert_eq!(fubst::serde::to_vec(&role).unwrap(), expected);
            assert_eq!(fubst::serde::to_vec(&state).unwrap(), expected);
            assert_eq!(
                fubst::serde::from_slice::<NativeReplicaRole>(&expected).unwrap(),
                role
            );
            assert_eq!(
                fubst::serde::from_slice::<NativeReplicaState>(&expected).unwrap(),
                state
            );
            assert_eq!(serde_json::to_value(role).unwrap(), serde_json::json!(code));
            assert_eq!(
                serde_json::from_value::<NativeReplicaRole>(serde_json::json!(code)).unwrap(),
                role
            );
            assert_eq!(
                serde_json::from_value::<NativeReplicaState>(serde_json::json!(code)).unwrap(),
                state
            );
        }
    }

    #[test]
    fn sdk_role_conversion_is_explicit_and_rejects_unrecognized_native_roles() {
        for sdk in [
            crate::ReplicaRole::Unknown,
            crate::ReplicaRole::None,
            crate::ReplicaRole::Primary,
            crate::ReplicaRole::IdleSecondary,
            crate::ReplicaRole::ActiveSecondary,
        ] {
            let native = NativeReplicaRole::from(sdk);
            assert_eq!(crate::ReplicaRole::try_from(native).unwrap(), sdk);
        }
        assert_ne!(
            NativeReplicaRole::from(crate::ReplicaRole::Primary).code(),
            crate::ReplicaRole::Primary as i32 as i64
        );
        for code in [-1, 5, i64::MAX] {
            let role = NativeReplicaRole::from(code);
            assert!(!role.is_known() && !role.is_active());
            assert!(crate::ReplicaRole::try_from(role).is_err());
            assert!(!NativeReplicaState::from(code).is_known());
        }
    }
}
