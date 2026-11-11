use super::*;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Configuration {
    pub unit: FailoverUnit,
    pub replicas: ObjectArray<Vec<Replica>>,
    pub service: Service,
}

#[derive(Serialize, Deserialize)]
struct Options<T> {
    value: T,
}
#[derive(Serialize, Deserialize)]
struct Inherited<B, T> {
    base: Inline<B>,
    options: Derived<Options<T>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DoReconfiguration {
    pub configuration: Configuration,
    pub phase0_duration: i64,
}
impl Default for DoReconfiguration {
    fn default() -> Self {
        Self {
            configuration: Configuration::default(),
            phase0_duration: i64::MAX,
        }
    }
}
impl Serialize for DoReconfiguration {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Inherited {
            base: Inline(&self.configuration),
            options: Derived(Options {
                value: self.phase0_duration,
            }),
        }
        .serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for DoReconfiguration {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let body = Inherited::<Configuration, i64>::deserialize(deserializer)?;
        Ok(Self {
            configuration: body.base.0,
            phase0_duration: body.options.0.value,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigurationReply {
    pub unit: FailoverUnit,
    pub replicas: ObjectArray<Vec<Replica>>,
    pub error: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Activate {
    pub configuration: Configuration,
    pub deactivation: ReplicaDeactivationInfo,
}
impl Serialize for Activate {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Inherited {
            base: Inline(&self.configuration),
            options: Derived(Options {
                value: &self.deactivation,
            }),
        }
        .serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for Activate {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let body = Inherited::<Configuration, ReplicaDeactivationInfo>::deserialize(deserializer)?;
        Ok(Self {
            configuration: body.base.0,
            deactivation: body.options.0.value,
        })
    }
}
pub type ActivateReply = AddInstanceReply;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Deactivate {
    pub configuration: Configuration,
    pub force: bool,
    pub deactivation: ReplicaDeactivationInfo,
}
#[derive(Serialize, Deserialize)]
struct DeactivateOptions {
    force: bool,
    deactivation: ReplicaDeactivationInfo,
}
impl Serialize for Deactivate {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Body<'a> {
            base: Inline<&'a Configuration>,
            options: Derived<DeactivateOptions>,
        }
        Body {
            base: Inline(&self.configuration),
            options: Derived(DeactivateOptions {
                force: self.force,
                deactivation: self.deactivation.clone(),
            }),
        }
        .serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for Deactivate {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Body {
            base: Inline<Configuration>,
            options: Derived<DeactivateOptions>,
        }
        let body = Body::deserialize(deserializer)?;
        Ok(Self {
            configuration: body.base.0,
            force: body.options.0.force,
            deactivation: body.options.0.deactivation,
        })
    }
}
pub type DeactivateReply = AddInstanceReply;

pub fn deactivate_message(body: &Deactivate, generation: &GenerationHeader) -> Result<Message> {
    reliability_message(body, "Deactivate", RA_ACTOR, true, Some(generation))
}
pub fn decode_deactivate(message: &Message) -> Result<(Deactivate, GenerationHeader)> {
    check_factory(message, "Deactivate", &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}
pub fn deactivate_reply_message(
    body: &DeactivateReply,
    generation: &GenerationHeader,
) -> Result<Message> {
    reliability_message(body, "DeactivateReply", RA_ACTOR, true, Some(generation))
}
pub fn decode_deactivate_reply(message: &Message) -> Result<(DeactivateReply, GenerationHeader)> {
    check_factory(message, "DeactivateReply", &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}

pub fn do_reconfiguration_message(
    body: &DoReconfiguration,
    generation: &GenerationHeader,
) -> Result<Message> {
    reliability_message(body, "DoReconfiguration", RA_ACTOR, true, Some(generation))
}
pub fn decode_do_reconfiguration(
    message: &Message,
) -> Result<(DoReconfiguration, GenerationHeader)> {
    check_factory(message, "DoReconfiguration", &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}
pub fn do_reconfiguration_reply_message(
    body: &ConfigurationReply,
    owner: Owner,
) -> Result<Message> {
    reliability_message(
        body,
        "DoReconfigurationReply",
        match owner {
            Owner::Fm => FM_ACTOR,
            Owner::Fmm => FMM_ACTOR,
        },
        true,
        None,
    )
}
pub fn decode_do_reconfiguration_reply(message: &Message) -> Result<(ConfigurationReply, Owner)> {
    let actor = check_factory(message, "DoReconfigurationReply", &[FM_ACTOR, FMM_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        if actor == FM_ACTOR {
            Owner::Fm
        } else {
            Owner::Fmm
        },
    ))
}
pub fn activate_message(body: &Activate, generation: &GenerationHeader) -> Result<Message> {
    reliability_message(body, "Activate", RA_ACTOR, true, Some(generation))
}
pub fn decode_activate(message: &Message) -> Result<(Activate, GenerationHeader)> {
    check_factory(message, "Activate", &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}
pub fn activate_reply_message(
    body: &ActivateReply,
    generation: &GenerationHeader,
) -> Result<Message> {
    reliability_message(body, "ActivateReply", RA_ACTOR, true, Some(generation))
}
pub fn decode_activate_reply(message: &Message) -> Result<(ActivateReply, GenerationHeader)> {
    check_factory(message, "ActivateReply", &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}
