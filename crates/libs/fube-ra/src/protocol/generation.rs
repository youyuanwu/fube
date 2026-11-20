//! Ordinary-FM generation negotiation and local inventory upload.
use super::*;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LfumUpload {
    pub generation: GenerationNumber,
    pub node: NodeDescription,
    pub units: ObjectArray<Vec<FailoverUnitInfo>>,
    pub any_replica_found: bool,
}

/// The request carries generation in its body, not a GenerationHeader.
pub fn lfum_upload_message(body: &LfumUpload) -> Result<Message> {
    reliability_message(body, "LFUMUpload", FM_ACTOR, true, None)
}

pub fn decode_lfum_upload(message: &Message) -> Result<LfumUpload> {
    without_generation(message, "LFUMUpload")?;
    decode_body(message)
}

pub fn lfum_upload_reply_message(generation: &GenerationHeader) -> Result<Message> {
    ordinary_fm(generation)?;
    envelope(
        vec![
            header::actor(ActorId::new(RA_ACTOR)?)?,
            header::action("LFUMUploadReply")?,
            priority(true)?,
            generation.header()?,
        ],
        Vec::new(),
    )
}

pub fn decode_lfum_upload_reply(message: &Message) -> Result<GenerationHeader> {
    check_factory(message, "LFUMUploadReply", &[RA_ACTOR])?;
    if !message.body().is_empty() {
        return Err(Error::Protocol("LFUMUploadReply must be bodyless"));
    }
    let generation = from_slice(unique(message, HeaderId::new(GENERATION_HEADER)?)?.value())?;
    ordinary_fm(&generation)?;
    Ok(generation)
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationProposalReply {
    pub proposed: GenerationNumber,
    pub current: GenerationNumber,
    pub error: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationUpdate {
    pub fm_service_epoch: Epoch,
}

/// Submit through a correlated request context, not a one-way sender.
/// The request transport owns MessageId/correlation metadata.
pub fn generation_proposal_message(generation: &GenerationHeader) -> Result<Message> {
    envelope(
        vec![
            header::actor(ActorId::new(RA_ACTOR)?)?,
            header::action("GenerationProposal")?,
            priority(true)?,
            generation.header()?,
        ],
        Vec::new(),
    )
}

pub fn decode_generation_proposal(message: &Message) -> Result<GenerationHeader> {
    check_factory(message, "GenerationProposal", &[RA_ACTOR])?;
    if !message.body().is_empty() {
        return Err(Error::Protocol("GenerationProposal must be bodyless"));
    }
    from_slice(unique(message, HeaderId::new(GENERATION_HEADER)?)?.value())
}

/// Reply on the original request context. Unlike SendToFMM, that path does not
/// rewrite the native FM factory actor, even for an FMM proposal.
pub fn generation_proposal_reply_message(body: &GenerationProposalReply) -> Result<Message> {
    reliability_message(body, "GenerationProposalReply", FM_ACTOR, true, None)
}

fn without_generation(message: &Message, action: &str) -> Result<()> {
    check_factory(message, action, &[FM_ACTOR])?;
    if message
        .headers()
        .records()
        .any(|h| h.id().get() == GENERATION_HEADER)
    {
        return Err(Error::Protocol("unexpected generation negotiation header"));
    }
    Ok(())
}

pub fn decode_generation_proposal_reply(message: &Message) -> Result<GenerationProposalReply> {
    without_generation(message, "GenerationProposalReply")?;
    decode_body(message)
}

fn ordinary_fm(generation: &GenerationHeader) -> Result<()> {
    if generation.for_fm_replica {
        return Err(Error::Protocol("FMM generation control is not supported"));
    }
    Ok(())
}

pub fn generation_update_message(
    body: &GenerationUpdate,
    generation: &GenerationHeader,
) -> Result<Message> {
    ordinary_fm(generation)?;
    reliability_message(body, "GenerationUpdate", RA_ACTOR, true, Some(generation))
}

pub fn decode_generation_update(message: &Message) -> Result<(GenerationUpdate, GenerationHeader)> {
    check_factory(message, "GenerationUpdate", &[RA_ACTOR])?;
    let generation = from_slice(unique(message, HeaderId::new(GENERATION_HEADER)?)?.value())?;
    ordinary_fm(&generation)?;
    Ok((decode_body(message)?, generation))
}

/// Ordinary-FM one-way rejection: the body itself is the current proposal.
/// FMM routing/actor rewriting remains outside this selected profile.
pub fn generation_update_reject_message(body: &GenerationNumber) -> Result<Message> {
    reliability_message(body, "GenerationUpdateReject", FM_ACTOR, true, None)
}

pub fn decode_generation_update_reject(message: &Message) -> Result<GenerationNumber> {
    without_generation(message, "GenerationUpdateReject")?;
    decode_body(message)
}
