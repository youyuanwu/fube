//! Typed bootstrap calls on the original FM/Node relationship.
use super::{bounded_parts, deadline, err, outbound, Link, Properties, Result, LIMIT};
use fube_ra::protocol::{
    generation_proposal_message, lfum_upload_message, node_up_message, GenerationHeader,
    GenerationProposalReply, LfumUpload, NodeUp, NodeUpAck, FM_ACTOR, GENERATION_HEADER, RA_ACTOR,
    RS_ACTOR,
};
use furpc::{
    header, Body, ClientCodecLimits, Codec, CodecError, DecodeBuf, Decoder, EncodeBuf, Encoder,
    Error, HeaderId, HeaderRef, Headers, Message, PeerRequestService, Request, Response, RpcClient,
};
use serde::de::DeserializeOwned;
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};
use tower_service::Service;

mod server;

/// Opaque LFUM action/body pairing prepared from the caller's frozen inventory.
pub struct LfumUploadRequest(Message);

impl LfumUploadRequest {
    pub fn new(body: &LfumUpload) -> Result<Self> {
        lfum_upload_message(body).map(Self).map_err(err)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Method {
    NodeUp,
    LfumUpload,
    GenerationProposal,
}

impl Method {
    fn reply(self) -> (i64, &'static str) {
        match self {
            Self::NodeUp => (RS_ACTOR, "NodeUpAck"),
            Self::LfumUpload => (RA_ACTOR, "LFUMUploadReply"),
            Self::GenerationProposal => (FM_ACTOR, "GenerationProposalReply"),
        }
    }

    fn admit(self, link: &Link, response: &Response<Body>) -> Result<ValidatedReply> {
        bounded_parts(response.headers(), response.body()).map_err(err)?;
        let headers = response.headers();
        let props = Properties::from_headers(headers).map_err(err)?;
        if (props.actor, props.action.as_str()) != self.reply()
            || props.expects_reply
            || props.uncorrelated_reply
            || !props.high_priority
        {
            return Err("unsupported bootstrap reply".into());
        }
        let action = unique(headers, header::ACTION).map_err(err)?;
        unique(headers, header::ACTOR).map_err(err)?;
        unique(headers, header::HIGH_PRIORITY).map_err(err)?;
        if !link.is_current_parts(headers, response.body())? {
            return Err("foreign/stale/duplicate native FM generation".into());
        }
        let mut budget = 4 * LIMIT;
        // NodeUpAck's native decoder accounts for its headers and body together.
        if self == Self::NodeUp {
            budget = budget
                .checked_sub(action.value().len().saturating_mul(2))
                .ok_or("bootstrap decode allocation")?;
        }
        let generation = if self == Self::GenerationProposal {
            if headers
                .records()
                .any(|header| header.id().get() == GENERATION_HEADER)
            {
                return Err("unexpected generation negotiation header".into());
            }
            None
        } else {
            let header =
                unique(headers, HeaderId::new(GENERATION_HEADER).map_err(err)?).map_err(err)?;
            Some(
                fubst::serde::from_slice_with_budget(header.value(), &mut |bytes| {
                    reserve(&mut budget, bytes)
                })
                .map_err(err)?,
            )
        };
        if self == Self::LfumUpload && !response.body().is_empty() {
            return Err("LFUMUploadReply must be bodyless".into());
        }
        Ok(ValidatedReply { budget, generation })
    }
}

fn unique(headers: &Headers, id: HeaderId) -> furpc::Result<HeaderRef<'_>> {
    let mut matching = headers.records().filter(|header| header.id() == id);
    let header = matching
        .next()
        .ok_or(Error::Protocol("missing reliability header"))?;
    if matching.next().is_some() {
        return Err(Error::Protocol("duplicate reliability header"));
    }
    Ok(header)
}

struct ValidatedReply {
    budget: usize,
    generation: Option<GenerationHeader>,
}

struct BootstrapService {
    transport: PeerRequestService,
    link: Link,
    method: Method,
    admitted: Arc<OnceLock<ValidatedReply>>,
}

impl Service<Request<Body>> for BootstrapService {
    type Response = Response<Body>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = furpc::Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        if !self.link.is_live() {
            return Poll::Ready(Err(Error::Closed));
        }
        self.transport.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        if !self.link.is_live() {
            return Box::pin(async { Err(Error::Closed) });
        }
        let future = self.transport.call(request);
        let link = self.link.clone();
        let method = self.method;
        let admitted = self.admitted.clone();
        Box::pin(async move {
            let response = future.await?;
            let metadata = method
                .admit(&link, &response)
                .map_err(|source| Error::Codec {
                    phase: "bootstrap admission",
                    source: CodecError::invalid_payload(source),
                })?;
            admitted
                .set(metadata)
                .map_err(|_| Error::Internal("bootstrap reply already admitted"))?;
            Ok(response)
        })
    }
}

struct BootstrapCodec<R> {
    admitted: Arc<OnceLock<ValidatedReply>>,
    marker: PhantomData<fn() -> R>,
}
struct PreparedEncoder;

impl<R: DeserializeOwned> Codec for BootstrapCodec<R> {
    type Encode = Body;
    type Decode = (R, Option<GenerationHeader>);
    type Encoder = PreparedEncoder;
    type Decoder = Self;

    fn encoder(&mut self) -> Self::Encoder {
        PreparedEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        Self {
            admitted: self.admitted.clone(),
            marker: PhantomData,
        }
    }
}

impl Encoder for PreparedEncoder {
    type Item = Body;

    fn encode(
        &mut self,
        body: Body,
        dst: &mut EncodeBuf<'_>,
    ) -> std::result::Result<(), CodecError> {
        dst.append_body(body)
    }
}

impl<R: DeserializeOwned> Decoder for BootstrapCodec<R> {
    type Item = (R, Option<GenerationHeader>);

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> std::result::Result<Self::Item, CodecError> {
        // Admission supplies header metadata before the body-only codec runs.
        let metadata = self.admitted.get().ok_or_else(|| {
            CodecError::invalid_payload(Error::Internal("unadmitted bootstrap reply"))
        })?;
        let value = decode_body(src, metadata.budget)?;
        Ok((value, metadata.generation.clone()))
    }
}

fn decode_body<R: DeserializeOwned>(
    src: &mut DecodeBuf<'_>,
    mut budget: usize,
) -> std::result::Result<R, CodecError> {
    let mut decode = |bytes: &[u8]| {
        fubst::serde::from_slice_with_budget(bytes, &mut |bytes| reserve(&mut budget, bytes))
            .map_err(CodecError::from)
    };
    let len = src.remaining();
    if src.chunk().len() == len {
        let value = decode(src.chunk())?;
        src.advance(len)?;
        Ok(value)
    } else {
        decode(&src.copy_remaining()?)
    }
}

fn reserve(budget: &mut usize, bytes: usize) -> fubst::Result<()> {
    *budget = budget
        .checked_sub(bytes)
        .ok_or(fubst::Error::ResourceLimit("cluster native decode"))?;
    Ok(())
}

impl Link {
    pub async fn node_up(&self, body: &NodeUp) -> Result<(NodeUpAck, GenerationHeader)> {
        let (ack, generation) = self
            .bootstrap(node_up_message(body).map_err(err)?, Method::NodeUp)
            .await?
            .ok_or("native request reply timeout")?;
        Ok((ack, generation.ok_or("NodeUpAck missing generation")?))
    }

    pub async fn lfum_upload(
        &self,
        request: LfumUploadRequest,
    ) -> Result<Option<GenerationHeader>> {
        self.bootstrap::<()>(request.0, Method::LfumUpload)
            .await?
            .map(|(_, generation)| generation.ok_or("LFUMUploadReply missing generation".into()))
            .transpose()
    }

    pub async fn generation_proposal(
        &self,
        generation: &GenerationHeader,
    ) -> Result<GenerationProposalReply> {
        self.generation_proposal_optional(generation)
            .await?
            .ok_or("native request reply timeout".into())
    }

    pub async fn generation_proposal_optional(
        &self,
        generation: &GenerationHeader,
    ) -> Result<Option<GenerationProposalReply>> {
        Ok(self
            .bootstrap(
                generation_proposal_message(generation).map_err(err)?,
                Method::GenerationProposal,
            )
            .await?
            .map(|(reply, _)| reply))
    }

    async fn bootstrap<R: DeserializeOwned + Send>(
        &self,
        message: Message,
        method: Method,
    ) -> Result<Option<(R, Option<GenerationHeader>)>> {
        let message = self.seal(message)?;
        outbound(self.session.role(), &message, true).map_err(err)?;
        let mut request = Request::new(message.body().clone());
        *request.headers_mut() = message.headers().clone();
        let admitted = Arc::new(OnceLock::new());
        let mut client = RpcClient::new(BootstrapService {
            transport: PeerRequestService::new(self.session.peer.clone()),
            link: self.clone(),
            method,
            admitted: admitted.clone(),
        })
        .codec_limits(ClientCodecLimits {
            max_encoded_body_bytes: LIMIT,
            max_response_body_bytes: LIMIT,
        })
        .map_err(err)?;
        let codec = BootstrapCodec::<R> {
            admitted,
            marker: PhantomData,
        };
        match client.call(request, codec, deadline()).await {
            Ok(response) => Ok(Some(response.into_body())),
            Err(Error::Timeout(_)) => Ok(None),
            Err(error) => Err(err(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_reply_decode_keeps_header_reservations_for_contiguous_and_segmented_bodies() {
        let value = "native-bootstrap".repeat(4);
        let bytes = fubst::serde::to_vec(&value).unwrap();
        for split in [0, bytes.len() / 2] {
            let body = Body::from_segments(vec![
                Body::from_bytes(bytes[..split].to_vec()).unwrap(),
                Body::from_bytes(bytes[split..].to_vec()).unwrap(),
            ])
            .unwrap();
            for budget in [1, 4096] {
                let admitted = Arc::new(OnceLock::new());
                assert!(admitted
                    .set(ValidatedReply {
                        budget,
                        generation: None
                    })
                    .is_ok());
                let mut codec = BootstrapCodec::<String> {
                    admitted,
                    marker: PhantomData,
                };
                let result = furpc::codec::decode(&mut codec, &body, LIMIT);
                if budget == 1 {
                    assert!(matches!(result, Err(CodecError::ResourceLimit { .. })));
                } else {
                    assert_eq!(result.unwrap(), (value.clone(), None));
                }
            }
        }
    }
}
