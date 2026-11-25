//! Typed bootstrap calls on the original FM/Node relationship.
use super::{
    bounded_parts, deadline, err, one_way, outbound_headers, Link, Properties, Result, LIMIT,
};
use fube_ra::protocol::{
    GenerationHeader, GenerationProposalReply, LfumUpload, NodeUp, NodeUpAck, FM_ACTOR,
    GENERATION_HEADER, RA_ACTOR, RS_ACTOR,
};
use furpc::{
    header, Body, ClientCodecLimits, CodecError, DecodeBuf, Decoder, EncodeBuf, Encoder, Error,
    FubstEncoder, HeaderId, HeaderRef, Headers, PeerRequestService, Request, Response, RpcClient,
};
use serde::de::DeserializeOwned;
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tower_service::Service;

mod server;

/// Owned typed snapshot of one caller-frozen LFUM inventory.
pub struct LfumUploadRequest(Request<LfumUpload>);

impl LfumUploadRequest {
    pub fn new(body: &LfumUpload) -> Result<Self> {
        one_way::request(body.clone(), "LFUMUpload", FM_ACTOR, true, None).map(Self)
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

    fn admit(self, link: &Link, response: &Response<Body>) -> Result<()> {
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
        unique(headers, header::ACTION).map_err(err)?;
        unique(headers, header::ACTOR).map_err(err)?;
        unique(headers, header::HIGH_PRIORITY).map_err(err)?;
        if !link.is_current_parts(headers, response.body())? {
            return Err("foreign/stale/duplicate native FM generation".into());
        }
        Ok(())
    }
}

pub(super) fn unique(headers: &Headers, id: HeaderId) -> furpc::Result<HeaderRef<'_>> {
    let mut matching = headers.records().filter(|header| header.id() == id);
    let header = matching
        .next()
        .ok_or(Error::Protocol("missing reliability header"))?;
    if matching.next().is_some() {
        return Err(Error::Protocol("duplicate reliability header"));
    }
    Ok(header)
}

struct BootstrapService {
    transport: PeerRequestService,
    link: Link,
    method: Method,
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
        Box::pin(async move {
            let response = future.await?;
            method
                .admit(&link, &response)
                .map_err(|source| Error::Codec {
                    phase: "bootstrap admission",
                    source: CodecError::invalid_payload(source),
                })?;
            Ok(response)
        })
    }
}

struct BootstrapDecoder<R> {
    method: Method,
    marker: PhantomData<fn() -> R>,
}
struct EmptyEncoder;

impl Encoder for EmptyEncoder {
    type Item = ();

    fn encode(&mut self, _: (), _: &mut EncodeBuf<'_>) -> std::result::Result<(), CodecError> {
        Ok(())
    }
}

impl<R: DeserializeOwned> Decoder for BootstrapDecoder<R> {
    type Item = (R, Option<GenerationHeader>);

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> std::result::Result<Self::Item, CodecError> {
        let headers = src.headers();
        let mut budget = 4 * LIMIT;
        // NodeUpAck's native decoder accounts for its headers and body together.
        if self.method == Method::NodeUp {
            let action = unique(headers, header::ACTION).map_err(CodecError::invalid_payload)?;
            budget = budget
                .checked_sub(action.value().len().saturating_mul(2))
                .ok_or_else(|| CodecError::invalid_payload("bootstrap decode allocation"))?;
        }
        let generation = if self.method == Method::GenerationProposal {
            if headers
                .records()
                .any(|header| header.id().get() == GENERATION_HEADER)
            {
                return Err(CodecError::invalid_payload(
                    "unexpected generation negotiation header",
                ));
            }
            None
        } else {
            let header = unique(
                headers,
                HeaderId::new(GENERATION_HEADER).map_err(CodecError::invalid_payload)?,
            )
            .map_err(CodecError::invalid_payload)?;
            Some(fubst::serde::from_slice_with_budget(
                header.value(),
                &mut |bytes| reserve(&mut budget, bytes),
            )?)
        };
        if self.method == Method::LfumUpload && !src.is_empty() {
            return Err(CodecError::invalid_payload(
                "LFUMUploadReply must be bodyless",
            ));
        }
        let value = decode_body(src, budget)?;
        Ok((value, generation))
    }
}

pub(super) fn decode_body<R: DeserializeOwned>(
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
        self.node_up_optional(body)
            .await?
            .ok_or("native request reply timeout".into())
    }

    pub(super) async fn node_up_optional(
        &self,
        body: &NodeUp,
    ) -> Result<Option<(NodeUpAck, GenerationHeader)>> {
        self.bootstrap(
            one_way::request(body, "NodeUp", FM_ACTOR, true, None)?,
            FubstEncoder::<&NodeUp>::default(),
            Method::NodeUp,
        )
        .await?
        .map(|(ack, generation)| Ok((ack, generation.ok_or("NodeUpAck missing generation")?)))
        .transpose()
    }

    pub async fn lfum_upload(
        &self,
        request: LfumUploadRequest,
    ) -> Result<Option<GenerationHeader>> {
        self.bootstrap::<(), _>(
            request.0,
            FubstEncoder::<LfumUpload>::default(),
            Method::LfumUpload,
        )
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
                one_way::request((), "GenerationProposal", RA_ACTOR, true, Some(generation))?,
                EmptyEncoder,
                Method::GenerationProposal,
            )
            .await?
            .map(|(reply, _)| reply))
    }

    async fn bootstrap<R, E>(
        &self,
        mut request: Request<E::Item>,
        encoder: E,
        method: Method,
    ) -> Result<Option<(R, Option<GenerationHeader>)>>
    where
        R: DeserializeOwned + Send,
        E: Encoder + Send,
        E::Item: Send,
    {
        self.seal_headers(request.headers_mut())?;
        if !self.is_live() {
            return Err(err(Error::Closed));
        }
        outbound_headers(self.session.role(), request.headers(), true).map_err(err)?;
        let mut client = RpcClient::new(BootstrapService {
            transport: PeerRequestService::new(self.session.peer.clone()),
            link: self.clone(),
            method,
        })
        .codec_limits(ClientCodecLimits {
            max_encoded_body_bytes: LIMIT,
            max_response_body_bytes: LIMIT,
        })
        .map_err(err)?;
        let decoder = BootstrapDecoder::<R> {
            method,
            marker: PhantomData,
        };
        match client.call(request, encoder, decoder, deadline()).await {
            Ok(response) => Ok(Some(response.into_body())),
            Err(Error::Timeout(_)) => Ok(None),
            Err(error) => Err(err(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::links;
    use super::*;
    use fube_ra::protocol::{
        decode_lfum_upload, lfum_upload_message, node_up_ack_message, node_up_message,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::time::{Duration, Instant};

    struct CountedEncoder(Arc<AtomicUsize>);

    impl Encoder for CountedEncoder {
        type Item = NodeUp;

        fn encode(
            &mut self,
            body: NodeUp,
            dst: &mut EncodeBuf<'_>,
        ) -> std::result::Result<(), CodecError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            assert_eq!(dst.headers().effective().actor.get(), FM_ACTOR);
            assert!(dst
                .headers()
                .records()
                .any(|header| header.id().get() == super::super::PTOP));
            FubstEncoder::<NodeUp>::default().encode(body, dst)
        }
    }

    #[tokio::test]
    async fn bootstrap_encoding_is_poll_guarded_and_retired_links_never_encode() {
        let (node, fm) = links(false).await;
        let body = NodeUp::default();
        let request = || one_way::request(body.clone(), "NodeUp", FM_ACTOR, true, None).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        drop(node.bootstrap::<NodeUpAck, _>(
            request(),
            CountedEncoder(calls.clone()),
            Method::NodeUp,
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let mut expired = request();
        expired.set_deadline(Instant::now() - Duration::from_secs(1));
        assert!(node
            .bootstrap::<NodeUpAck, _>(expired, CountedEncoder(calls.clone()), Method::NodeUp,)
            .await
            .unwrap()
            .is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(fm
            .receive(Instant::now() + Duration::from_millis(5))
            .await
            .unwrap()
            .is_none());

        let ack = NodeUpAck::default();
        let generation = fm.generation();
        let (result, ()) = tokio::join!(
            node.bootstrap::<NodeUpAck, _>(
                request(),
                CountedEncoder(calls.clone()),
                Method::NodeUp,
            ),
            async {
                let mut incoming = fm.receive_checked(deadline()).await.unwrap().unwrap();
                let expected = node_up_message(&body).unwrap();
                assert!(incoming
                    .message
                    .body()
                    .segments()
                    .flatten()
                    .eq(expected.body().segments().flatten()));
                incoming
                    .reply
                    .take()
                    .unwrap()
                    .reply(
                        fm.seal(node_up_ack_message(&ack, &generation).unwrap())
                            .unwrap(),
                        deadline(),
                    )
                    .await
                    .unwrap();
            }
        );
        assert_eq!(result.unwrap(), Some((ack, Some(generation))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        node.retire();
        assert!(node
            .bootstrap::<NodeUpAck, _>(request(), CountedEncoder(calls.clone()), Method::NodeUp,)
            .await
            .unwrap_err()
            .contains("Closed"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }

    #[tokio::test]
    async fn lfum_snapshot_stays_typed_until_bounded_encoding_and_keeps_original_inventory() {
        let (node, fm) = links(false).await;
        let mut body = LfumUpload {
            generation: node.generation().number,
            ..Default::default()
        };
        body.node.name = "frozen-node".into();
        let expected = body.clone();
        let request = LfumUploadRequest::new(&body).unwrap();
        assert_eq!(request.0.body(), &expected);
        body.node.name = "changed-node".into();
        body.generation.generation += 1;
        let generation = fm.generation();
        let (result, ()) = tokio::join!(node.lfum_upload(request), async {
            let mut incoming = fm.receive_checked(deadline()).await.unwrap().unwrap();
            assert_eq!(decode_lfum_upload(&incoming.message).unwrap(), expected);
            let native = lfum_upload_message(&expected).unwrap();
            assert!(incoming
                .message
                .body()
                .segments()
                .flatten()
                .eq(native.body().segments().flatten()));
            incoming
                .reply
                .take()
                .unwrap()
                .lfum_upload(&fm, |decoded| {
                    assert_eq!(decoded, expected);
                    Ok(generation.clone())
                })
                .await
                .unwrap();
        });
        assert_eq!(result.unwrap(), Some(generation));

        body.node.name = "x".repeat(LIMIT);
        let oversized = LfumUploadRequest::new(&body).unwrap();
        assert!(node
            .lfum_upload(oversized)
            .await
            .unwrap_err()
            .contains("ResourceLimit"));
        assert!(node.is_live());
        assert!(fm
            .receive(Instant::now() + Duration::from_millis(5))
            .await
            .unwrap()
            .is_none());
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }

    #[test]
    fn reply_decoder_reads_actual_headers_without_a_shared_metadata_handoff() {
        let ack = NodeUpAck::default();
        let generation = GenerationHeader {
            number: fube_ra::protocol::GenerationNumber {
                generation: 42,
                owner: [3; 16],
            },
            for_fm_replica: false,
        };
        let native = node_up_ack_message(&ack, &generation).unwrap();
        let bytes: Vec<_> = native.body().segments().flatten().copied().collect();
        for split in [0, bytes.len() / 2] {
            let body = Body::from_segments(vec![
                Body::from_bytes(bytes[..split].to_vec()).unwrap(),
                Body::from_bytes(bytes[split..].to_vec()).unwrap(),
            ])
            .unwrap();
            let mut decoder = BootstrapDecoder::<NodeUpAck> {
                method: Method::NodeUp,
                marker: PhantomData,
            };
            let mut src = DecodeBuf::with_headers(&body, native.headers());
            assert_eq!(
                decoder.decode(&mut src).unwrap(),
                (ack.clone(), Some(generation.clone()))
            );
            assert!(src.is_empty());
            assert!(decoder
                .decode(&mut DecodeBuf::new(&body))
                .unwrap_err()
                .to_string()
                .contains("missing reliability header"));
        }
    }

    #[test]
    fn body_decode_honors_remaining_header_budget_for_contiguous_and_segmented_bodies() {
        let value = "native-bootstrap".repeat(4);
        let bytes = fubst::serde::to_vec(&value).unwrap();
        for split in [0, bytes.len() / 2] {
            let body = Body::from_segments(vec![
                Body::from_bytes(bytes[..split].to_vec()).unwrap(),
                Body::from_bytes(bytes[split..].to_vec()).unwrap(),
            ])
            .unwrap();
            for budget in [1, 4096] {
                let mut decoder = |src: &mut DecodeBuf<'_>| decode_body::<String>(src, budget);
                let result = furpc::codec::decode(&mut decoder, &body, LIMIT);
                if budget == 1 {
                    assert!(matches!(result, Err(CodecError::ResourceLimit { .. })));
                } else {
                    assert_eq!(result.unwrap(), value);
                }
            }
        }
    }
}
