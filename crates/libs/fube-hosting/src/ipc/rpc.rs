//! Typed Hosting methods; domain admission precedes bounded DTO decoding.
use super::{admit_dispatch, deadline, dispatch, protocol, reply_budget, wire, Entry, Shared};
use fube_core::protocol::actor as actors;
use furpc::{
    header, ActorId, Body, Codec, CodecError, DecodeBuf, Decoder, EncodeBuf, Encoder, Error,
    FubstCodec, IngressResponse, Reply, Request, Result,
};
use serde::de::DeserializeOwned;
use std::{
    error::Error as _,
    future::{ready, Ready},
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
    time::Instant,
};
use tower_service::Service;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Method {
    Start,
    Finish,
    RegisterRuntime,
    RegisterType,
    UnregisterRuntime,
    UnregisterHost,
    Lease,
}

impl Method {
    const ALL: [Self; 7] = [
        Self::Start,
        Self::Finish,
        Self::RegisterRuntime,
        Self::RegisterType,
        Self::UnregisterRuntime,
        Self::UnregisterHost,
        Self::Lease,
    ];

    pub(super) fn envelope(self) -> (i64, &'static str) {
        match self {
            Self::Start => (
                actors::APPLICATION_HOST_MANAGER,
                "StartRegisterApplicationHostRequest",
            ),
            Self::Finish => (
                actors::APPLICATION_HOST_MANAGER,
                "FinishRegisterApplicationHostRequest",
            ),
            Self::RegisterRuntime => (
                actors::FABRIC_RUNTIME_MANAGER,
                "RegisterFabricRuntimeRequest",
            ),
            Self::RegisterType => (actors::FABRIC_RUNTIME_MANAGER, "RegisterServiceTypeRequest"),
            Self::UnregisterRuntime => (
                actors::FABRIC_RUNTIME_MANAGER,
                "UnregisterFabricRuntimeRequest",
            ),
            Self::UnregisterHost => (
                actors::APPLICATION_HOST_MANAGER,
                "UnregisterApplicationHostRequest",
            ),
            Self::Lease => (actors::HOSTING, ""),
        }
    }

    pub(super) fn select(actor: i64, action: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|method| method.envelope() == (actor, action))
            .ok_or(protocol("unsupported Hosting capability"))
    }
}

pub(super) enum Call {
    Start(wire::StartRequest),
    Finish(wire::IdRequest),
    RegisterRuntime(wire::RuntimeRequest),
    RegisterType(wire::TypeRequest),
    UnregisterRuntime(wire::IdRequest),
    UnregisterHost(wire::UnregisterHost),
    Lease,
}

impl Call {
    pub(super) fn method(&self) -> Method {
        match self {
            Self::Start(_) => Method::Start,
            Self::Finish(_) => Method::Finish,
            Self::RegisterRuntime(_) => Method::RegisterRuntime,
            Self::RegisterType(_) => Method::RegisterType,
            Self::UnregisterRuntime(_) => Method::UnregisterRuntime,
            Self::UnregisterHost(_) => Method::UnregisterHost,
            Self::Lease => Method::Lease,
        }
    }
}

// Keep DTOs inline instead of adding an allocation outside the reply budget.
#[allow(clippy::large_enum_variant)]
pub(super) enum Response {
    Start(wire::StartReply),
    Error(wire::ErrorReply),
    Lease(wire::LeaseReply),
}

#[derive(Clone)]
pub(super) struct HostingRpc {
    shared: Arc<Shared>,
    entry: Arc<Entry>,
    method: Method,
    type_deadline: Option<Instant>,
}

impl HostingRpc {
    pub(super) fn new(
        shared: Arc<Shared>,
        entry: Arc<Entry>,
        method: Method,
        type_deadline: Option<Instant>,
    ) -> Self {
        Self {
            shared,
            entry,
            method,
            type_deadline,
        }
    }

    fn codec(&self) -> HostingCodec {
        HostingCodec(self.clone())
    }

    pub(super) fn method(&self) -> Method {
        self.method
    }

    pub(super) async fn unary(self, request: Request<Body>) -> Result<IngressResponse> {
        let mut rpc = furpc::server::Rpc::new(self.codec());
        let lease = self.method == Method::Lease;
        let entry = self.entry.clone();
        let result = rpc.unary(self, request).await.map_err(|error| {
            // Admission errors pass through the decoder, but remain domain causes.
            if let Error::Codec { source, .. } = &error {
                if let Some(original) = source
                    .source()
                    .and_then(|source| source.downcast_ref::<Error>())
                {
                    return original.clone();
                }
            }
            error
        })?;
        // Cleanup replies retain their original runner even after lease retirement.
        if lease && !entry.authority.live() {
            return Err(Error::Closed);
        }
        Ok(result)
    }

    pub(super) fn decode(&self, src: &mut DecodeBuf<'_>) -> Result<Call> {
        if self.method == Method::Lease {
            if !src.is_empty() {
                return Err(protocol("invalid actor-15 request"));
            }
            deadline(&self.shared, &self.entry)?;
            if !self.entry.ready.load(Ordering::SeqCst) {
                return Err(protocol("lease before completed registration"));
            }
            return Ok(Call::Lease);
        }
        let (_state, _) =
            admit_dispatch(&self.shared, &self.entry, self.method, self.type_deadline)?;
        let budget = self.shared.config.decode_bytes;
        Ok(match self.method {
            Method::Start => Call::Start(decode_body(src, budget)?),
            Method::Finish => Call::Finish(decode_body(src, budget)?),
            Method::RegisterRuntime => Call::RegisterRuntime(decode_body(src, budget)?),
            Method::RegisterType => Call::RegisterType(decode_body(src, budget)?),
            Method::UnregisterRuntime => Call::UnregisterRuntime(decode_body(src, budget)?),
            Method::UnregisterHost => Call::UnregisterHost(decode_body(src, budget)?),
            Method::Lease => return Err(Error::Internal("Hosting RPC method")),
        })
    }
}

impl Service<Request<Call>> for HostingRpc {
    type Response = Reply<Response>;
    type Error = Error;
    type Future = Ready<Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Call>) -> Self::Future {
        ready((|| {
            let call = request.into_body();
            if call.method() != self.method {
                return Err(Error::Internal("Hosting RPC schema"));
            }
            let response = if matches!(call, Call::Lease) {
                deadline(&self.shared, &self.entry)?;
                if !self.entry.ready.load(Ordering::SeqCst) {
                    return Err(protocol("lease before completed registration"));
                }
                let ttl = self.entry.authority.poll().ok_or(Error::Closed)?;
                reply_budget(&self.shared, 32)?;
                Response::Lease(wire::LeaseReply { ttl })
            } else {
                dispatch(&self.shared, &self.entry, call, self.type_deadline)?
            };
            Reply::new(response).append_header(header::actor(ActorId::new(actors::EMPTY)?)?)
        })())
    }
}

struct HostingCodec(HostingRpc);
struct RequestDecoder(HostingRpc);
struct ResponseEncoder;

impl Codec for HostingCodec {
    type Encode = Response;
    type Decode = Call;
    type Encoder = ResponseEncoder;
    type Decoder = RequestDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        ResponseEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RequestDecoder(self.0.clone())
    }
}

impl Decoder for RequestDecoder {
    type Item = Call;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> std::result::Result<Call, CodecError> {
        self.0.decode(src).map_err(|error| match error {
            Error::Codec { source, .. } => source,
            error => CodecError::invalid_payload(error),
        })
    }
}

impl Encoder for ResponseEncoder {
    type Item = Response;

    fn encode(
        &mut self,
        item: Response,
        dst: &mut EncodeBuf<'_>,
    ) -> std::result::Result<(), CodecError> {
        match item {
            Response::Start(value) => FubstCodec::<wire::StartReply, ()>::default()
                .encoder()
                .encode(value, dst),
            Response::Error(value) => FubstCodec::<wire::ErrorReply, ()>::default()
                .encoder()
                .encode(value, dst),
            Response::Lease(value) => FubstCodec::<wire::LeaseReply, ()>::default()
                .encoder()
                .encode(value, dst),
        }
    }
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T> {
    let mut left = limit;
    fubst::serde::from_slice_with_budget(bytes, &mut |capacity| {
        left = left
            .checked_sub(capacity)
            .ok_or(fubst::Error::ResourceLimit("Hosting decode budget"))?;
        Ok(())
    })
    .map_err(|source| Error::Codec {
        phase: "decode",
        source: source.into(),
    })
}

fn decode_body<T: DeserializeOwned>(src: &mut DecodeBuf<'_>, budget: usize) -> Result<T> {
    let map = |source| Error::Codec {
        phase: "decode",
        source,
    };
    let len = src.remaining();
    if src.chunk().len() == len {
        let value = decode(src.chunk(), budget)?;
        src.advance(len).map_err(map)?;
        Ok(value)
    } else {
        decode(&src.copy_remaining().map_err(map)?, budget)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segmented_dto_decode_preserves_the_allocation_budget() {
        let value = wire::IdRequest {
            id: "admitted-host".repeat(32),
        };
        let bytes = fubst::serde::to_vec(&value).unwrap();
        for split in [0, bytes.len() / 2] {
            let body = Body::from_segments(vec![
                Body::from_bytes(bytes[..split].to_vec()).unwrap(),
                Body::from_bytes(bytes[split..].to_vec()).unwrap(),
            ])
            .unwrap();
            let decoded: wire::IdRequest = decode_body(&mut DecodeBuf::new(&body), 4096).unwrap();
            assert_eq!(decoded.id, value.id);
            assert!(matches!(
                decode_body::<wire::IdRequest>(&mut DecodeBuf::new(&body), 1),
                Err(Error::Codec {
                    phase: "decode",
                    source: CodecError::ResourceLimit { .. },
                })
            ));
        }
    }
}
