//! Typed method adapters inside canonical byte-facing ingress services.
//!
//! These adapters own body conversion, not native reply authority or execution
//! scopes. Construct one per invocation in the outer service future.
//!
//! ```no_run
//! use furpc::{server, Body, FubstCodec, IngressResponse, Reply, Request};
//! use std::{future::{ready, Ready}, task::{Context, Poll}};
//! use tower_service::Service;
//!
//! struct Echo;
//! impl Service<Request<u32>> for Echo {
//!     type Response = Reply<u32>;
//!     type Error = furpc::Error;
//!     type Future = Ready<furpc::Result<Self::Response>>;
//!     fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
//!         Poll::Ready(Ok(()))
//!     }
//!     fn call(&mut self, request: Request<u32>) -> Self::Future {
//!         ready(Ok(Reply::new(request.into_body())))
//!     }
//! }
//!
//! // Install this byte-facing method through an IngressService request binding.
//! async fn method(request: Request<Body>) -> furpc::Result<IngressResponse> {
//!     server::Rpc::new(FubstCodec::<u32, u32>::default())
//!         .unary(Echo, request).await
//! }
//! ```

use tower_service::Service;

use crate::{
    codec,
    service::{InvocationKind, InvocationState},
    wire::headers::HeaderDecodeContext,
    Body, BoxError, Codec, Error, IngressResponse, Message, Reply, Request, Result,
};

/// An invocation-local codec adapting typed services above byte ingress.
/// Construction is inert; the codec and typed services need neither Clone nor Sync.
pub struct Rpc<C> {
    codec: C,
}

impl<C: Codec> Rpc<C> {
    pub fn new(codec: C) -> Self {
        Self { codec }
    }

    /// Decode, call the same prepared typed service, and encode response data.
    /// The original runner alone correlates and sends the authorized reply.
    pub async fn unary<S>(
        &mut self,
        mut service: S,
        request: Request<Body>,
    ) -> Result<IngressResponse>
    where
        C: Send + 'static,
        C::Encode: Send + 'static,
        C::Decode: Send + 'static,
        C::Encoder: Send + 'static,
        C::Decoder: Send + 'static,
        S: Service<Request<C::Decode>, Response = Reply<C::Encode>> + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
    {
        let state = InvocationState::for_request(&request, InvocationKind::Request)?;
        state
            .run(async {
                let (parts, body) = request.into_parts();
                let value = codec::decode(&mut self.codec.decoder(), &body).map_err(|source| {
                    Error::Codec {
                        phase: "decode",
                        source,
                    }
                })?;
                drop(body);
                state.check()?;
                state.ready(&mut service).await?;
                let reply = service
                    .call(Request::from_parts(parts, value))
                    .await
                    .map_err(|error| Error::handler("typed handler", error))?;
                state.check()?;
                let (headers, item) = reply.into_parts();
                let body = codec::encode(
                    &mut self.codec.encoder(),
                    item,
                    state.max_response_body_bytes,
                )
                .map_err(|source| Error::Codec {
                    phase: "encode",
                    source,
                })?;
                state.check()?;
                let message = Message::from_received(headers, body);
                message.validate(&HeaderDecodeContext::native(), u32::MAX as usize)?;
                Ok(IngressResponse::Reply(message))
            })
            .await
    }

    /// Decode and call a typed one-way service without constructing an encoder.
    /// Completion is not a semantic ACK or permission to originate a reply.
    pub async fn one_way<S>(
        &mut self,
        mut service: S,
        request: Request<Body>,
    ) -> Result<IngressResponse>
    where
        C: Send + 'static,
        C::Decode: Send + 'static,
        C::Decoder: Send + 'static,
        S: Service<Request<C::Decode>, Response = ()> + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
    {
        let state = InvocationState::for_request(&request, InvocationKind::OneWay)?;
        state
            .run(async {
                let (parts, body) = request.into_parts();
                let value = codec::decode(&mut self.codec.decoder(), &body).map_err(|source| {
                    Error::Codec {
                        phase: "decode",
                        source,
                    }
                })?;
                drop(body);
                state.check()?;
                state.ready(&mut service).await?;
                service
                    .call(Request::from_parts(parts, value))
                    .await
                    .map_err(|error| Error::handler("typed handler", error))?;
                state.check()?;
                Ok(IngressResponse::NoReply)
            })
            .await
    }
}
