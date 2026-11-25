//! Typed method adapters inside canonical byte-facing ingress services.
//!
//! These adapters own body conversion, not native reply authority or execution
//! scopes. Construct one per invocation in the outer service future.
//!
//! ```no_run
//! use furpc::{server, Body, FubstEncoder, FubstDecoder, IngressResponse, Reply, Request};
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
//!     server::unary(Echo, request, FubstDecoder::<u32>::default(),
//!         FubstEncoder::<u32>::default()).await
//! }
//! ```

use tower_service::Service;

use crate::{
    codec,
    service::{InvocationKind, InvocationState},
    wire::headers::HeaderDecodeContext,
    Body, BoxError, Decoder, Encoder, Error, IngressResponse, Message, Reply, Request, Result,
};

/// Decode, call the same prepared typed service, and encode response data.
/// Accepts authorized ordinary requests or native notifications.
/// The original runner alone correlates and sends the authorized reply.
pub async fn unary<S, D, E>(
    mut service: S,
    request: Request<Body>,
    mut decoder: D,
    mut encoder: E,
) -> Result<IngressResponse>
where
    E: Encoder + Send,
    E::Item: Send + 'static,
    D: Decoder + Send,
    D::Item: Send + 'static,
    S: Service<Request<D::Item>, Response = Reply<E::Item>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    let state = InvocationState::for_reply(&request)?;
    state
        .run(async {
            let (parts, body) = request.into_parts();
            let value = codec::decode_with(&body, &parts.headers, state.max_body_bytes, |src| {
                decoder.decode(src)
            })
            .map_err(|source| Error::Codec {
                phase: "decode",
                source,
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
            let body = codec::encode_with_headers(
                &mut encoder,
                item,
                &headers,
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
/// Decode one original one-way invocation with its actual decoder state.
pub async fn one_way<S, D>(
    mut service: S,
    request: Request<Body>,
    mut decoder: D,
) -> Result<IngressResponse>
where
    D: Decoder + Send,
    D::Item: Send + 'static,
    S: Service<Request<D::Item>, Response = ()> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    let state = InvocationState::for_request(&request, InvocationKind::OneWay)?;
    state
        .run(async {
            let (parts, body) = request.into_parts();
            let value = codec::decode_with(&body, &parts.headers, state.max_body_bytes, |src| {
                decoder.decode(src)
            })
            .map_err(|source| Error::Codec {
                phase: "decode",
                source,
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
