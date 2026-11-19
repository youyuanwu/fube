use std::future::poll_fn;

use tokio::time::{timeout_at, Instant};
use tower_service::Service;

use crate::{
    body::MAX_BODY_BYTES, codec, Body, BoxError, Codec, CodecError, Error, Request, Response,
    Result,
};

/// Optional encoded-request and response-input restrictions, not heap quotas.
/// Zero permits empty bodies; defaults allow the full native body capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientCodecLimits {
    pub max_encoded_body_bytes: usize,
    pub max_response_body_bytes: usize,
}

impl Default for ClientCodecLimits {
    fn default() -> Self {
        Self {
            max_encoded_body_bytes: MAX_BODY_BYTES,
            max_response_body_bytes: MAX_BODY_BYTES,
        }
    }
}

impl ClientCodecLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_encoded_body_bytes > MAX_BODY_BYTES
            || self.max_response_body_bytes > MAX_BODY_BYTES
        {
            return Err(Error::InvalidInput(
                "codec body limit exceeds native frame capacity",
            ));
        }
        Ok(())
    }
}

/// Typed calls over a byte-facing Tower service, with a consumed codec per call.
///
/// Construction is inert. Calls borrow the exact transport mutably, never clone
/// it, and never retry or replay. Clone the client before concurrent calls when
/// its transport supports cloning.
#[derive(Debug, Clone)]
pub struct RpcClient<T> {
    transport: T,
    limits: ClientCodecLimits,
}

impl<T> RpcClient<T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            limits: ClientCodecLimits::default(),
        }
    }

    pub fn codec_limits(mut self, limits: ClientCodecLimits) -> Result<Self> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }

    pub fn into_inner(self) -> T {
        self.transport
    }

    /// Configure the wrapped service between calls without replacing its readiness state.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.transport
    }
}

impl<T> RpcClient<T>
where
    T: Service<Request<Body>, Response = Response<Body>> + Send,
    T::Future: Send,
    T::Error: Into<BoxError>,
{
    /// One absolute deadline covers encoding, readiness, submission, and receipt.
    /// An on-time native response stays on time even when observed/decoded later.
    /// Synthetic responses use service-result observation time instead.
    pub async fn call<C>(
        &mut self,
        request: Request<C::Encode>,
        mut codec: C,
        deadline: Instant,
    ) -> Result<Response<C::Decode>>
    where
        C: Codec + Send,
        C::Encode: Send,
        C::Decode: Send,
        C::Encoder: Send,
        C::Decoder: Send,
    {
        let deadline = request
            .deadline()
            .map_or(deadline, |local| local.min(deadline));
        if Instant::now() >= deadline {
            return Err(Error::Timeout("RPC"));
        }
        let (mut parts, item) = request.into_parts();
        parts.deadline = Some(deadline);
        let body = codec::encode(
            &mut codec.encoder(),
            item,
            self.limits.max_encoded_body_bytes,
        )
        .map_err(|source| Error::Codec {
            phase: "encode",
            source,
        })?;
        if Instant::now() >= deadline {
            return Err(Error::Timeout("RPC"));
        }
        timeout_at(deadline, poll_fn(|cx| self.transport.poll_ready(cx)))
            .await
            .map_err(|_| Error::Timeout("RPC"))?
            .map_err(|error| Error::transport("RPC readiness", error))?;
        if Instant::now() >= deadline {
            return Err(Error::Timeout("RPC"));
        }
        let response = timeout_at(
            deadline,
            self.transport.call(Request::from_parts(parts, body)),
        )
        .await
        .map_err(|_| Error::Timeout("RPC"))?
        .map_err(|error| Error::transport("RPC service", error))?;
        if response.settled_at().unwrap_or_else(Instant::now) > deadline {
            return Err(Error::Timeout("RPC"));
        }
        let (parts, body) = response.into_parts();
        if body.len() > self.limits.max_response_body_bytes {
            return Err(Error::Codec {
                phase: "decode",
                source: CodecError::resource_limit("response body bytes"),
            });
        }
        let value = codec::decode(&mut codec.decoder(), &body).map_err(|source| Error::Codec {
            phase: "decode",
            source,
        })?;
        Ok(Response::from_parts(parts, value))
    }
}
