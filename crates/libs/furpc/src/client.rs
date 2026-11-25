use std::future::poll_fn;

use tokio::time::{timeout_at, Instant};
use tower_service::Service;

use crate::{
    body::MAX_BODY_BYTES, codec, Body, BoxError, CodecError, DecodeBuf, Decoder, Encoder, Error,
    Request, Response, Result, WriteReceipt,
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

/// Typed calls over a byte-facing service, consuming actual encoder/decoder states.
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
    pub async fn call<E, D>(
        &mut self,
        request: Request<E::Item>,
        mut encoder: E,
        mut decoder: D,
        deadline: Instant,
    ) -> Result<Response<D::Item>>
    where
        E: Encoder + Send,
        E::Item: Send,
        D: Decoder + Send,
        D::Item: Send,
    {
        let deadline = effective_deadline(&request, deadline, "RPC")?;
        let request = encode_request(
            request,
            &mut encoder,
            deadline,
            self.limits.max_encoded_body_bytes,
        )?;
        let response = self.exchange(request, deadline).await?;
        decode_response(response, self.limits.max_response_body_bytes, |src| {
            decoder.decode(src)
        })
    }

    async fn exchange(
        &mut self,
        request: Request<Body>,
        deadline: Instant,
    ) -> Result<Response<Body>> {
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
        let response = timeout_at(deadline, self.transport.call(request))
            .await
            .map_err(|_| Error::Timeout("RPC"))?
            .map_err(|error| Error::transport("RPC service", error))?;
        if response.settled_at().unwrap_or_else(Instant::now) > deadline {
            return Err(Error::Timeout("RPC"));
        }
        Ok(response)
    }
}

/// Typed one-way sends over a byte-facing Tower service.
///
/// Calls consume an encoder and borrow the exact service mutably, without retries
/// or reply decoding. Success means a complete local write, not a semantic ACK.
///
/// ```no_run
/// use furpc::{header, FubstEncoder, OneWayClient, Peer, PeerSendService, Request, WriteReceipt};
/// use tokio::time::Instant;
///
/// async fn report(peer: Peer, deadline: Instant) -> furpc::Result<WriteReceipt> {
///     OneWayClient::new(PeerSendService::new(peer))
///         .send(
///             Request::new(42u32).append_header(header::action("Report")?)?,
///             FubstEncoder::<u32>::default(),
///             deadline,
///         ).await
/// }
/// ```
#[derive(Debug, Clone)]
pub struct OneWayClient<T> {
    transport: T,
    max_encoded_body_bytes: usize,
}

impl<T> OneWayClient<T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            max_encoded_body_bytes: MAX_BODY_BYTES,
        }
    }

    /// Restrict encoded output, not codec allocations. Zero permits empty bodies.
    pub fn max_encoded_body_bytes(mut self, limit: usize) -> Result<Self> {
        if limit > MAX_BODY_BYTES {
            return Err(Error::InvalidInput(
                "codec body limit exceeds native frame capacity",
            ));
        }
        self.max_encoded_body_bytes = limit;
        Ok(self)
    }

    pub fn into_inner(self) -> T {
        self.transport
    }

    /// Configure the wrapped service between sends without replacing readiness state.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.transport
    }
}

impl<T> OneWayClient<T>
where
    T: Service<Request<Body>, Response = WriteReceipt> + Send,
    T::Future: Send,
    T::Error: Into<BoxError>,
{
    /// One absolute deadline covers encoding, readiness, submission and writing.
    /// Native write settlement is preserved when its result is observed later.
    /// Dropping the future does not prove retraction of committed output.
    pub async fn send<E>(
        &mut self,
        request: Request<E::Item>,
        mut encoder: E,
        deadline: Instant,
    ) -> Result<WriteReceipt>
    where
        E: Encoder + Send,
        E::Item: Send,
    {
        let deadline = effective_deadline(&request, deadline, "send")?;
        let request = encode_request(request, &mut encoder, deadline, self.max_encoded_body_bytes)?;
        self.write(request, deadline).await
    }

    async fn write(&mut self, request: Request<Body>, deadline: Instant) -> Result<WriteReceipt> {
        if Instant::now() >= deadline {
            return Err(Error::Timeout("send"));
        }
        timeout_at(deadline, poll_fn(|cx| self.transport.poll_ready(cx)))
            .await
            .map_err(|_| Error::Timeout("send"))?
            .map_err(|error| Error::transport("send readiness", error))?;
        if Instant::now() >= deadline {
            return Err(Error::Timeout("send"));
        }
        let receipt = timeout_at(deadline, self.transport.call(request))
            .await
            .map_err(|_| Error::Timeout("send"))?
            .map_err(|error| Error::transport("send service", error))?;
        if receipt.written_at() > deadline {
            return Err(Error::Timeout("send"));
        }
        Ok(receipt)
    }
}

fn effective_deadline<Q>(
    request: &Request<Q>,
    deadline: Instant,
    phase: &'static str,
) -> Result<Instant> {
    let deadline = request
        .deadline()
        .map_or(deadline, |local| local.min(deadline));
    if Instant::now() >= deadline {
        return Err(Error::Timeout(phase));
    }
    Ok(deadline)
}

fn encode_request<E: Encoder>(
    request: Request<E::Item>,
    encoder: &mut E,
    deadline: Instant,
    max_bytes: usize,
) -> Result<Request<Body>> {
    let (mut parts, item) = request.into_parts();
    parts.deadline = Some(deadline);
    let body =
        codec::encode_with_headers(encoder, item, &parts.headers, max_bytes).map_err(|source| {
            Error::Codec {
                phase: "encode",
                source,
            }
        })?;
    Ok(Request::from_parts(parts, body))
}

fn decode_response<D>(
    response: Response<Body>,
    max_bytes: usize,
    decode: impl FnOnce(&mut DecodeBuf<'_>) -> std::result::Result<D, CodecError>,
) -> Result<Response<D>> {
    let (parts, body) = response.into_parts();
    if body.len() > max_bytes {
        return Err(Error::Codec {
            phase: "decode",
            source: CodecError::resource_limit("response body bytes"),
        });
    }
    let value = codec::decode_with(&body, &parts.headers, max_bytes, decode).map_err(|source| {
        Error::Codec {
            phase: "decode",
            source,
        }
    })?;
    Ok(Response::from_parts(parts, value))
}
