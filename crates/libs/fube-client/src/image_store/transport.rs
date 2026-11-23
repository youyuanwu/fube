//! Typed original-Peer methods; upload stages and semantic completion stay in the owner.
use super::{wire, FurpcClientError, ImageStoreError, Result, UploadState};
use furpc::{
    header, ActorId, Body, Codec, CodecError, DecodeBuf, Decoder, EncodeBuf, Encoder, FubstCodec,
    HeaderRecord, Headers, OneWayClient, Peer, PeerRequestService, PeerSendService, Request,
    Response, RpcClient,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};
use tokio::time::Instant;
use tower_service::Service;

pub(super) enum UploadRequest {
    Create(HeaderRecord),
    Content { number: u64, bytes: Vec<u8> },
    Commit,
    CommitAck,
    DeleteSession,
}

impl UploadRequest {
    pub(super) fn create(path: &str, size: u64) -> Result<Self> {
        wire::create(path, size).map(Self::Create)
    }

    fn prepare(self, activity: [u8; 16], deadline: Instant) -> Result<Request<Body>> {
        let mut headers = wire::headers(activity)?;
        let (action, body) = match self {
            Self::Create(create) => {
                headers.push(create);
                ("FileCreate", Body::empty())
            }
            Self::Content { number, bytes } => {
                headers.push(wire::sequence(number, bytes.len() as u64)?);
                ("FileContent", Body::from_bytes(bytes)?)
            }
            Self::Commit => ("FileUploadCommit", Body::empty()),
            Self::CommitAck => ("FileUploadCommitAck", Body::empty()),
            Self::DeleteSession => ("FileUploadDeleteSession", Body::empty()),
        };
        headers.extend([
            header::actor(ActorId::new(26)?)?,
            header::action(action)?,
            wire::timeout(deadline)?,
        ]);
        let mut request = Request::new(body);
        for header in headers {
            request = request.append_header(header)?;
        }
        Ok(request)
    }
}

pub(super) struct UploadClient {
    peer: Peer,
    sends: OneWayClient<PeerSendService>,
    state: Arc<UploadState>,
}

impl UploadClient {
    pub(super) fn new(peer: Peer, state: Arc<UploadState>) -> Self {
        Self {
            sends: OneWayClient::new(PeerSendService::new(peer.clone())),
            peer,
            state,
        }
    }

    pub(super) async fn delete(
        &mut self,
        destination: &str,
        activity: [u8; 16],
        deadline: Instant,
    ) -> Result<Option<[u8; 16]>> {
        self.state.check()?;
        let (mut headers, body) = wire::delete(destination, activity)?;
        headers.extend([
            header::actor(ActorId::new(5)?)?,
            header::action("ForwardMessage")?,
            wire::timeout(deadline)?,
        ]);
        let mut request = Request::new(body);
        for header in headers {
            request = request.append_header(header)?;
        }
        let received = Arc::new(OnceLock::new());
        let mut client = RpcClient::new(DeleteService {
            transport: PeerRequestService::new(self.peer.clone()),
            state: self.state.clone(),
            received: received.clone(),
        });
        let response = client
            .call(request, DeleteCodec(received), deadline)
            .await
            .map_err(ImageStoreError::from)?;
        Ok(response.into_body())
    }

    pub(super) async fn send(
        &mut self,
        request: UploadRequest,
        activity: [u8; 16],
        deadline: Instant,
    ) -> Result<()> {
        self.state.check()?;
        self.sends
            .send(
                request.prepare(activity, deadline)?,
                PreparedBodyCodec,
                deadline,
            )
            .await
            .map(|_| ())
            .map_err(ImageStoreError::from)
    }
}

struct DeleteService {
    transport: PeerRequestService,
    state: Arc<UploadState>,
    received: Arc<OnceLock<Headers>>,
}

impl Service<Request<Body>> for DeleteService {
    type Response = Response<Body>;
    type Error = ImageStoreError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Err(error) = self.state.check() {
            return Poll::Ready(Err(error));
        }
        self.transport.poll_ready(cx).map_err(ImageStoreError::from)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let future = self.transport.call(request);
        let state = self.state.clone();
        let received = self.received.clone();
        Box::pin(async move {
            let response = future.await?;
            state.check()?;
            received
                .set(response.headers().clone())
                .map_err(|_| furpc::Error::Internal("Delete reply already admitted"))?;
            Ok(response)
        })
    }
}

pub(super) struct PreparedEncoder;
struct PreparedBodyCodec;

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

impl Codec for PreparedBodyCodec {
    type Encode = Body;
    type Decode = ();
    type Encoder = PreparedEncoder;
    type Decoder = <FubstCodec<(), ()> as Codec>::Decoder;

    fn encoder(&mut self) -> Self::Encoder {
        PreparedEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        FubstCodec::<(), ()>::default().decoder()
    }
}

struct DeleteCodec(Arc<OnceLock<Headers>>);

impl Codec for DeleteCodec {
    type Encode = Body;
    type Decode = Option<[u8; 16]>;
    type Encoder = PreparedEncoder;
    type Decoder = Self;

    fn encoder(&mut self) -> Self::Encoder {
        PreparedEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        Self(self.0.clone())
    }
}

impl Decoder for DeleteCodec {
    type Item = Option<[u8; 16]>;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> std::result::Result<Self::Item, CodecError> {
        let headers = self.0.get().ok_or_else(|| {
            CodecError::invalid_payload(furpc::Error::Internal("unadmitted Delete reply"))
        })?;
        wire::decode_delete(headers, src).map_err(codec_error)
    }
}

pub(super) struct AckCodec(pub(super) Headers);

impl Codec for AckCodec {
    type Encode = Body;
    type Decode = wire::FileTransferReply;
    type Encoder = PreparedEncoder;
    type Decoder = Self;

    fn encoder(&mut self) -> Self::Encoder {
        PreparedEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        Self(self.0.clone())
    }
}

impl Decoder for AckCodec {
    type Item = wire::FileTransferReply;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> std::result::Result<Self::Item, CodecError> {
        wire::decode_reply(&self.0, src).map_err(codec_error)
    }
}

fn codec_error(error: ImageStoreError) -> CodecError {
    match &error {
        ImageStoreError::Common(
            FurpcClientError::Decode(fubst::Error::ResourceLimit(_))
            | FurpcClientError::Native(furpc::Error::ResourceLimit(_)),
        ) => CodecError::resource_limit(error),
        _ => CodecError::invalid_payload(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fubst::serde::to_vec;
    use serde::Serialize;
    use std::time::Duration;

    fn decode_error(error: CodecError) -> ImageStoreError {
        furpc::Error::Codec {
            phase: "decode",
            source: error,
        }
        .into()
    }

    #[test]
    fn typed_content_keeps_raw_chunk_storage_and_native_sequence() {
        #[derive(serde::Deserialize)]
        struct Sequence {
            number: u64,
            last: bool,
            buffer_size: u64,
        }
        let bytes = vec![0x67; 1024];
        let storage = bytes.as_ptr();
        let request = UploadRequest::Content { number: 7, bytes }
            .prepare([3; 16], Instant::now() + Duration::from_secs(5))
            .unwrap();
        let sequence: Sequence = fubst::serde::from_slice(
            request
                .headers()
                .first_raw(furpc::HeaderId::new(0x803c).unwrap())
                .unwrap()
                .value(),
        )
        .unwrap();
        assert_eq!(
            (sequence.number, sequence.last, sequence.buffer_size),
            (7, false, 1024)
        );
        let body =
            furpc::codec::encode(&mut PreparedBodyCodec.encoder(), request.into_body(), 1024)
                .unwrap();
        assert_eq!(body.len(), 1024);
        assert_eq!(body.segments().next().unwrap().as_ptr(), storage);
    }

    #[test]
    fn typed_delete_and_ack_codecs_preserve_fragmented_native_failures_and_error_classes() {
        #[derive(Serialize)]
        struct Failure<'a> {
            code: i64,
            diagnostic: &'a str,
        }
        for code in [wire::FILE_NOT_FOUND, -7, 0] {
            let bytes = to_vec(&Failure {
                code,
                diagnostic: "native failure",
            })
            .unwrap();
            for split in [0, bytes.len() / 2] {
                let body = Body::from_segments(vec![
                    Body::from_bytes(bytes[..split].to_vec()).unwrap(),
                    Body::from_bytes(bytes[split..].to_vec()).unwrap(),
                ])
                .unwrap();
                let mut headers = Headers::new();
                headers
                    .append(header::actor(ActorId::new(5).unwrap()).unwrap())
                    .unwrap();
                headers
                    .append(header::action("ClientOperationFailure").unwrap())
                    .unwrap();
                let received = Arc::new(OnceLock::from(headers));
                let deleted = furpc::codec::decode(&mut DeleteCodec(received), &body, body.len())
                    .map_err(decode_error);
                match code {
                    wire::FILE_NOT_FOUND => assert_eq!(deleted.unwrap(), None),
                    -7 => assert!(matches!(deleted, Err(ImageStoreError::Common(
                        FurpcClientError::Domain(failure)
                    )) if failure.code == -7 && failure.message == "native failure")),
                    _ => assert!(matches!(
                        deleted,
                        Err(ImageStoreError::Common(FurpcClientError::Protocol(
                            "invalid native upload failure code"
                        )))
                    )),
                }

                let mut headers = Headers::new();
                for header in wire::headers([3; 16]).unwrap() {
                    headers.append(header).unwrap();
                }
                headers
                    .append(header::actor(ActorId::new(27).unwrap()).unwrap())
                    .unwrap();
                headers
                    .append(header::action("ClientOperationFailure").unwrap())
                    .unwrap();
                let acknowledged =
                    furpc::codec::decode(&mut AckCodec(headers.clone()), &body, body.len())
                        .map_err(decode_error);
                if code == 0 {
                    assert!(matches!(
                        acknowledged,
                        Err(ImageStoreError::Common(FurpcClientError::Protocol(
                            "invalid native upload failure code"
                        )))
                    ));
                } else {
                    let failure = acknowledged.unwrap().result.unwrap_err();
                    assert_eq!(
                        (failure.code, failure.message.as_str()),
                        (code, "native failure")
                    );
                }
                let malformed = Body::from_bytes(vec![0xff]).unwrap();
                assert!(matches!(
                    furpc::codec::decode(&mut AckCodec(headers), &malformed, 1)
                        .map_err(decode_error),
                    Err(ImageStoreError::Common(FurpcClientError::Decode(_)))
                ));
            }
        }
    }
}
