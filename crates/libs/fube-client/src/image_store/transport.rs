//! Typed original-Peer methods; upload stages and semantic completion stay in the owner.
use super::{wire, FurpcClientError, ImageStoreError, Result, UploadState};
use furpc::{
    header, ActorId, Body, CodecError, DecodeBuf, EncodeBuf, Encoder, FubstEncoder, OneWayClient,
    Peer, PeerRequestService, PeerSendService, Request, Response, RpcClient,
};
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::time::Instant;
use tower_service::Service;

pub(super) enum UploadRequest<'a> {
    Create { path: &'a str, size: u64 },
    Content { number: u64, bytes: Vec<u8> },
    Commit,
    CommitAck,
    DeleteSession,
}

impl UploadRequest<'_> {
    fn prepare(self, activity: [u8; 16], deadline: Instant) -> Result<Request<Self>> {
        let mut headers = wire::headers(activity)?;
        let action = match &self {
            Self::Create { path, size } => {
                headers.push(wire::create(path, *size)?);
                "FileCreate"
            }
            Self::Content { number, bytes } => {
                headers.push(wire::sequence(*number, bytes.len() as u64)?);
                "FileContent"
            }
            Self::Commit => "FileUploadCommit",
            Self::CommitAck => "FileUploadCommitAck",
            Self::DeleteSession => "FileUploadDeleteSession",
        };
        headers.extend([
            header::actor(ActorId::new(26)?)?,
            header::action(action)?,
            wire::timeout(deadline)?,
        ]);
        let mut request = Request::new(self);
        for header in headers {
            request = request.append_header(header)?;
        }
        Ok(request)
    }
}

struct UploadEncoder<'a>(PhantomData<&'a str>);

impl<'a> Encoder for UploadEncoder<'a> {
    type Item = UploadRequest<'a>;

    fn encode(
        &mut self,
        request: Self::Item,
        dst: &mut EncodeBuf<'_>,
    ) -> std::result::Result<(), CodecError> {
        match request {
            UploadRequest::Content { bytes, .. } => {
                dst.append_body(Body::from_bytes(bytes).map_err(|error| match error {
                    furpc::Error::ResourceLimit(_) => CodecError::resource_limit(error),
                    _ => CodecError::invalid_payload(error),
                })?)
            }
            UploadRequest::Create { .. }
            | UploadRequest::Commit
            | UploadRequest::CommitAck
            | UploadRequest::DeleteSession => Ok(()),
        }
    }
}

pub(super) struct UploadClient {
    requests: RpcClient<DeleteService>,
    sends: OneWayClient<PeerSendService>,
    state: Arc<UploadState>,
}

impl UploadClient {
    pub(super) fn new(peer: Peer, state: Arc<UploadState>) -> Self {
        Self {
            requests: RpcClient::new(DeleteService {
                transport: PeerRequestService::new(peer.clone()),
                state: state.clone(),
            }),
            sends: OneWayClient::new(PeerSendService::new(peer)),
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
        let response = self
            .requests
            .call(
                request,
                FubstEncoder::<wire::Delete<'_>>::default(),
                decode_delete,
                deadline,
            )
            .await
            .map_err(ImageStoreError::from)?;
        Ok(response.into_body())
    }

    pub(super) async fn send(
        &mut self,
        request: UploadRequest<'_>,
        activity: [u8; 16],
        deadline: Instant,
    ) -> Result<()> {
        let request = self.prepare(request, activity, deadline)?;
        self.send_request(request, deadline).await
    }

    pub(super) fn prepare<'a>(
        &self,
        request: UploadRequest<'a>,
        activity: [u8; 16],
        deadline: Instant,
    ) -> Result<Request<UploadRequest<'a>>> {
        self.state.check()?;
        request.prepare(activity, deadline)
    }

    pub(super) async fn send_request(
        &mut self,
        request: Request<UploadRequest<'_>>,
        deadline: Instant,
    ) -> Result<()> {
        self.state.check()?;
        self.sends
            .send(request, UploadEncoder(PhantomData), deadline)
            .await
            .map(|_| ())
            .map_err(ImageStoreError::from)
    }
}

fn decode_delete(src: &mut DecodeBuf<'_>) -> std::result::Result<Option<[u8; 16]>, CodecError> {
    wire::decode_delete(src.headers(), src).map_err(codec_error)
}

pub(super) fn decode_ack(
    src: &mut DecodeBuf<'_>,
) -> std::result::Result<wire::FileTransferReply, CodecError> {
    wire::decode_reply(src.headers(), src).map_err(codec_error)
}

struct DeleteService {
    transport: PeerRequestService,
    state: Arc<UploadState>,
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
        Box::pin(async move {
            let response = future.await?;
            state.check()?;
            Ok(response)
        })
    }
}

pub(super) fn codec_error(error: ImageStoreError) -> CodecError {
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
    use furpc::{DecodeBuf, Headers};
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
    fn typed_delete_encoding_preserves_native_bytes_and_local_error_classification() {
        #[derive(Serialize)]
        struct Expected<'a> {
            path: &'a str,
        }
        let path = "Package/nested";
        let expected = to_vec(&Expected { path }).unwrap();
        let (_, payload) = wire::delete(path, [3; 16]).unwrap();
        let body = furpc::codec::encode(
            &mut FubstEncoder::<wire::Delete<'_>>::default(),
            payload,
            expected.len(),
        )
        .unwrap();
        assert_eq!(body.segments().next().unwrap(), expected);
        let (_, payload) = wire::delete(path, [3; 16]).unwrap();
        let source =
            furpc::codec::encode(&mut FubstEncoder::<wire::Delete<'_>>::default(), payload, 1)
                .unwrap_err();
        let error = ImageStoreError::from(furpc::Error::Codec {
            phase: "encode",
            source,
        });
        assert!(matches!(
            error,
            ImageStoreError::Common(FurpcClientError::Decode(fubst::Error::ResourceLimit(_)))
        ));
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
        let body = furpc::codec::encode(&mut UploadEncoder(PhantomData), request.into_body(), 1024)
            .unwrap();
        assert_eq!(body.len(), 1024);
        assert_eq!(body.segments().next().unwrap().as_ptr(), storage);
    }

    #[test]
    fn delete_and_ack_profiles_preserve_fragmented_native_failures_and_error_classes() {
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
                let deleted = decode_delete(&mut DecodeBuf::with_headers(&body, &headers))
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
                    decode_ack(&mut DecodeBuf::with_headers(&body, &headers)).map_err(decode_error);
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
                    decode_ack(&mut DecodeBuf::with_headers(&malformed, &headers))
                        .map_err(decode_error),
                    Err(ImageStoreError::Common(FurpcClientError::Decode(_)))
                ));
            }
        }
    }

    #[test]
    fn header_dependent_decoder_rejects_empty_standalone_headers() {
        let error = furpc::codec::decode(&mut decode_ack, &Body::empty(), 0)
            .map_err(decode_error)
            .unwrap_err();
        assert!(matches!(
            error,
            ImageStoreError::Common(FurpcClientError::Protocol(
                "invalid native file-transfer envelope"
            ))
        ));
    }
}
