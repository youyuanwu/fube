//! Original-Link sends and original-invocation typed handoffs, not domain completion.
use super::{
    bootstrap::{decode_body, PreparedBodyCodec, PreparedEncoder},
    bounded, deadline, err, fm_intake,
    fm_server::FmReport,
    outbound, peer_intake,
    session::OneWaySlot,
    Headers, Link, Message, Properties, Result, Role, LIMIT,
};
use fube_ra::protocol::{FailoverUnitReply, GenerationUpdate};
use furpc::{
    Body, Codec, CodecError, DecodeBuf, Decoder, Error, OneWayClient, PeerSendService, Request,
};
use serde::de::DeserializeOwned;
use std::{
    future::{ready, Ready},
    marker::PhantomData,
    task::{Context, Poll},
};
use tokio::sync::oneshot;
use tower_service::Service;

pub(super) async fn send_prepared(
    link: &Link,
    client: &mut OneWayClient<PeerSendService>,
    message: Message,
) -> Result<()> {
    let message = link.seal(message)?;
    if !link.is_live() {
        return Err(err(Error::Closed));
    }
    outbound(link.session.role(), &message, false).map_err(err)?;
    let mut request = Request::new(message.body().clone());
    *request.headers_mut() = message.headers().clone();
    client
        .send(request, PreparedBodyCodec, deadline())
        .await
        .map(|_| ())
        .map_err(err)
}

pub(super) struct Budgeted<T>(PhantomData<fn() -> T>);

impl<T> Default for Budgeted<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: DeserializeOwned> Decoder for Budgeted<T> {
    type Item = T;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> std::result::Result<T, CodecError> {
        decode_body(src, 4 * LIMIT)
    }
}

pub(super) fn decode_error(error: CodecError) -> String {
    let Some(source) = std::error::Error::source(&error) else {
        return err(error);
    };
    if let Some(error) = source.downcast_ref::<fubst::Error>() {
        err(error)
    } else if let Some(error) = source.downcast_ref::<Error>() {
        err(error)
    } else {
        source.to_string()
    }
}

type Decode<T> = fn(&Headers, &mut DecodeBuf<'_>) -> std::result::Result<T, CodecError>;

struct IntakeCodec<T> {
    headers: Headers,
    decode: Decode<T>,
}

impl<T> Codec for IntakeCodec<T> {
    type Encode = Body;
    type Decode = T;
    type Encoder = PreparedEncoder;
    type Decoder = Self;

    fn encoder(&mut self) -> Self::Encoder {
        PreparedEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        Self {
            headers: self.headers.clone(),
            decode: self.decode,
        }
    }
}

impl<T> Decoder for IntakeCodec<T> {
    type Item = T;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> std::result::Result<T, CodecError> {
        (self.decode)(&self.headers, src)
    }
}

pub(super) fn decode_message<T>(message: &Message, decode: Decode<T>) -> Result<T> {
    bounded(message).map_err(err)?;
    furpc::codec::decode(
        &mut IntakeCodec {
            headers: message.headers().clone(),
            decode,
        },
        message.body(),
        LIMIT,
    )
    .map_err(decode_error)
}

#[derive(Clone)]
pub(in crate::control) enum Call {
    Fm(fm_intake::Incoming),
    Report(FmReport),
    Peer(peer_intake::Incoming),
    GenerationUpdate(peer_intake::Payload<GenerationUpdate>),
    Deprecated(peer_intake::Payload<FailoverUnitReply>),
}

pub(in crate::control) struct Prepared {
    pub headers: Headers,
    pub action: String,
    pub call: Call,
    pub source_generation: u64,
}

fn node(headers: &Headers, src: &mut DecodeBuf<'_>) -> std::result::Result<Call, CodecError> {
    let action = Properties::from_headers(headers)
        .map_err(CodecError::invalid_payload)?
        .action;
    match action.as_str() {
        "GenerationUpdate" => peer_intake::Payload::decode_parts(headers, src, "GenerationUpdate")
            .map(Call::GenerationUpdate),
        "ChangeConfigurationReply" => {
            peer_intake::Payload::decode_parts(headers, src, "ChangeConfigurationReply")
                .map(Call::Deprecated)
        }
        _ => fm_intake::Incoming::decode_parts(headers, src).map(|(_, call)| Call::Fm(call)),
    }
}

fn peer(headers: &Headers, src: &mut DecodeBuf<'_>) -> std::result::Result<Call, CodecError> {
    peer_intake::Incoming::decode_parts(headers, src).map(|(_, call)| Call::Peer(call))
}

fn fm(headers: &Headers, src: &mut DecodeBuf<'_>) -> std::result::Result<Call, CodecError> {
    FmReport::decode_parts(headers, src).map(Call::Report)
}

struct Capture(Option<oneshot::Sender<Prepared>>);

impl Service<Request<Call>> for Capture {
    type Response = ();
    type Error = Error;
    type Future = Ready<furpc::Result<()>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Call>) -> Self::Future {
        let context = request
            .context()
            .map(|context| context.source().generation())
            .ok_or(Error::Internal("one-way handoff missing received context"));
        let action = Properties::from_headers(request.headers()).map(|props| props.action);
        let (parts, call) = request.into_parts();
        ready(action.and_then(|action| {
            let source_generation = context?;
            self.0
                .take()
                .ok_or(Error::Internal("one-way handoff already called"))?
                .send(Prepared {
                    headers: parts.headers,
                    action,
                    call,
                    source_generation,
                })
                .map_err(|_| Error::Closed)
        }))
    }
}

impl OneWaySlot {
    pub(super) async fn prepare(self, link: &Link) -> Result<Prepared> {
        let result = async {
            if !link.is_live() {
                return Err(Error::Closed);
            }
            let context = self
                .request
                .context()
                .ok_or(Error::Protocol("one-way missing original request context"))?;
            if context.source().generation() != link.session.peer.info().generation {
                return Err(Error::Protocol("foreign Channel source capability"));
            }
            super::bounded_parts(self.request.headers(), self.request.body()).map_err(
                |source| Error::Codec {
                    phase: "one-way admission",
                    source: CodecError::invalid_payload(source),
                },
            )?;
            let decode = match self.role {
                Role::Node => node,
                Role::Peer => peer,
                Role::Fm => fm,
            };
            let codec = IntakeCodec {
                headers: self.request.headers().clone(),
                decode,
            };
            let (send, receive) = oneshot::channel();
            let response = furpc::server::Rpc::new(codec)
                .one_way(Capture(Some(send)), self.request)
                .await?;
            let prepared = receive.await.map_err(|_| Error::Closed)?;
            Ok((response, prepared))
        }
        .await;
        match result {
            Ok((response, prepared)) => {
                self.response
                    .send(Ok(response))
                    .map_err(|_| err(Error::Closed))?;
                Ok(prepared)
            }
            Err(error) => {
                let cause = match &error {
                    Error::Codec { source, .. } => decode_error(source.clone()),
                    _ => err(&error),
                };
                // The primary decode/invocation error survives a retired receiver.
                let _ = self.response.send(Err(error));
                Err(cause)
            }
        }
    }
}

pub(super) fn discard(
    headers: &Headers,
    src: &mut DecodeBuf<'_>,
) -> std::result::Result<(), CodecError> {
    use fube_ra::protocol::*;
    let action = Properties::from_headers(headers)
        .map_err(CodecError::invalid_payload)?
        .action;
    macro_rules! body {
        ($ty:ty) => {
            decode_body::<$ty>(src, 4 * LIMIT).map(drop)
        };
    }
    match action.as_str() {
        "GetLSN" => body!(GetLsn),
        "GetLSNReply" => body!(GetLsnReply),
        "ReplicaEndpointUpdated"
        | "AddInstance"
        | "AddPrimary"
        | "AddReplica"
        | "RemoveReplica" => body!(AddInstance),
        "ReplicaEndpointUpdatedReply"
        | "AddInstanceReply"
        | "AddPrimaryReply"
        | "AddReplicaReply"
        | "RemoveReplicaReply"
        | "RemoveInstanceReply"
        | "DeleteReplicaReply"
        | "CreateReplicaReply" => body!(AddInstanceReply),
        "DoReconfiguration" => body!(DoReconfiguration),
        "DoReconfigurationReply" => body!(ConfigurationReply),
        "ChangeConfiguration" => body!(Configuration),
        "ChangeConfigurationReply" => body!(FailoverUnitReply),
        "Activate" => body!(Activate),
        "ActivateReply" => body!(ActivateReply),
        "Deactivate" => body!(Deactivate),
        "DeactivateReply" => body!(DeactivateReply),
        "GenerationProposal" | "LFUMUploadReply" if src.is_empty() => Ok(()),
        "GenerationProposal" => Err(CodecError::invalid_payload(
            "GenerationProposal must be bodyless",
        )),
        "LFUMUploadReply" => Err(CodecError::invalid_payload(
            "LFUMUploadReply must be bodyless",
        )),
        "LFUMUpload" => body!(LfumUpload),
        "GenerationProposalReply" => body!(GenerationProposalReply),
        "GenerationUpdate" => body!(GenerationUpdate),
        "GenerationUpdateReject" => body!(GenerationNumber),
        "NodeUp" => body!(NodeUp),
        "NodeUpAck" => body!(NodeUpAck),
        "CreateReplica" => body!(CreateReplica),
        "RemoveInstance" | "DeleteReplica" => body!(RemoveInstance),
        "ReplicaUp" | "ReplicaUpReply" => body!(ReplicaUp),
        "ServiceTypeNotification" | "ServiceTypeNotificationReply" => {
            body!(ServiceTypeNotification)
        }
        "NodeActivateRequest" | "NodeDeactivateRequest" => body!(NodeActivationRequest),
        "NodeActivateReply" | "NodeDeactivateReply" => body!(NodeActivationReply),
        "NodeUpdateServiceRequest" => body!(NodeUpdateServiceRequest),
        "NodeUpdateServiceReply" => body!(NodeUpdateServiceReply),
        "ServiceTypeEnabled" | "ServiceTypeDisabled" => body!(ServiceTypeUpdate),
        "ServiceTypeEnabledReply" | "ServiceTypeDisabledReply" => body!(ServiceTypeUpdateReply),
        _ => Err(CodecError::invalid_payload(
            "unsupported native action retained at boundary",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{deadline, tests::links};
    use super::*;
    use fube_ra::protocol::{
        change_configuration_reply_message, decode_change_configuration_reply,
        service_type_enabled_reply_message, ServiceTypeUpdateReply,
    };
    use tokio::time::{sleep_until, Duration};

    #[tokio::test]
    async fn original_one_way_context_rejects_retired_and_expired_decode_handoffs() {
        for receiving_fm in [false, true] {
            for expired in [false, true] {
                let (node, fm) = links(false).await;
                let original = if receiving_fm {
                    fube_ra::protocol::service_type_enabled_message(
                        &fube_ra::protocol::ServiceTypeUpdate::default(),
                    )
                } else {
                    service_type_enabled_reply_message(
                        &ServiceTypeUpdateReply { sequence: 41 },
                        &node.generation(),
                    )
                }
                .unwrap();
                let (receiver, sender) = if receiving_fm { (fm, node) } else { (node, fm) };
                let mut builder = Message::builder().body(Body::from_bytes(vec![0xff]).unwrap());
                for header in original.headers().records() {
                    builder = builder.append_header(header.to_owned());
                }
                sender.send(builder.build().unwrap()).await.unwrap();
                let mut incoming = receiver.session.receive(deadline()).await.unwrap().unwrap();
                let slot = incoming.one_way.take().unwrap();
                if expired {
                    sleep_until(
                        slot.request.context().unwrap().deadline() + Duration::from_millis(20),
                    )
                    .await;
                } else {
                    receiver.retire();
                }
                let error = slot.prepare(&receiver).await.err().unwrap();
                assert!(
                    error.contains("Closed") || error.contains("Timeout"),
                    "{error}"
                );
                receiver.close().await.unwrap();
                sender.close().await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn deprecated_reply_handoff_keeps_original_body_and_headers_after_view_mutation() {
        let (node, fm) = links(false).await;
        let original = change_configuration_reply_message(
            &FailoverUnitReply {
                error: -7,
                ..Default::default()
            },
            &node.generation(),
        )
        .unwrap();
        let expected = decode_change_configuration_reply(&original).unwrap();
        fm.send(original).await.unwrap();
        let mut incoming = node.receive_checked(deadline()).await.unwrap().unwrap();
        incoming.message = Message::builder()
            .body(Body::from_bytes(vec![0xff]).unwrap())
            .build()
            .unwrap();
        for _ in 0..2 {
            assert_eq!(incoming.action().unwrap(), "ChangeConfigurationReply");
            let Call::Deprecated(payload) = &incoming.prepared().unwrap().call else {
                panic!("expected an original deprecated-reply handoff");
            };
            assert_eq!(payload.clone().into_parts().unwrap(), expected);
        }
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }
}
