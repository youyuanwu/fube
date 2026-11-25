//! Original-Link sends and original-invocation typed handoffs, not domain completion.
use super::{
    bootstrap::decode_body, bounded, deadline, err, fm_intake, fm_server::FmReport,
    outbound_headers, peer_intake, session::OneWaySlot, Headers, Link, Message, Properties, Result,
    Role, LIMIT,
};
use fube_ra::protocol::{FailoverUnitReply, GenerationHeader, GenerationUpdate};
use furpc::{
    header, ActorId, CodecError, DecodeBuf, Decoder, Error, FubstEncoder, HeaderRecord,
    OneWayClient, PeerSendService, Request,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    future::{ready, Ready},
    marker::PhantomData,
    task::{Context, Poll},
};
use tokio::sync::oneshot;
use tower_service::Service;

pub(super) fn request<T>(
    body: T,
    action: &str,
    actor: i64,
    high_priority: bool,
    generation: Option<&GenerationHeader>,
) -> Result<Request<T>> {
    let mut request = Request::new(body);
    *request.headers_mut() = headers(action, actor, high_priority, generation)?;
    Ok(request)
}

pub(super) fn headers(
    action: &str,
    actor: i64,
    high_priority: bool,
    generation: Option<&GenerationHeader>,
) -> Result<Headers> {
    #[derive(Serialize)]
    struct Priority {
        value: bool,
    }
    let mut headers = Headers::new();
    for header in [
        header::actor(ActorId::new(actor).map_err(err)?).map_err(err)?,
        header::action(action).map_err(err)?,
        HeaderRecord::native(
            header::HIGH_PRIORITY,
            &Priority {
                value: high_priority,
            },
        )
        .map_err(err)?,
    ] {
        headers.append(header).map_err(err)?;
    }
    if let Some(generation) = generation {
        headers
            .append(generation.header().map_err(err)?)
            .map_err(err)?;
    }
    Ok(headers)
}

pub(super) async fn send<T: Serialize + Send>(
    link: &Link,
    client: &mut OneWayClient<PeerSendService>,
    mut request: Request<T>,
) -> Result<()> {
    link.seal_headers(request.headers_mut())?;
    if !link.is_live() {
        return Err(err(Error::Closed));
    }
    outbound_headers(link.session.role(), request.headers(), false).map_err(err)?;
    client
        .send(request, FubstEncoder::<T>::default(), deadline())
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

struct IntakeDecoder<T> {
    headers: Headers,
    decode: Decode<T>,
}

impl<T> Decoder for IntakeDecoder<T> {
    type Item = T;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> std::result::Result<T, CodecError> {
        (self.decode)(&self.headers, src)
    }
}

pub(super) fn decode_message<T>(message: &Message, decode: Decode<T>) -> Result<T> {
    bounded(message).map_err(err)?;
    furpc::codec::decode(
        &mut IntakeDecoder {
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
            let decoder = IntakeDecoder {
                headers: self.request.headers().clone(),
                decode,
            };
            let (send, receive) = oneshot::channel();
            let response =
                furpc::server::one_way(Capture(Some(send)), self.request, decoder).await?;
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
    use furpc::Body;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::time::{sleep_until, Duration};

    struct Counted {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    impl Serialize for Counted {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(serde::ser::Error::custom(
                    "injected outbound encode failure",
                ));
            }
            fube_ra::protocol::NodeDeactivateReply { sequence: 42 }.serialize(serializer)
        }
    }

    #[tokio::test]
    async fn typed_send_encodes_only_when_polled_with_an_unexpired_deadline() {
        use fube_ra::protocol::{decode_node_deactivate_reply, Owner, FM_ACTOR};

        let (sender, receiver) = links(false).await;
        let mut client = OneWayClient::new(PeerSendService::new(sender.session.peer.clone()))
            .max_encoded_body_bytes(LIMIT)
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let body = Counted {
            calls: calls.clone(),
            fail: false,
        };
        let make_request = || request(&body, "NodeDeactivateReply", FM_ACTOR, false, None).unwrap();
        let prepared = make_request();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(send(&sender, &mut client, prepared));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let mut expired = make_request();
        expired.set_deadline(tokio::time::Instant::now() - Duration::from_secs(1));
        assert!(send(&sender, &mut client, expired)
            .await
            .unwrap_err()
            .contains("Timeout"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(receiver
            .receive(tokio::time::Instant::now() + Duration::from_millis(5))
            .await
            .unwrap()
            .is_none());

        send(&sender, &mut client, make_request()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let incoming = receiver.receive_checked(deadline()).await.unwrap().unwrap();
        assert_eq!(
            decode_node_deactivate_reply(&incoming.message).unwrap(),
            (
                fube_ra::protocol::NodeDeactivateReply { sequence: 42 },
                Owner::Fm,
            )
        );
        sender.close().await.unwrap();
        receiver.close().await.unwrap();
    }

    #[tokio::test]
    async fn typed_send_guards_encoding_and_never_submits_failed_serialization() {
        use fube_ra::protocol::{FM_ACTOR, RA_ACTOR};

        let (sender, receiver) = links(false).await;
        let mut client = OneWayClient::new(PeerSendService::new(sender.session.peer.clone()))
            .max_encoded_body_bytes(LIMIT)
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut body = Counted {
            calls: calls.clone(),
            fail: false,
        };
        let invalid = request(&body, "GetLSN", RA_ACTOR, true, Some(&sender.generation())).unwrap();
        assert!(send(&sender, &mut client, invalid).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        body.fail = true;
        let invalid = request(&body, "NodeDeactivateReply", FM_ACTOR, false, None).unwrap();
        let error = send(&sender, &mut client, invalid).await.unwrap_err();
        assert!(error.contains("phase: \"encode\""), "{error}");
        assert!(error.contains("InvalidPayload"), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(sender.is_live());
        assert!(receiver
            .receive(tokio::time::Instant::now() + Duration::from_millis(5))
            .await
            .unwrap()
            .is_none());

        body.fail = false;
        let valid = request(&body, "NodeDeactivateReply", FM_ACTOR, false, None).unwrap();
        send(&sender, &mut client, valid).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        receiver.receive_checked(deadline()).await.unwrap().unwrap();

        sender.retire();
        let retired = request(&body, "NodeDeactivateReply", FM_ACTOR, false, None).unwrap();
        assert!(send(&sender, &mut client, retired)
            .await
            .unwrap_err()
            .contains("Closed"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        sender.close().await.unwrap();
        receiver.close().await.unwrap();
    }

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
