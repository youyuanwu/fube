//! Deferred typed handlers; the caller keeps domain state and commit policy.
use super::{
    bounded_parts, decode_body, err, unique, Body, CodecError, DecodeBuf, Decoder, Error,
    GenerationHeader, GenerationProposalReply, LfumUpload, Link, Message, Method, NodeUp,
    NodeUpAck, PreparedEncoder, Properties, Request, Result, FM_ACTOR, GENERATION_HEADER, LIMIT,
    RA_ACTOR,
};
use crate::control::wire::session::ResponseSlot;
use fube_ra::protocol::{
    generation_proposal_reply_message, lfum_upload_reply_message, node_up_ack_message,
};
use furpc::{header, HeaderId, Reply};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::oneshot;
use tower_service::Service;

enum Call {
    NodeUp(NodeUp),
    LfumUpload(LfumUpload),
    GenerationProposal(GenerationHeader),
}

struct ServerDecoder {
    method: Method,
    budget: usize,
    generation: Option<GenerationHeader>,
}

impl ServerDecoder {
    fn admitted(method: Method, link: &Link, request: &Request<Body>) -> Result<Self> {
        let context = request
            .context()
            .ok_or("bootstrap missing original request context")?;
        if context.source().generation() != link.session.peer.info().generation {
            return Err("foreign Channel source capability".into());
        }
        bounded_parts(request.headers(), request.body()).map_err(err)?;
        let headers = request.headers();
        let props = Properties::from_headers(headers).map_err(err)?;
        let expected = match method {
            Method::NodeUp => (FM_ACTOR, "NodeUp"),
            Method::LfumUpload => (FM_ACTOR, "LFUMUpload"),
            Method::GenerationProposal => (RA_ACTOR, "GenerationProposal"),
        };
        if (props.actor, props.action.as_str()) != expected
            || !props.expects_reply
            || props.uncorrelated_reply
            || !props.high_priority
        {
            return Err("unsupported bootstrap invocation".into());
        }
        let action = unique(headers, header::ACTION).map_err(err)?;
        unique(headers, header::ACTOR).map_err(err)?;
        unique(headers, header::HIGH_PRIORITY).map_err(err)?;
        if !link.is_current_parts(headers, request.body())? {
            return Err("foreign/stale/duplicate native FM generation".into());
        }
        let mut budget = 4 * LIMIT;
        if method == Method::NodeUp {
            budget = budget
                .checked_sub(action.value().len().saturating_mul(2))
                .ok_or("bootstrap decode allocation")?;
        }
        let generation = match method {
            Method::GenerationProposal => {
                if !request.body().is_empty() {
                    return Err("GenerationProposal must be bodyless".into());
                }
                Some(super::super::decode(
                    unique(headers, HeaderId::new(GENERATION_HEADER).map_err(err)?)
                        .map_err(err)?
                        .value(),
                )?)
            }
            Method::LfumUpload => {
                if headers
                    .records()
                    .any(|header| header.id().get() == GENERATION_HEADER)
                {
                    return Err("unexpected generation negotiation header".into());
                }
                None
            }
            Method::NodeUp => None,
        };
        Ok(Self {
            method,
            budget,
            generation,
        })
    }
}

impl Decoder for ServerDecoder {
    type Item = Call;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> std::result::Result<Call, CodecError> {
        match self.method {
            Method::NodeUp => decode_body(src, self.budget).map(Call::NodeUp),
            Method::LfumUpload => decode_body(src, self.budget).map(Call::LfumUpload),
            Method::GenerationProposal => {
                if !src.is_empty() {
                    return Err(CodecError::invalid_payload(
                        "GenerationProposal must be bodyless",
                    ));
                }
                self.generation
                    .take()
                    .map(Call::GenerationProposal)
                    .ok_or_else(|| {
                        CodecError::invalid_payload("GenerationProposal missing generation")
                    })
            }
        }
    }
}

struct Handoff {
    call: Option<oneshot::Sender<Call>>,
    reply: Option<oneshot::Receiver<Result<Reply<Body>>>>,
}

impl Service<Request<Call>> for Handoff {
    type Response = Reply<Body>;
    type Error = String;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Call>) -> Self::Future {
        let prepared = (|| {
            self.call
                .take()
                .ok_or("bootstrap handler already called")?
                .send(request.into_body())
                .map_err(|_| err(Error::Closed))?;
            self.reply
                .take()
                .ok_or_else(|| "bootstrap handler missing reply".to_owned())
        })();
        Box::pin(async move { prepared?.await.map_err(|_| err(Error::Closed))? })
    }
}

impl ResponseSlot {
    pub async fn node_up(
        self,
        link: &Link,
        handler: impl FnOnce(NodeUp) -> Result<(NodeUpAck, GenerationHeader)> + Send,
    ) -> Result<(NodeUpAck, GenerationHeader)> {
        self.unary(
            link,
            Method::NodeUp,
            |call| match call {
                Call::NodeUp(body) => handler(body),
                _ => Err("mismatched NodeUp invocation".into()),
            },
            |reply| node_up_ack_message(&reply.0, &reply.1).map_err(err),
        )
        .await
    }

    pub async fn lfum_upload(
        self,
        link: &Link,
        handler: impl FnOnce(LfumUpload) -> Result<GenerationHeader> + Send,
    ) -> Result<GenerationHeader> {
        self.unary(
            link,
            Method::LfumUpload,
            |call| match call {
                Call::LfumUpload(body) => handler(body),
                _ => Err("mismatched LFUMUpload invocation".into()),
            },
            |reply| lfum_upload_reply_message(reply).map_err(err),
        )
        .await
    }

    pub async fn generation_proposal(
        self,
        link: &Link,
        handler: impl FnOnce(GenerationHeader) -> Result<GenerationProposalReply> + Send,
    ) -> Result<GenerationProposalReply> {
        self.unary(
            link,
            Method::GenerationProposal,
            |call| match call {
                Call::GenerationProposal(header) => handler(header),
                _ => Err("mismatched GenerationProposal invocation".into()),
            },
            |reply| generation_proposal_reply_message(reply).map_err(err),
        )
        .await
    }

    async fn unary<R: Send>(
        self,
        link: &Link,
        method: Method,
        handler: impl FnOnce(Call) -> Result<R> + Send,
        prepare: impl FnOnce(&R) -> Result<Message> + Send,
    ) -> Result<R> {
        let decoder = match ServerDecoder::admitted(method, link, &self.request) {
            Ok(decoder) => decoder,
            Err(source) => {
                let error = Error::Codec {
                    phase: "bootstrap admission",
                    source: CodecError::invalid_payload(source.clone()),
                };
                // Retain the admission cause even if its original runner retired.
                let _ = self.response.send(Err(error));
                return Err(source);
            }
        };
        let (send, receive) = oneshot::channel();
        let (reply, response) = oneshot::channel();
        let service = Handoff {
            call: Some(send),
            reply: Some(response),
        };
        // Co-poll the typed service and borrowed domain handler, never spawn a task.
        let (encoded, handled) = tokio::join!(
            furpc::server::unary(service, self.request, decoder, PreparedEncoder),
            async {
                let call = receive.await.map_err(|_| err(Error::Closed))?;
                let result = handler(call);
                let prepared = result
                    .as_ref()
                    .map_err(Clone::clone)
                    .and_then(prepare)
                    .and_then(|message| link.seal(message))
                    .and_then(|message| {
                        let mut reply = Reply::new(message.body().clone());
                        for header in message.headers().records() {
                            reply = reply.append_header(header.to_owned()).map_err(err)?;
                        }
                        Ok(reply)
                    });
                reply.send(prepared).map_err(|_| err(Error::Closed))?;
                result
            }
        );
        let error = encoded.as_ref().err().map(err);
        let sent = self.response.send(encoded).map_err(|_| err(Error::Closed));
        if let Some(error) = error {
            return Err(error);
        }
        sent?;
        handled
    }
}
