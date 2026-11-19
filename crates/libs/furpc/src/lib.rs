//! Service Fabric native RPC transport.
//!
//! Shared native messages, one-way sends, and managed byte RPC over clear transport.
//!
//! [`Endpoint`] establishes an owning [`Channel`]; its [`Peer`] sends on one
//! generation. [`Server::serve_with_shutdown`] runs accepted I/O with the same
//! driver and canonical [`IngressService`] execution. Managed correlation is the
//! default; [`ConnectionOptions::raw_only`] keeps caller-owned one-way IDs.
//! [`RpcClient`] adapts that byte service with a consumed application [`Codec`]
//! per call. [`server::Rpc`] adapts typed handlers inside byte-facing services.
//! [`Router`] optionally selects exact native Actor/Action services; raw ingress
//! remains independent. TLS is not implemented.
//!
//! ```no_run
//! use furpc::{Body, ConnectionOptions, Endpoint, Message};
//! use tokio::time::{Duration, Instant};
//!
//! # async fn example() -> furpc::Result<()> {
//! let channel = Endpoint::from_shared("tcp://127.0.0.1:19000")?
//!     .connection_options(ConnectionOptions::raw_only())
//!     .connect().await?;
//! let deadline = Instant::now() + Duration::from_secs(5);
//! let peer = channel.peer(deadline).await?;
//! let message = Message::builder().body(Body::from_bytes(vec![1, 2, 3])?).build()?;
//! peer.send(message, deadline).await?; // Complete local write, not a remote ACK.
//! # Ok(())
//! # }
//! ```
//!
//! Typed clients select a body codec for each call:
//!
//! ```no_run
//! use furpc::{Endpoint, FubstCodec, Request, RpcClient};
//! use serde::{Deserialize, Serialize};
//! use tokio::time::{Duration, Instant};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Echo { text: String }
//!
//! # async fn example() -> furpc::Result<()> {
//! let channel = Endpoint::from_shared("tcp://127.0.0.1:19000")?.connect().await?;
//! let mut rpc = RpcClient::new(channel);
//! let response = rpc.call(
//!     Request::new(Echo { text: "hello".into() }),
//!     FubstCodec::<Echo, Echo>::default(),
//!     Instant::now() + Duration::from_secs(5),
//! ).await?;
//! assert_eq!(response.body().text, "hello");
//! # Ok(())
//! # }
//! ```
//!
//! Byte RPC keeps application bodies opaque:
//!
//! ```no_run
//! use furpc::{Body, Endpoint, Request};
//! use tower_service::Service;
//! use tokio::time::{Duration, Instant};
//!
//! # async fn example() -> furpc::Result<()> {
//! let mut channel = Endpoint::from_shared("tcp://127.0.0.1:19000")?.connect().await?;
//! let mut request = Request::new(Body::from_bytes(vec![1, 2, 3])?);
//! request.set_deadline(Instant::now() + Duration::from_secs(5));
//! std::future::poll_fn(|cx| channel.poll_ready(cx)).await?;
//! let response = channel.call(request).await?;
//! # let _ = response;
//! # Ok(())
//! # }
//! ```

mod body;
mod client;
pub mod codec;
mod connection;
mod error;
pub mod header;
mod message;
mod operation;
mod peer;
mod reply;
mod request;
mod response;
mod routing;
mod rpc;
pub mod server;
mod service;
mod transport;
mod wire;

pub use client::{ClientCodecLimits, RpcClient};
pub use codec::{Codec, CodecError, DecodeBuf, Decoder, EncodeBuf, Encoder, FubstCodec};
pub use error::{BoxError, Error, ErrorSource, Result};
pub use header::{ActorId, CommonHeaders, HeaderId, HeaderRecord, HeaderRef, Headers};
pub use message::{Body, Message, MessageBuilder};
pub use operation::{Attempt, AttemptDisposition, CancelOutcome, Operation, WriteReceipt};
pub use peer::{Peer, PeerInfo, SendCheck, SendKind, SendPolicy, Source};
pub use reply::Reply;
pub use request::{Request, RequestContext, RequestParts};
pub use response::{Response, ResponseParts};
pub use routing::{OneWayRouteOptions, RequestRouteOptions, Router};
pub use rpc::RemoteFault;
pub use service::{
    EnvelopeCheck, HandlerLimits, IngressLimits, IngressOptions, IngressResponse, IngressService,
    RequestPolicy, ServiceBinding,
};
pub use transport::{
    Accept, Accepted, Channel, Connected, ConnectionOptions, Endpoint, IoInfo, Server,
    ServerBuilder, StreamAcceptor, TcpAcceptor, TcpConnector, TransportIo, TransportLimits,
};
pub use wire::headers::{Fault, MessageId};
