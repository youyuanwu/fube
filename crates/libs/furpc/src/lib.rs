//! Service Fabric native RPC transport.
//!
//! Shared native messages and explicit raw-only, one-way clear transport.
//!
//! [`Endpoint`] establishes an owning [`Channel`]; its [`Peer`] sends on one
//! generation. [`Server::serve_with_shutdown`] runs accepted I/O with the same
//! driver and canonical [`IngressService`] execution. Select
//! [`ConnectionOptions::raw_only`] explicitly; managed RPC, typed body codecs,
//! Actor routing and TLS are not implemented.
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

mod body;
mod connection;
mod error;
pub mod header;
mod message;
mod operation;
mod peer;
mod request;
mod service;
mod transport;
mod wire;

pub use error::{BoxError, Error, ErrorSource, Result};
pub use header::{ActorId, CommonHeaders, HeaderId, HeaderRecord, HeaderRef, Headers};
pub use message::{Body, Message, MessageBuilder};
pub use operation::{Attempt, AttemptDisposition, CancelOutcome, Operation, WriteReceipt};
pub use peer::{Peer, PeerInfo, SendCheck, SendKind, SendPolicy, Source};
pub use request::{Request, RequestContext, RequestParts};
pub use service::{
    EnvelopeCheck, HandlerLimits, IngressLimits, IngressOptions, IngressResponse, IngressService,
    ServiceBinding,
};
pub use transport::{
    Accept, Accepted, Channel, Connected, ConnectionOptions, Endpoint, IoInfo, Server,
    ServerBuilder, StreamAcceptor, TcpAcceptor, TcpConnector, TransportIo, TransportLimits,
};
pub use wire::headers::{Fault, MessageId};
