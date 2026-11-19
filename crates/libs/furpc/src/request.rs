use crate::{HeaderRecord, Headers, Peer, Result, Source};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Weak,
};
use tokio::time::Instant;

#[derive(Debug)]
pub struct RequestContext {
    pub(crate) source: Source,
    pub(crate) peer: Option<Peer>,
    pub(crate) deadline: Instant,
    pub(crate) live: Weak<AtomicBool>,
}
impl RequestContext {
    pub fn source(&self) -> &Source {
        &self.source
    }
    pub fn peer(&self) -> Option<&Peer> {
        self.peer.as_ref()
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(crate) fn is_live(&self) -> bool {
        self.live
            .upgrade()
            .is_some_and(|live| live.load(Ordering::Acquire))
    }
}

#[derive(Debug)]
pub struct RequestParts {
    pub headers: Headers,
    pub deadline: Option<Instant>,
    pub(crate) context: Option<RequestContext>,
}
impl RequestParts {
    pub fn context(&self) -> Option<&RequestContext> {
        self.context.as_ref()
    }
}

#[derive(Debug)]
pub struct Request<B> {
    pub(crate) parts: RequestParts,
    body: B,
}
impl<B> Request<B> {
    pub fn new(body: B) -> Self {
        Self {
            parts: RequestParts {
                headers: Headers::new(),
                deadline: None,
                context: None,
            },
            body,
        }
    }
    pub fn append_header(mut self, header: HeaderRecord) -> Result<Self> {
        self.parts.headers.append(header)?;
        Ok(self)
    }
    pub fn headers(&self) -> &Headers {
        &self.parts.headers
    }
    pub fn headers_mut(&mut self) -> &mut Headers {
        &mut self.parts.headers
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.parts.deadline
    }
    /// Sets outbound call metadata, never the trusted received-context deadline.
    pub fn set_deadline(&mut self, deadline: Instant) {
        self.parts.deadline = Some(deadline);
    }
    pub fn context(&self) -> Option<&RequestContext> {
        self.parts.context.as_ref()
    }
    pub fn body(&self) -> &B {
        &self.body
    }
    pub fn body_mut(&mut self) -> &mut B {
        &mut self.body
    }
    pub fn into_body(self) -> B {
        self.body
    }
    pub fn into_parts(self) -> (RequestParts, B) {
        (self.parts, self.body)
    }
    pub fn from_parts(parts: RequestParts, body: B) -> Self {
        Self { parts, body }
    }
    pub fn map<T>(self, f: impl FnOnce(B) -> T) -> Request<T> {
        Request {
            parts: self.parts,
            body: f(self.body),
        }
    }
}
