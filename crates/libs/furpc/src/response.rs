use crate::{Headers, Source};
use tokio::time::Instant;

#[derive(Debug)]
pub struct ResponseParts {
    pub headers: Headers,
    source: Option<Source>,
    settled_at: Option<Instant>,
}
impl ResponseParts {
    pub fn source(&self) -> Option<&Source> {
        self.source.as_ref()
    }
}

/// Complete response; synthetic responses have no native source/settlement.
#[derive(Debug)]
pub struct Response<B> {
    parts: ResponseParts,
    body: B,
}
impl<B> Response<B> {
    pub fn new(body: B) -> Self {
        Self {
            parts: ResponseParts {
                headers: Headers::new(),
                source: None,
                settled_at: None,
            },
            body,
        }
    }
    pub(crate) fn received(headers: Headers, body: B, source: Source, settled_at: Instant) -> Self {
        Self {
            parts: ResponseParts {
                headers,
                source: Some(source),
                settled_at: Some(settled_at),
            },
            body,
        }
    }
    pub fn headers(&self) -> &Headers {
        &self.parts.headers
    }
    pub fn headers_mut(&mut self) -> &mut Headers {
        &mut self.parts.headers
    }
    pub fn source(&self) -> Option<&Source> {
        self.parts.source()
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
    pub fn into_parts(self) -> (ResponseParts, B) {
        (self.parts, self.body)
    }
    pub fn from_parts(parts: ResponseParts, body: B) -> Self {
        Self { parts, body }
    }
    pub fn map<T>(self, f: impl FnOnce(B) -> T) -> Response<T> {
        Response {
            parts: self.parts,
            body: f(self.body),
        }
    }
    pub(crate) fn settled_at(&self) -> Option<Instant> {
        self.parts.settled_at
    }
}
