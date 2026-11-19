use crate::{HeaderRecord, Headers, Result};

/// Typed response data, not permission to send a native reply.
#[derive(Debug)]
pub struct Reply<B> {
    headers: Headers,
    body: B,
}

impl<B> Reply<B> {
    pub fn new(body: B) -> Self {
        Self {
            headers: Headers::new(),
            body,
        }
    }

    pub fn append_header(mut self, header: HeaderRecord) -> Result<Self> {
        self.headers.append(header)?;
        Ok(self)
    }

    pub(crate) fn into_parts(self) -> (Headers, B) {
        (self.headers, self.body)
    }
}
