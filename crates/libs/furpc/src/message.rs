use std::ops::Range;

use crate::{
    body::Body as StoredBody,
    construction as policy,
    resource::{admit_allocations, AllocationRef, ByteScope},
    wire::headers::HeaderDecodeContext,
    Error, HeaderRecord, Headers, Result,
};

/// Immutable segmented native bytes. Clones/slices preserve allocation leases.
#[derive(Debug, Clone)]
pub struct Body {
    inner: StoredBody,
}

impl Body {
    pub const fn empty() -> Self {
        Self {
            inner: StoredBody::empty(),
        }
    }

    /// Adopt caller bytes under standalone construction policy, without admission.
    /// Empty inputs are discarded, including any unused capacity.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Ok(Self {
            inner: StoredBody::from_bytes(bytes, policy::BODY_LIMITS)?,
        })
    }

    /// Compose bounded visible segments without copying payload bytes.
    /// Existing leases survive; new segment bookkeeping remains unadmitted.
    pub fn from_segments(segments: Vec<Body>) -> Result<Self> {
        if segments.len() > policy::MAX_BODY_SEGMENTS {
            return Err(Error::ResourceLimit("body segment count"));
        }
        segments.iter().try_fold(0usize, |total, body| {
            total
                .checked_add(body.len())
                .filter(|len| *len <= policy::MAX_BODY_BYTES)
                .ok_or(Error::ResourceLimit("constructed body bytes"))
        })?;
        let count = segments
            .iter()
            .flat_map(|body| body.inner.owned_segments())
            .take(policy::MAX_BODY_SEGMENTS + 1)
            .count();
        if count > policy::MAX_BODY_SEGMENTS {
            return Err(Error::ResourceLimit("body segment count"));
        }
        let mut stored = Vec::new();
        stored
            .try_reserve_exact(count)
            .map_err(|_| Error::ResourceLimit("body segment allocation"))?;
        stored.extend(segments.iter().flat_map(|body| body.inner.owned_segments()));
        Ok(Self {
            inner: StoredBody::from_segments(stored, policy::BODY_LIMITS)?,
        })
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Full backing/metadata charge, not visible slice length or process RSS.
    pub fn retained_bytes(&self) -> usize {
        self.inner.retained_bytes()
    }

    /// A zero-copy view retaining the parent's entire segment table/backing.
    pub fn slice(&self, range: Range<usize>) -> Result<Self> {
        Ok(Self {
            inner: self.inner.slice(range)?,
        })
    }

    /// Borrowed views cannot outlive this body's allocation owners.
    pub fn segments(&self) -> impl Iterator<Item = &[u8]> {
        self.inner.segments()
    }

    fn allocations(&self) -> impl Iterator<Item = AllocationRef<'_>> + Clone {
        self.inner.allocations()
    }
}

impl Default for Body {
    fn default() -> Self {
        Self::empty()
    }
}

impl TryFrom<Vec<u8>> for Body {
    type Error = Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        Self::from_bytes(bytes)
    }
}

/// An immutable raw native envelope. Constructing it grants no send/reply rights.
///
/// ```
/// use furpc::{header, ActorId, Body, Message};
///
/// let message = Message::builder()
///     .append_header(header::actor(ActorId::new(70000)?)?)
///     .append_header(header::action("OneWay")?)
///     .body(Body::from_bytes(vec![1, 2, 3])?)
///     .build()?;
/// assert_eq!(message.headers().first_actor().unwrap().get(), 70000);
/// # Ok::<(), furpc::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct Message {
    headers: Headers,
    body: Body,
}

impl Message {
    pub fn builder() -> MessageBuilder {
        MessageBuilder::default()
    }

    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    pub fn body(&self) -> &Body {
        &self.body
    }

    pub fn into_body(self) -> Body {
        self.body
    }

    /// Storage/preparation seam, not operation or authority admission. The driver
    /// must call after resolving managed headers and recheck all send gates.
    #[allow(dead_code, reason = "Preparation seam for the upcoming frame writer")]
    pub(crate) fn admit_storage(
        &self,
        context: &HeaderDecodeContext,
        max_frame_bytes: usize,
        scope: &ByteScope,
    ) -> Result<()> {
        self.headers.validate(context)?;
        validate_envelope(self.headers.encoded_len(), self.body.len(), max_frame_bytes)?;
        admit_allocations(
            self.headers.allocations().chain(self.body.allocations()),
            scope,
        )
    }
}

/// Standalone bounded construction with a sticky first error.
#[derive(Debug, Default)]
pub struct MessageBuilder {
    headers: Headers,
    body: Body,
    error: Option<Error>,
}

impl MessageBuilder {
    pub fn append_header(mut self, header: HeaderRecord) -> Self {
        if self.error.is_none() {
            let preflight = self
                .headers
                .encoded_len()
                .checked_add(4 + header.value().len())
                .ok_or(Error::InvalidInput("native frame length exceeds u32"))
                .and_then(|headers| validate_envelope(headers, self.body.len(), u32::MAX as usize));
            self.error = preflight.and_then(|()| self.headers.append(header)).err();
        }
        self
    }

    pub fn body(mut self, body: Body) -> Self {
        if self.error.is_none() {
            match validate_envelope(self.headers.encoded_len(), body.len(), u32::MAX as usize) {
                Ok(()) => self.body = body,
                Err(error) => self.error = Some(error),
            }
        }
        self
    }

    pub fn build(self) -> Result<Message> {
        if let Some(error) = self.error {
            return Err(error);
        }
        validate_envelope(
            self.headers.encoded_len(),
            self.body.len(),
            u32::MAX as usize,
        )?;
        Ok(Message {
            headers: self.headers,
            body: self.body,
        })
    }
}

fn validate_envelope(headers_len: usize, body_len: usize, max_frame_bytes: usize) -> Result<()> {
    if headers_len > u16::MAX as usize {
        return Err(Error::InvalidInput("native header area exceeds u16"));
    }
    let len = 12usize
        .checked_add(headers_len)
        .and_then(|len| len.checked_add(body_len))
        .filter(|len| *len <= u32::MAX as usize)
        .ok_or(Error::InvalidInput("native frame length exceeds u32"))?;
    if len > max_frame_bytes {
        return Err(Error::ResourceLimit("frame bytes"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
