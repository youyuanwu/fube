use crate::{
    header, Body, EnvelopeCheck, Error, HeaderRecord, Headers, Message, MessageId, Response,
    Result, Source,
};
use std::{
    collections::HashMap,
    future::poll_fn,
    sync::{Arc, Mutex, OnceLock, Weak},
    task::{Poll, Waker},
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit},
    time::Instant,
};

struct Allocator {
    guid: [u8; 16],
    index: u32,
}
impl Allocator {
    fn next(&mut self) -> MessageId {
        if self.index == u32::MAX {
            self.guid = uuid::Uuid::new_v4().to_bytes_le();
            self.index = 0;
        }
        self.index += 1;
        MessageId {
            guid: self.guid,
            index: self.index,
        }
    }
}
pub(crate) fn next_id() -> MessageId {
    static IDS: OnceLock<Mutex<Allocator>> = OnceLock::new();
    IDS.get_or_init(|| {
        Mutex::new(Allocator {
            guid: uuid::Uuid::new_v4().to_bytes_le(),
            index: 0,
        })
    })
    .lock()
    .unwrap()
    .next()
}

#[derive(serde::Serialize)]
struct Flag {
    value: bool,
}

pub(crate) fn check_managed(headers: &Headers) -> Result<()> {
    if [
        header::MESSAGE_ID,
        header::RELATES_TO,
        header::EXPECTS_REPLY,
        header::UNCORRELATED_REPLY,
    ]
    .iter()
    .any(|id| headers.first_raw(*id).is_some())
    {
        return Err(Error::InvalidInput(
            "caller supplied managed correlation fields",
        ));
    }
    Ok(())
}
pub(crate) enum EnvelopeKind {
    OneWay,
    Request,
    Notification,
    Reply(MessageId),
}

pub(crate) fn envelope(
    mut headers: Headers,
    body: Body,
    id: MessageId,
    kind: EnvelopeKind,
) -> Result<Message> {
    check_managed(&headers)?;
    headers.append(HeaderRecord::native(header::MESSAGE_ID, &id)?)?;
    if matches!(kind, EnvelopeKind::Request | EnvelopeKind::Notification) {
        headers.append(HeaderRecord::native(
            header::EXPECTS_REPLY,
            &Flag { value: true },
        )?)?;
    }
    if matches!(kind, EnvelopeKind::Notification) {
        headers.append(HeaderRecord::native(
            header::UNCORRELATED_REPLY,
            &Flag { value: true },
        )?)?;
    }
    if let EnvelopeKind::Reply(id) = kind {
        headers.append(HeaderRecord::native(header::RELATES_TO, &id)?)?;
    }
    Ok(Message::from_received(headers, body))
}

/// Native Fault details. Message code units and the received body remain intact.
#[derive(Debug)]
pub struct RemoteFault {
    code: i64,
    header_code: i64,
    message: Arc<[u16]>,
    body: Body,
    body_error: Option<fubst::Error>,
}
impl PartialEq for RemoteFault {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl Eq for RemoteFault {}
impl RemoteFault {
    pub fn code(&self) -> i64 {
        self.code
    }
    pub fn header_code(&self) -> i64 {
        self.header_code
    }
    pub fn message(&self) -> &[u16] {
        &self.message
    }
    pub fn body(&self) -> &Body {
        &self.body
    }
    pub fn body_error(&self) -> Option<fubst::Error> {
        self.body_error
    }
    fn decode(message: Message) -> Self {
        let fault = message.headers().effective().fault;
        let mut result = Self {
            code: fault.code,
            header_code: fault.code,
            message: Arc::from([]),
            body: message.into_body(),
            body_error: None,
        };
        if fault.has_body {
            match reject_fault(&result.body) {
                Ok((code, message)) if !message.is_empty() => {
                    result.code = code;
                    result.message = message.into();
                }
                Ok(_) => {}
                Err(error) => result.body_error = Some(error),
            }
        }
        result
    }
}
fn reject_fault(body: &Body) -> fubst::Result<(i64, Vec<u16>)> {
    let mut segments = body.segments();
    let bytes = segments.next().unwrap_or(&[]);
    if segments.next().is_some() {
        return Err(fubst::Error::InvalidInput("segmented received Fault body"));
    }
    let mut decoder = fubst::Decoder::with_limits(
        bytes,
        fubst::Limits {
            max_bytes: body.len(),
            ..Default::default()
        },
    )?;
    let details = decoder.object(|decoder| {
        if decoder.is_scope_end()? {
            return Ok((0, Vec::new()));
        }
        decoder.object(|decoder| {
            let code = if decoder.is_scope_end()? {
                0
            } else {
                decoder.i64()?
            };
            let bytes = if decoder.is_scope_end()? {
                &[][..]
            } else {
                decoder.string_utf16_bytes()?
            };
            let mut message = Vec::new();
            message
                .try_reserve_exact(bytes.len() / 2)
                .map_err(|_| fubst::Error::ResourceLimit("Fault message"))?;
            message.extend(
                bytes
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| u16::from_le_bytes(*pair)),
            );
            Ok((code, message))
        })
    })?;
    decoder.finish()?;
    Ok(details)
}

struct PendingState {
    result: Option<(Instant, Result<Response<Body>>)>,
    waker: Option<Waker>,
}
pub(crate) struct Pending {
    state: Mutex<PendingState>,
    _slot: OwnedSemaphorePermit,
    deadline: Instant,
    acknowledgment: Option<EnvelopeCheck>,
}
impl Pending {
    pub(crate) fn check(&self, headers: &Headers, source: &Source) -> Result<()> {
        if let Some(check) = self.acknowledgment {
            check(headers, source)?;
        }
        Ok(())
    }

    fn finish(&self, result: Result<Response<Body>>, now: Instant) {
        let mut state = self.state.lock().unwrap();
        if state.result.is_some() {
            return;
        }
        state.result = Some((now, result));
        let wake = state.waker.take();
        drop(state);
        if let Some(wake) = wake {
            wake.wake();
        }
    }
    pub(crate) async fn wait(&self, deadline: Instant) -> Result<Response<Body>> {
        poll_fn(|cx| {
            let mut state = self.state.lock().unwrap();
            if let Some((settled, result)) = state.result.take() {
                Poll::Ready(match result {
                    Ok(response) => match response.settled_at() {
                        Some(time) if time <= deadline => Ok(response),
                        Some(_) => Err(Error::Timeout("RPC")),
                        None => Err(Error::Internal("native response lacks settlement time")),
                    },
                    Err(error) if settled <= deadline => Err(error),
                    Err(_) => Err(Error::Timeout("RPC")),
                })
            } else {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

#[derive(Default)]
struct TableState {
    closed: Option<Error>,
    entries: HashMap<MessageId, Arc<Pending>>,
}
#[derive(Default)]
pub(crate) struct Table(Mutex<TableState>, Notify);
impl Table {
    pub(crate) async fn changed(&self) {
        self.1.notified().await;
    }
    pub(crate) fn insert(
        self: &Arc<Self>,
        id: MessageId,
        slot: OwnedSemaphorePermit,
        deadline: Instant,
        acknowledgment: Option<EnvelopeCheck>,
    ) -> Result<Registration> {
        let mut table = self.0.lock().unwrap();
        if let Some(error) = &table.closed {
            return Err(error.clone());
        }
        if table.entries.contains_key(&id) {
            return Err(Error::Internal("managed ID reused"));
        }
        table
            .entries
            .try_reserve(1)
            .map_err(|_| Error::ResourceLimit("pending correlation allocation"))?;
        let pending = Arc::new(Pending {
            state: Mutex::new(PendingState {
                result: None,
                waker: None,
            }),
            _slot: slot,
            deadline,
            acknowledgment,
        });
        table.entries.insert(id, pending.clone());
        drop(table);
        self.1.notify_one();
        Ok(Registration {
            id,
            pending,
            table: Arc::downgrade(self),
        })
    }
    pub(crate) fn lookup(&self, id: MessageId) -> Option<Arc<Pending>> {
        self.0
            .lock()
            .unwrap()
            .entries
            .get(&id)
            .filter(|pending| pending.deadline > Instant::now())
            .cloned()
    }
    pub(crate) fn complete(
        &self,
        id: MessageId,
        expected: &Arc<Pending>,
        message: Message,
        source: Source,
    ) {
        let mut table = self.0.lock().unwrap();
        if !table
            .entries
            .get(&id)
            .is_some_and(|entry| Arc::ptr_eq(entry, expected))
        {
            return;
        }
        table.entries.remove(&id);
        drop(table);
        let now = Instant::now();
        let result = if message.headers().effective().fault.code != 0 {
            Err(Error::RemoteFault(Arc::new(RemoteFault::decode(message))))
        } else {
            Ok(Response::received(
                message.headers().clone(),
                message.into_body(),
                source,
                now,
            ))
        };
        expected.finish(result, now);
        self.1.notify_one();
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.0.lock().unwrap().entries.is_empty()
    }
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.0
            .lock()
            .unwrap()
            .entries
            .values()
            .map(|pending| pending.deadline)
            .min()
    }
    pub(crate) fn expire(&self) -> bool {
        let now = Instant::now();
        let expired = {
            let mut table = self.0.lock().unwrap();
            let id = table
                .entries
                .iter()
                .find(|(_, pending)| pending.deadline <= now)
                .map(|(id, _)| *id);
            id.and_then(|id| table.entries.remove(&id))
        };
        if let Some(pending) = expired {
            pending.finish(Err(Error::Timeout("RPC")), pending.deadline);
            true
        } else {
            false
        }
    }
    pub(crate) fn close(&self, error: Error) {
        let mut table = self.0.lock().unwrap();
        if table.closed.is_some() {
            return;
        }
        table.closed = Some(error.clone());
        let entries = std::mem::take(&mut table.entries);
        drop(table);
        for pending in entries.into_values() {
            pending.finish(Err(error.clone()), Instant::now());
        }
        self.1.notify_one();
    }
}
pub(crate) struct Registration {
    id: MessageId,
    pub(crate) pending: Arc<Pending>,
    table: Weak<Table>,
}
impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(table) = self.table.upgrade() {
            let removed = table.0.lock().unwrap().entries.remove(&self.id);
            drop(removed);
            table.1.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn namespace_rotates_before_counter_reuse() {
        let mut ids = Allocator {
            guid: [1; 16],
            index: u32::MAX - 1,
        };
        let last = ids.next();
        let next = ids.next();
        assert_eq!(last.index, u32::MAX);
        assert_ne!(last.guid, next.guid);
        assert_eq!(next.index, 1);
    }
}
