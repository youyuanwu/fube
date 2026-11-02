//! Operational clear-TCP session I/O shared by gateway and peer adapters.
//!
//! This layer has no actor, RPC, IPC, routing or acknowledgment policy. Adapters
//! own finite queues and retain their admission charges while awaiting a write.
//! Canceling an established write future is not safe: owners must join it.

use std::{future::Future, io, net::SocketAddr, sync::Mutex, task::Poll, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
    time::{timeout_at, Instant},
};

use crate::{
    connection::{write_message, MAX_FRAME_TIME},
    frame::{validate_fixed, Frame, FIXED_HEADER_SIZE, MAX_FRAME_SIZE},
    message::{ListenInstance, Message, MessageId},
    Error, Result,
};

/// Direction determines the native listen-side nonce, not replica identity.
#[derive(Clone, Copy, Debug)]
pub enum Role {
    Initiating,
    Accepted,
}

/// Finite per-frame limits. Idle waiting before a first byte has no timeout.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub frame_time: Duration,
}

impl Limits {
    pub fn validate(self) -> Result<()> {
        if !(FIXED_HEADER_SIZE..=MAX_FRAME_SIZE).contains(&self.max_frame_bytes)
            || self.frame_time.is_zero()
            || self.frame_time > MAX_FRAME_TIME
        {
            return Err(Error::InvalidInput("invalid raw session limits"));
        }
        Ok(())
    }
}

/// Header-stage admission, before proportional body allocation.
pub enum Admission<T> {
    Retain(T),
    Drain,
}

/// An adapter's opaque charge travels with the decoded frame.
pub struct Received<T> {
    pub message: Message,
    pub admission: T,
}

/// Read or drain one frame under one absolute partial-frame watchdog.
///
/// The hook sees only the validated, bounded native header area and total wire
/// length. A drained body uses constant scratch space and is not decoded.
pub async fn read_frame<R, T, F>(
    reader: &mut R,
    limits: Limits,
    admit: F,
) -> Result<Option<Received<T>>>
where
    R: AsyncRead + Unpin,
    F: FnOnce(&[u8], usize) -> Result<Admission<T>>,
{
    limits.validate()?;
    let mut fixed = [0; FIXED_HEADER_SIZE];
    reader.read_exact(&mut fixed[..1]).await?;
    let deadline = Instant::now() + limits.frame_time;
    timeout_at(deadline, async {
        reader.read_exact(&mut fixed[1..]).await?;
        let (size, header_size) = validate_fixed(&fixed)?;
        if size > limits.max_frame_bytes {
            return Err(Error::ResourceLimit("raw inbound frame limit"));
        }
        let mut headers = vec![0; header_size];
        reader.read_exact(&mut headers).await?;
        let admission = match admit(&headers, size)? {
            Admission::Retain(admission) => admission,
            Admission::Drain => {
                let mut scratch = [0; 8192];
                let mut remaining = size - FIXED_HEADER_SIZE - header_size;
                while remaining != 0 {
                    let length = remaining.min(scratch.len());
                    reader.read_exact(&mut scratch[..length]).await?;
                    remaining -= length;
                }
                return Ok(None);
            }
        };
        let mut bytes = vec![0; size];
        bytes[..FIXED_HEADER_SIZE].copy_from_slice(&fixed);
        bytes[FIXED_HEADER_SIZE..FIXED_HEADER_SIZE + header_size].copy_from_slice(&headers);
        reader
            .read_exact(&mut bytes[FIXED_HEADER_SIZE + header_size..])
            .await?;
        Ok(Some(Received {
            message: Message::from_frame(Frame::decode(&bytes)?)?,
            admission,
        }))
    })
    .await
    .map_err(|_| Error::Timeout)?
}

pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R, limits: Limits) -> Result<Message> {
    Ok(read_frame(reader, limits, |_, _| Ok(Admission::Retain(())))
        .await?
        .expect("unconditional admission")
        .message)
}

/// Establish an already-connected or accepted stream. Constructor cancellation
/// disposes this unpublished stream; it never leaves a live partial session.
/// The caller validates the remote hello's profile before publishing authority.
pub async fn establish<S, T, F>(
    mut stream: S,
    role: Role,
    mut local: ListenInstance,
    id: MessageId,
    limits: Limits,
    deadline: Instant,
    admit: F,
) -> Result<(S, Received<T>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(&[u8], usize) -> Result<Admission<T>>,
{
    limits.validate()?;
    if Instant::now() >= deadline {
        return Err(Error::Timeout);
    }
    local.nonce = match role {
        Role::Initiating => [0; 16],
        Role::Accepted => uuid::Uuid::new_v4().to_bytes_le(),
    };
    timeout_at(deadline, async {
        write_message(&mut stream, &Message::listen_instance(id, &local)?).await?;
        let remote = read_frame(&mut stream, limits, admit)
            .await?
            .ok_or(Error::Protocol("missing ListenInstance"))?;
        Ok((stream, remote))
    })
    .await
    .map_err(|_| Error::Timeout)?
}

/// Connect and exchange under the *same* absolute establishment deadline.
pub async fn connect<T, F>(
    endpoint: SocketAddr,
    local: ListenInstance,
    id: MessageId,
    limits: Limits,
    deadline: Instant,
    admit: F,
) -> Result<(TcpStream, Received<T>)>
where
    F: FnOnce(&[u8], usize) -> Result<Admission<T>>,
{
    limits.validate()?;
    if Instant::now() >= deadline {
        return Err(Error::Timeout);
    }
    let stream = timeout_at(deadline, TcpStream::connect(endpoint))
        .await
        .map_err(|_| Error::Timeout)??;
    stream.set_nodelay(true)?;
    establish(stream, Role::Initiating, local, id, limits, deadline, admit).await
}

/// Write one frame with an adapter-owned synchronous visibility fence.
///
/// `attempt` must call the supplied socket poll at most once while holding its
/// cancellation/terminal/authority gate. `None` rejects unstarted work; Pending
/// is not visibility. It may record visibility only for a positive result.
/// `changed` wakes an attempt when adapter state changes.
///
/// Returns false for fenced or expired unstarted work. After a positive write this
/// function owns the entire suffix until success or independent frame failure;
/// the absolute operation deadline no longer cancels byte progress. The caller
/// must retain bytes/charges and join this future. Success is local write only.
pub async fn write_frame<W, F>(
    writer: &mut W,
    bytes: &[u8],
    deadline: Instant,
    limits: Limits,
    changed: &Notify,
    mut attempt: F,
) -> Result<bool>
where
    W: AsyncWrite + Unpin,
    F: FnMut(&mut dyn FnMut() -> Poll<io::Result<usize>>) -> Option<Poll<io::Result<usize>>>,
{
    limits.validate()?;
    if bytes.len() < FIXED_HEADER_SIZE || bytes.len() > limits.max_frame_bytes {
        return Err(Error::ResourceLimit("raw outbound frame limit"));
    }
    let first = timeout_at(deadline, async {
        loop {
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            tokio::select! {
                result = std::future::poll_fn(|cx| {
                    if Instant::now() >= deadline {
                        return Poll::Ready(Ok(None));
                    }
                    let mut poll = || std::pin::Pin::new(&mut *writer).poll_write(cx, bytes);
                    match attempt(&mut poll) {
                        None => Poll::Ready(Ok(None)),
                        Some(Poll::Pending) => Poll::Pending,
                        Some(Poll::Ready(Ok(0))) => Poll::Ready(Err(Error::Io(io::ErrorKind::WriteZero.into()))),
                        Some(Poll::Ready(Ok(size))) => Poll::Ready(Ok(Some(size))),
                        Some(Poll::Ready(Err(error))) => Poll::Ready(Err(error.into())),
                    }
                }) => return result,
                _ = notified => {},
            }
        }
    })
    .await;
    let first = match first {
        Ok(result) => result?,
        Err(_) => return Ok(false),
    };
    let Some(offset) = first else {
        return Ok(false);
    };
    timeout_at(
        Instant::now() + limits.frame_time,
        writer.write_all(&bytes[offset..]),
    )
    .await
    .map_err(|_| Error::Timeout)??;
    Ok(true)
}

/// Stop the reader on terminal notification, but always join the writer.
/// Each adapter records the first terminal cause and wakes unstarted work.
pub async fn join_io<R, W, S, T, E>(reading: R, writing: W, stopped: S, terminate: T, closed: E)
where
    R: Future<Output = std::result::Result<(), E>>,
    W: Future<Output = std::result::Result<(), E>>,
    S: Future<Output = ()>,
    T: Fn(E),
{
    let reading = async {
        tokio::select! {
            biased;
            _ = stopped => {},
            result = reading => terminate(result.err().unwrap_or(closed)),
        }
    };
    let writing = async {
        if let Err(error) = writing.await {
            terminate(error);
        }
    };
    tokio::join!(reading, writing);
}

/// Nonowning stop/visibility state for adapters without an existing terminal
/// gate. Holding this state does not own a task or socket. First cause wins.
pub struct SessionState<E> {
    terminal: Mutex<Option<E>>,
    pub changed: Notify,
}

impl<E: Clone> Default for SessionState<E> {
    fn default() -> Self {
        Self {
            terminal: Mutex::new(None),
            changed: Notify::new(),
        }
    }
}

impl<E: Clone> SessionState<E> {
    pub fn terminate(&self, cause: E) {
        let mut terminal = self.terminal.lock().unwrap();
        if terminal.is_none() {
            *terminal = Some(cause);
            self.changed.notify_waiters();
        }
    }

    pub fn error(&self) -> Option<E> {
        self.terminal.lock().unwrap().clone()
    }

    /// Samples adapter authority and polls under the same gate as termination.
    pub fn attempt(
        &self,
        valid: impl FnOnce() -> bool,
        write: &mut dyn FnMut() -> Poll<io::Result<usize>>,
    ) -> Option<Poll<io::Result<usize>>> {
        let terminal = self.terminal.lock().unwrap();
        if terminal.is_some() || !valid() {
            return None;
        }
        Some(write())
    }

    pub async fn stopped(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.error().is_some() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
#[path = "raw_session_tests.rs"]
mod tests;
