use std::{net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::{timeout_at, Instant},
};

use crate::{
    frame::{validate_fixed, Frame, FIXED_HEADER_SIZE},
    message::{Header, ListenInstance, Message, MessageId, EXPECTS_REPLY},
    Error, Result,
};

const MAX_INTERLEAVED_MESSAGES: usize = 1024;

pub fn deadline_after(timeout: Duration) -> Result<Instant> {
    let ticks = timeout.as_nanos() / 100;
    if ticks == 0 || ticks > i64::MAX as u128 {
        return Err(Error::InvalidInput(
            "timeout must be positive, at least 100ns, and fit native TimeSpan",
        ));
    }
    Instant::now()
        .checked_add(timeout)
        .ok_or(Error::InvalidInput("timeout exceeds monotonic clock range"))
}

fn remaining_ticks(deadline: Instant) -> Result<i64> {
    let ticks = deadline
        .checked_duration_since(Instant::now())
        .ok_or(Error::Timeout)?
        .as_nanos()
        / 100;
    if ticks == 0 {
        return Err(Error::Timeout);
    }
    i64::try_from(ticks).map_err(|_| Error::InvalidInput("timeout exceeds native TimeSpan"))
}

/// Reads one complete bounded clear frame. Cancelling a partial read requires
/// discarding the reader; Connection enforces this for its owned socket.
pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Message> {
    let mut fixed = [0; FIXED_HEADER_SIZE];
    reader.read_exact(&mut fixed).await?;
    let (size, _) = validate_fixed(&fixed)?;
    let mut bytes = vec![0; size];
    bytes[..FIXED_HEADER_SIZE].copy_from_slice(&fixed);
    reader.read_exact(&mut bytes[FIXED_HEADER_SIZE..]).await?;
    Message::from_frame(Frame::decode(&bytes)?)
}

/// Writes a complete message; a cancelled partial write requires closing the
/// stream rather than sending another message after its missing suffix.
pub async fn write_message<W: AsyncWrite + Unpin>(writer: &mut W, message: &Message) -> Result<()> {
    let bytes = message.to_frame()?.encode()?;
    writer.write_all(&bytes).await?;
    Ok(())
}

/// Single-flight, development-only clear TCP. No reconnect or background tasks.
pub struct Connection {
    stream: Option<TcpStream>,
    process_id: [u8; 16],
    next_index: u32,
    remote: ListenInstance,
}

impl Connection {
    pub async fn connect_clear(endpoint: SocketAddr, deadline: Instant) -> Result<Self> {
        remaining_ticks(deadline)?;
        let process_id = uuid::Uuid::new_v4().to_bytes_le();
        let id = MessageId {
            guid: process_id,
            index: 1,
        };
        let (stream, remote) = timeout_at(deadline, async {
            let mut stream = TcpStream::connect(endpoint).await?;
            stream.set_nodelay(true)?;
            write_message(
                &mut stream,
                &Message::listen_instance(id, &ListenInstance::default())?,
            )
            .await?;
            let message = read_message(&mut stream).await?;
            let properties = message.properties()?;
            if let Some(fault) = properties.fault.filter(|fault| fault.code != 0) {
                return Err(Error::Remote {
                    code: fault.code,
                    message: "native fault during connection establishment".into(),
                });
            }
            if properties.actor != 1 || !properties.action.is_empty() {
                return Err(Error::Protocol("expected remote ListenInstance control"));
            }
            let remote = ListenInstance::decode(&message.body)?;
            Ok((stream, remote))
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(Self {
            stream: Some(stream),
            process_id,
            next_index: 2,
            remote,
        })
    }

    pub fn is_closed(&self) -> bool {
        self.stream.is_none()
    }

    /// Discard the socket immediately, including after malformed reply data.
    pub fn abort(&mut self) {
        drop(self.stream.take());
    }

    pub fn remote_instance(&self) -> &ListenInstance {
        &self.remote
    }

    pub async fn request(
        &mut self,
        action: &str,
        actor: i64,
        extra_headers: Vec<Header>,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<Message> {
        // The future owns the socket until completion. Cancellation, timeout,
        // or any I/O/protocol failure drops it and leaves this handle closed.
        let mut stream = self.stream.take().ok_or(Error::Closed)?;
        let id = MessageId {
            guid: self.process_id,
            index: self.next_index,
        };
        self.next_index = self
            .next_index
            .checked_add(1)
            .ok_or(Error::Protocol("message ID index exhausted"))?;
        let mut headers = vec![Header::action(action)?, Header::actor(actor)?];
        headers.extend(extra_headers);
        headers.push(Header::timeout(remaining_ticks(deadline)?)?);
        headers.push(Header::boolean(EXPECTS_REPLY, true)?);
        headers.push(Header::message_id(id)?);
        let request = Message { headers, body };
        let result = timeout_at(deadline, async {
            write_message(&mut stream, &request).await?;
            for _ in 0..MAX_INTERLEAVED_MESSAGES {
                let reply = read_message(&mut stream).await?;
                let properties = reply.properties()?;
                if properties.actor == 1
                    && properties.action.is_empty()
                    && properties.fault.is_none()
                {
                    self.remote = ListenInstance::decode(&reply.body)?;
                    continue;
                }
                if !properties.uncorrelated_reply && properties.relates_to == id {
                    return Ok(reply);
                }
            }
            Err(Error::Protocol("too many unrelated or control messages"))
        })
        .await
        .map_err(|_| Error::Timeout)?;
        let reply = result?;
        self.stream = Some(stream);
        Ok(reply)
    }

    pub async fn close(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            stream.shutdown().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::UNCORRELATED_REPLY;
    use tokio::{net::TcpListener, sync::oneshot, time::timeout};

    fn deadline() -> Instant {
        deadline_after(Duration::from_secs(2)).unwrap()
    }

    fn control() -> Message {
        Message::listen_instance(
            MessageId {
                guid: [1; 16],
                index: 1,
            },
            &ListenInstance {
                address: "127.0.0.1:19000".into(),
                instance: 123,
                nonce: [2; 16],
            },
        )
        .unwrap()
    }

    async fn establish_peer(stream: &mut TcpStream) {
        write_message(stream, &control()).await.unwrap();
        let local = read_message(stream).await.unwrap();
        let properties = local.properties().unwrap();
        assert_eq!(properties.actor, 1);
        assert_eq!(properties.action, "");
        assert!(properties.high_priority);
        assert_ne!(properties.id, MessageId::default());
        assert_eq!(
            ListenInstance::decode(&local.body).unwrap(),
            ListenInstance::default()
        );
    }

    fn response(id: MessageId) -> Message {
        Message {
            headers: vec![
                Header::actor(5).unwrap(),
                Header::action("PingRequest").unwrap(),
                Header::relates_to(id).unwrap(),
            ],
            body: vec![1, 2, 3],
        }
    }

    #[tokio::test]
    async fn wire_handles_every_split_and_coalesced_messages() {
        let message = control();
        let bytes = message.to_frame().unwrap().encode().unwrap();
        for split in 0..=bytes.len() {
            let (mut writer, mut reader) = tokio::io::duplex(7);
            let write = async {
                writer.write_all(&bytes[..split]).await.unwrap();
                tokio::task::yield_now().await;
                writer.write_all(&bytes[split..]).await.unwrap();
            };
            let read = async { assert_eq!(read_message(&mut reader).await.unwrap(), message) };
            tokio::join!(write, read);
        }
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        writer.write_all(&bytes.repeat(2)).await.unwrap();
        for _ in 0..2 {
            assert_eq!(read_message(&mut reader).await.unwrap(), message);
        }
        drop(writer);
        assert!(read_message(&mut reader).await.is_err());
    }

    #[tokio::test]
    async fn invalid_fixed_header_fails_without_waiting_for_body() {
        for (size, provider) in [(u32::MAX, 0), (11, 0), (12, 1)] {
            let (mut writer, mut reader) = tokio::io::duplex(32);
            let mut fixed = [0; 12];
            fixed[..4].copy_from_slice(&size.to_le_bytes());
            fixed[4] = provider;
            writer.write_all(&fixed).await.unwrap();
            assert!(timeout(Duration::from_secs(1), read_message(&mut reader))
                .await
                .unwrap()
                .is_err());
        }
    }

    #[tokio::test]
    async fn sequential_requests_ignore_unrelated_push_and_duplicate_replies() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            establish_peer(&mut stream).await;
            let mut previous = None;
            for index in 2..5 {
                let request = read_message(&mut stream).await.unwrap();
                let properties = request.properties().unwrap();
                assert_eq!(properties.id.index, index);
                assert!(properties.expects_reply);
                if let Some(previous) = previous {
                    write_message(&mut stream, &response(previous))
                        .await
                        .unwrap();
                }
                let wrong_id = MessageId {
                    index: properties.id.index + 1000,
                    ..properties.id
                };
                write_message(&mut stream, &response(wrong_id))
                    .await
                    .unwrap();
                let mut push = response(properties.id);
                push.headers
                    .push(Header::boolean(UNCORRELATED_REPLY, true).unwrap());
                write_message(&mut stream, &push).await.unwrap();
                write_message(&mut stream, &control()).await.unwrap();
                write_message(&mut stream, &response(properties.id))
                    .await
                    .unwrap();
                previous = Some(properties.id);
            }
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        });
        let mut connection = Connection::connect_clear(address, deadline())
            .await
            .unwrap();
        assert_eq!(connection.remote_instance().instance, 123);
        for _ in 0..3 {
            let reply = connection
                .request("PingRequest", 5, vec![], vec![], deadline())
                .await
                .unwrap();
            assert_eq!(reply.body, [1, 2, 3]);
        }
        connection.close().await.unwrap();
        connection.close().await.unwrap();
        assert!(connection.is_closed());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_closes_the_inflight_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            establish_peer(&mut stream).await;
            read_message(&mut stream).await.unwrap();
            assert_eq!(
                timeout(Duration::from_secs(1), stream.read(&mut [0]))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        let mut connection = Connection::connect_clear(address, deadline())
            .await
            .unwrap();
        let started = Instant::now();
        let result = connection
            .request(
                "PingRequest",
                5,
                vec![],
                vec![],
                deadline_after(Duration::from_millis(30)).unwrap(),
            )
            .await;
        assert!(matches!(result, Err(Error::Timeout)));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(connection.is_closed());
        assert!(matches!(
            connection
                .request("PingRequest", 5, vec![], vec![], deadline())
                .await,
            Err(Error::Closed)
        ));
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_inflight_io_closes_instead_of_reusing_a_partial_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (observed_tx, observed_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            establish_peer(&mut stream).await;
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 1);
            observed_tx.send(()).unwrap();
            let mut count = 0;
            let mut buffer = [0; 8192];
            loop {
                let received = timeout(Duration::from_secs(1), stream.read(&mut buffer))
                    .await
                    .unwrap();
                match received {
                    Ok(0) => break,
                    Ok(size) => count += size,
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => break,
                    Err(error) => panic!("unexpected peer read failure: {error}"),
                }
            }
            count
        });
        let mut connection = Connection::connect_clear(address, deadline())
            .await
            .unwrap();
        {
            let request = connection.request(
                "PingRequest",
                5,
                vec![],
                vec![0; 3 * 1024 * 1024],
                deadline(),
            );
            tokio::pin!(request);
            tokio::select! {
                result = &mut request => panic!("request unexpectedly completed: {result:?}"),
                result = observed_rx => result.unwrap(),
            }
        }
        assert!(connection.is_closed());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn disconnect_and_invalid_reply_close_the_connection() {
        for malformed in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                establish_peer(&mut stream).await;
                read_message(&mut stream).await.unwrap();
                if malformed {
                    let mut fixed = [0; 12];
                    fixed[..4].copy_from_slice(&12u32.to_le_bytes());
                    fixed[4] = 1;
                    stream.write_all(&fixed).await.unwrap();
                }
            });
            let mut connection = Connection::connect_clear(address, deadline())
                .await
                .unwrap();
            assert!(connection
                .request("PingRequest", 5, vec![], vec![], deadline())
                .await
                .is_err());
            assert!(connection.is_closed());
            peer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn silent_establishment_has_a_finite_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_message(&mut stream).await.unwrap();
            assert_eq!(
                timeout(Duration::from_secs(1), stream.read(&mut [0]))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        assert!(matches!(
            Connection::connect_clear(address, deadline_after(Duration::from_millis(30)).unwrap())
                .await,
            Err(Error::Timeout)
        ));
        peer.await.unwrap();
    }

    #[test]
    fn deadline_configuration_is_explicit_and_bounded() {
        assert!(deadline_after(Duration::ZERO).is_err());
        assert!(deadline_after(Duration::from_nanos(99)).is_err());
        assert!(deadline_after(Duration::MAX).is_err());
        assert!(deadline_after(Duration::from_secs(30)).is_ok());
    }
}
