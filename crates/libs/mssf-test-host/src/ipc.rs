use std::{
    io::{Read, Write},
    net::{Shutdown, TcpStream},
    sync::mpsc::{self, Receiver, SyncSender},
    thread::{self, JoinHandle},
    time::Duration,
};

use mssf_transport::{
    frame::{validate_fixed, Frame, FIXED_HEADER_SIZE},
    message::{Header, ListenInstance, Message, MessageId},
};

use crate::{HostError, Result};

pub struct Session {
    stream: TcpStream,
    pub incoming: Receiver<Result<Message>>,
    reader: Option<JoinHandle<()>>,
    guid: [u8; 16],
    index: u32,
}

fn read_message(stream: &mut TcpStream) -> Result<Message> {
    let mut fixed = [0; FIXED_HEADER_SIZE];
    stream.read_exact(&mut fixed)?;
    let (size, _) = validate_fixed(&fixed)?;
    let mut frame = vec![0; size];
    frame[..FIXED_HEADER_SIZE].copy_from_slice(&fixed);
    stream.read_exact(&mut frame[FIXED_HEADER_SIZE..])?;
    Ok(Message::from_frame(Frame::decode(&frame)?)?)
}

fn receive(mut stream: TcpStream, sender: SyncSender<Result<Message>>) {
    loop {
        let message = read_message(&mut stream);
        let failed = message.is_err();
        // A full control queue is a scenario failure, not a blocking reader
        // that could prevent shutdown or silently discard a required reply.
        if sender.try_send(message).is_err() || failed {
            let _ = stream.shutdown(Shutdown::Both);
            return;
        }
    }
}

impl Session {
    pub fn new(stream: TcpStream, address: String) -> Result<Self> {
        stream.set_read_timeout(Some(Duration::from_secs(120)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        stream.set_nodelay(true)?;
        let (sender, incoming) = mpsc::sync_channel(64);
        let read_stream = stream.try_clone()?;
        let reader = thread::spawn(move || receive(read_stream, sender));
        let mut session = Self {
            stream,
            incoming,
            reader: Some(reader),
            guid: uuid::Uuid::new_v4().to_bytes_le(),
            index: 0,
        };
        let id = session.id();
        session.send(Message::listen_instance(
            id,
            &ListenInstance {
                address,
                instance: 1,
                nonce: uuid::Uuid::new_v4().to_bytes_le(),
            },
        )?)?;
        Ok(session)
    }

    fn id(&mut self) -> MessageId {
        self.index += 1;
        MessageId {
            guid: self.guid,
            index: self.index,
        }
    }

    pub fn send(&mut self, message: Message) -> Result<()> {
        self.stream
            .write_all(&message.to_frame()?.encode_with_body_crc(true)?)?;
        Ok(())
    }

    pub fn one_way(&mut self, actor: i64, action: &str, body: Vec<u8>) -> Result<()> {
        let id = self.id();
        self.send(Message {
            headers: vec![
                Header::message_id(id)?,
                Header::actor(actor)?,
                Header::action(action)?,
            ],
            body,
        })
    }

    pub fn reply(&mut self, request: MessageId, action: Option<&str>, body: Vec<u8>) -> Result<()> {
        if request == MessageId::default() {
            return Err(HostError("correlated request missing MessageId".into()));
        }
        let id = self.id();
        let mut headers = vec![Header::message_id(id)?, Header::relates_to(request)?];
        if let Some(action) = action {
            headers.push(Header::action(action)?);
        }
        self.send(Message { headers, body })
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Both);
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn fragmented_frames_and_oversize_are_bounded() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        let message =
            Message::listen_instance(MessageId::default(), &ListenInstance::default()).unwrap();
        let bytes = message
            .to_frame()
            .unwrap()
            .encode_with_body_crc(true)
            .unwrap();
        let writer = thread::spawn(move || {
            for chunk in bytes.chunks(2) {
                client.write_all(chunk).unwrap();
            }
            let mut oversized = [0; FIXED_HEADER_SIZE];
            oversized[..4].copy_from_slice(&u32::MAX.to_le_bytes());
            client.write_all(&oversized).unwrap();
        });
        assert_eq!(read_message(&mut server).unwrap(), message);
        assert!(read_message(&mut server)
            .unwrap_err()
            .0
            .contains("oversized"));
        writer.join().unwrap();
    }

    #[test]
    fn correlated_reply_requires_nonzero_request_id() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        let mut session = Session::new(server, listener.local_addr().unwrap().to_string()).unwrap();
        assert!(session.reply(MessageId::default(), None, vec![]).is_err());
    }
}
