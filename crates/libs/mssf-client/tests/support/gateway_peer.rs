//! Framed offline peer helpers, never a native interoperability oracle.
use mssf_transport::{
    connection::{read_message, write_message},
    message::{Header, ListenInstance, Message, MessageId},
};
use tokio::net::{TcpListener, TcpStream};

pub async fn accept(listener: TcpListener) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    let control = read_message(&mut stream).await.unwrap();
    assert_eq!(control.properties().unwrap().actor, 1);
    write_message(
        &mut stream,
        &Message::listen_instance(
            MessageId {
                guid: [9; 16],
                index: 1,
            },
            &ListenInstance::default(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    stream
}

pub fn reply(request: &Message, action: &str, body: Vec<u8>) -> Message {
    Message {
        headers: vec![
            Header::actor(5).unwrap(),
            Header::action(action).unwrap(),
            Header::relates_to(request.properties().unwrap().id).unwrap(),
        ],
        body,
    }
}
