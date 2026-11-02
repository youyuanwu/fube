use super::*;
use crate::message::Header;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

fn limits() -> Limits {
    Limits {
        max_frame_bytes: MAX_FRAME_SIZE,
        frame_time: Duration::from_secs(2),
    }
}

fn message(body: Vec<u8>) -> Message {
    Message {
        headers: vec![Header::actor(7).unwrap()],
        body,
    }
}

#[tokio::test]
async fn accepted_and_initiating_exchange_directional_nonce_and_frames() {
    let (a, b) = tokio::io::duplex(8192);
    let deadline = Instant::now() + Duration::from_secs(1);
    let local = ListenInstance {
        address: "127.0.0.1:1234".into(),
        instance: 1,
        nonce: [8; 16],
    };
    let id = MessageId {
        guid: [1; 16],
        index: 1,
    };
    let (a, b) = tokio::join!(
        establish(a, Role::Initiating, local.clone(), id, limits(), deadline),
        establish(b, Role::Accepted, local, id, limits(), deadline),
    );
    let (mut a, hello_b) = a.unwrap();
    let (mut b, hello_a) = b.unwrap();
    assert_eq!(
        ListenInstance::decode(&hello_a.body).unwrap().nonce,
        [0; 16]
    );
    assert_ne!(
        ListenInstance::decode(&hello_b.body).unwrap().nonce,
        [0; 16]
    );
    let frame = message(vec![1; 4096]);
    let bytes = frame.to_frame().unwrap().encode().unwrap();
    let notify = Notify::new();
    let (sent, received) = tokio::join!(
        write_frame(&mut a, &bytes, deadline, limits(), &notify, |write| Some(
            write()
        )),
        read_message(&mut b, limits()),
    );
    assert!(sent.unwrap());
    assert_eq!(received.unwrap(), frame);
}

#[tokio::test]
async fn staged_drain_preserves_following_control_frame() {
    let (mut a, mut b) = tokio::io::duplex(128);
    let data = message(vec![1; 32 * 1024]);
    let control = message(vec![2]);
    let sending = async {
        write_message(&mut a, &data).await.unwrap();
        write_message(&mut a, &control).await.unwrap();
    };
    let reading = async {
        assert!(read_frame::<_, (), _>(&mut b, limits(), |headers, size| {
            assert!(headers.len() < 128);
            assert!(size > 32 * 1024);
            Ok(Admission::Drain)
        })
        .await
        .unwrap()
        .is_none());
        assert_eq!(read_message(&mut b, limits()).await.unwrap(), control);
    };
    tokio::join!(sending, reading);
}

#[tokio::test]
async fn started_write_keeps_suffix_after_fence_loss_and_deadline() {
    let (mut a, mut b) = tokio::io::duplex(16);
    let message = message(vec![3; 4096]);
    let bytes = message.to_frame().unwrap().encode().unwrap();
    let active = Arc::new(AtomicBool::new(true));
    let visible = Arc::new(AtomicBool::new(false));
    let changed = Notify::new();
    let deadline = Instant::now() + Duration::from_millis(10);
    let writing = write_frame(&mut a, &bytes, deadline, limits(), &changed, |write| {
        if !active.load(Ordering::Acquire) {
            return None;
        }
        let result = write();
        if matches!(result, Poll::Ready(Ok(n)) if n > 0) {
            visible.store(true, Ordering::Release);
        }
        Some(result)
    });
    let reading = async {
        while !visible.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        active.store(false, Ordering::Release);
        changed.notify_waiters();
        tokio::time::sleep_until(deadline + Duration::from_millis(10)).await;
        assert_eq!(read_message(&mut b, limits()).await.unwrap(), message);
    };
    let (written, ()) = tokio::join!(writing, reading);
    assert!(written.unwrap());
}

#[tokio::test(start_paused = true)]
async fn pending_first_write_expiry_is_not_a_started_frame_failure() {
    let (mut a, _b) = tokio::io::duplex(1);
    a.write_all(&[0]).await.unwrap();
    let bytes = message(vec![1]).to_frame().unwrap().encode().unwrap();
    let changed = Notify::new();
    assert!(!write_frame(
        &mut a,
        &bytes,
        Instant::now() + Duration::from_secs(1),
        limits(),
        &changed,
        |write| Some(write()),
    )
    .await
    .unwrap());
}

#[tokio::test(start_paused = true)]
async fn establishment_uses_supplied_absolute_deadline() {
    let (a, _b) = tokio::io::duplex(1);
    let start = Instant::now();
    assert!(matches!(
        establish(
            a,
            Role::Accepted,
            ListenInstance::default(),
            MessageId {
                guid: [1; 16],
                index: 1
            },
            limits(),
            start + Duration::from_millis(100)
        )
        .await,
        Err(Error::Timeout),
    ));
    assert_eq!(Instant::now() - start, Duration::from_millis(100));
}
