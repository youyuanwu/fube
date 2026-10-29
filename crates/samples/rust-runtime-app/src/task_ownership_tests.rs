use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Capture(Arc<AtomicUsize>);
impl Drop for Capture {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelled_shutdown_retains_handle_and_joins_all_captures() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let owner = Listener::default();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let listener_capture = Arc::new(());
        let retained = Arc::downgrade(&listener_capture);
        let dropped = Arc::new(AtomicUsize::new(0));
        let disposed = dropped.clone();
        let entered = Arc::new(tokio::sync::Notify::new());
        let started = entered.clone();
        owner
            .start(listener, Duration::from_secs(35), move |mut stream| {
                let capture = listener_capture.clone();
                let dropped = disposed.clone();
                let entered = started.clone();
                async move {
                    let _capture = capture;
                    let _disposed = Capture(dropped);
                    entered.notify_one();
                    let _ = stream.read(&mut [0]).await;
                }
            })
            .unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        entered.notified().await;
        {
            let mut shutdown = std::pin::pin!(owner.join());
            std::future::poll_fn(|cx| {
                assert!(shutdown.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
        }
        assert!(owner.task.lock().unwrap().is_some());
        owner.join().await.unwrap();
        assert!(owner.task.lock().unwrap().is_none());
        assert!(retained.upgrade().is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
        let rebound = TcpListener::bind(address).await.unwrap();
        assert!(owner
            .start(rebound, Duration::from_secs(1), |_| async {})
            .is_err());
        owner.join().await.unwrap();
    })
    .await
    .expect("application listener shutdown deadline");
}

#[tokio::test]
async fn request_capacity_is_64_and_completed_handles_are_reaped() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let owner = Listener::default();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (entered, mut started) = tokio::sync::mpsc::channel(64);
        let dropped = Arc::new(AtomicUsize::new(0));
        let disposed = dropped.clone();
        owner
            .start(listener, Duration::from_secs(35), move |mut stream| {
                let entered = entered.clone();
                let dropped = disposed.clone();
                async move {
                    let _disposed = Capture(dropped);
                    entered.send(()).await.unwrap();
                    let _ = stream.read(&mut [0]).await;
                }
            })
            .unwrap();
        let mut clients = Vec::new();
        for _ in 0..64 {
            clients.push(TcpStream::connect(address).await.unwrap());
            started.recv().await.unwrap();
        }
        let mut rejected = TcpStream::connect(address).await.unwrap();
        assert_eq!(rejected.read(&mut [0]).await.unwrap(), 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);

        clients[0].write_all(&[1]).await.unwrap();
        assert_eq!(clients[0].read(&mut [0]).await.unwrap(), 0);
        let mut replacement = TcpStream::connect(address).await.unwrap();
        started.recv().await.unwrap();
        owner.stop();
        owner.join().await.unwrap();
        assert_eq!(dropped.load(Ordering::SeqCst), 65);
        for client in &mut clients {
            assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
        }
        assert_eq!(replacement.read(&mut [0]).await.unwrap(), 0);
    })
    .await
    .expect("bounded listener/request cleanup deadline");
}
