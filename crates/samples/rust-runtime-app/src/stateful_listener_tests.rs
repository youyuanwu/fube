use super::*;

#[tokio::test]
async fn peer_listener_shutdown_joins_incomplete_requests_and_releases_server() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let server = PeerServer::new("127.0.0.1".into(), 0);
        let retained = Arc::downgrade(&server);
        let address = server.listen().await.unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(b"GET /incomplete").await.unwrap();
        server.stop();
        server.join().await.unwrap();
        // A connection still queued at stop may be reset rather than EOF.
        assert!(!matches!(client.read(&mut [0]).await, Ok(n) if n != 0));
        let _rebound = TcpListener::bind(address).await.unwrap();
        drop(server);
        assert!(retained.upgrade().is_none());
    })
    .await
    .expect("peer listener joined cleanup deadline");
}
