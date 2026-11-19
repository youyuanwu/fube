use furpc::{header, ActorId, Body, HeaderId, HeaderRecord, Headers, Message};

#[test]
fn public_construction_and_segmented_body_views_need_no_runtime() {
    let first = Body::from_bytes(b"abc".to_vec()).unwrap();
    let first_pointer = first.segments().next().unwrap().as_ptr();
    let second = Body::try_from(b"def".to_vec()).unwrap();
    let body = Body::from_segments(vec![first, second]).unwrap();
    assert_eq!(body.segments().next().unwrap().as_ptr(), first_pointer);
    let slice = body.slice(2..5).unwrap();
    assert_eq!(
        slice.segments().collect::<Vec<_>>(),
        [b"c".as_slice(), b"de"]
    );
    let message = Message::builder()
        .append_header(header::actor(ActorId::new(70000).unwrap()).unwrap())
        .append_header(header::action("OneWay").unwrap())
        .body(body)
        .build()
        .unwrap();
    assert_eq!(message.body().len(), 6);
    assert_eq!(message.headers().first_actor().unwrap().get(), 70000);
    assert_eq!(
        message.headers().first_action().unwrap(),
        "OneWay".encode_utf16().collect::<Vec<_>>()
    );
}

#[test]
fn unknown_records_remain_opaque_and_forward_as_owned_records() {
    let id = HeaderId::new(0x9001).unwrap();
    let mut headers = Headers::new();
    headers
        .append(HeaderRecord::from_encoded(id, vec![0xff]).unwrap())
        .unwrap();
    let record = headers.first_raw(id).unwrap().to_owned();
    drop(headers);
    let message = Message::builder().append_header(record).build().unwrap();
    assert_eq!(message.headers().first_raw(id).unwrap().value(), [0xff]);
    assert!(message.headers().first_actor().is_none());
}
