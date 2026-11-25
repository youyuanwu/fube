use std::{
    cell::Cell,
    future::{pending, ready, Future},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use furpc::{
    codec, header, Body, ClientCodecLimits, CodecError, DecodeBuf, Decoder, EncodeBuf, Encoder,
    Error, FubstDecoder, FubstEncoder, Request, Response, RpcClient,
};
use futures_util::FutureExt;
use tokio::{
    sync::oneshot,
    time::{advance, sleep, Instant, Sleep},
};
use tower_service::Service;

struct Function<F> {
    f: F,
    ready: Cell<bool>,
    delay: Option<Pin<Box<Sleep>>>,
}

fn service<F>(f: F) -> Function<F> {
    Function {
        f,
        ready: Cell::new(false),
        delay: None,
    }
}

impl<F, Fut> Service<Request<Body>> for Function<F>
where
    F: FnMut(Request<Body>) -> Fut,
    Fut: Future<Output = furpc::Result<Response<Body>>>,
{
    type Response = Response<Body>;
    type Error = Error;
    type Future = Fut;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        if let Some(delay) = &mut self.delay {
            std::task::ready!(delay.as_mut().poll(cx));
        }
        self.ready.set(true);
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Fut {
        assert!(
            self.ready.replace(false),
            "same-instance readiness required"
        );
        (self.f)(request)
    }
}

#[derive(Default)]
struct Counts {
    encoded: AtomicUsize,
    decoded: AtomicUsize,
}

// Neither direction state is Clone or Sync.
struct TextEncoder(Arc<Counts>, Cell<bool>);
struct TextDecoder(Arc<Counts>, Cell<bool>);

struct Text(String);
impl From<&str> for Text {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl Encoder for TextEncoder {
    type Item = Text;

    fn encode(&mut self, item: Text, dst: &mut EncodeBuf<'_>) -> Result<(), CodecError> {
        assert!(!self.1.replace(true));
        self.0.encoded.fetch_add(1, Ordering::SeqCst);
        dst.write_all(item.0.as_bytes())
    }
}
impl Decoder for TextDecoder {
    type Item = String;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<String, CodecError> {
        assert!(!self.1.replace(true));
        self.0.decoded.fetch_add(1, Ordering::SeqCst);
        String::from_utf8(src.copy_remaining()?).map_err(CodecError::invalid_payload)
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

#[test]
fn directional_fubst_states_encode_actual_types_and_decode_fragmented_bodies() {
    let bytes = fubst::serde::to_vec("typed").unwrap();
    let encoded = codec::encode(
        &mut FubstEncoder::<String>::default(),
        "typed".into(),
        bytes.len(),
    )
    .unwrap();
    assert_eq!(encoded.segments().next().unwrap(), bytes);
    let fragmented = Body::from_segments(vec![
        encoded.slice(0..3).unwrap(),
        encoded.slice(3..encoded.len()).unwrap(),
    ])
    .unwrap();
    assert_eq!(
        codec::decode(
            &mut FubstDecoder::<String>::default(),
            &fragmented,
            fragmented.len()
        )
        .unwrap(),
        "typed"
    );
    assert!(matches!(
        codec::decode(
            &mut FubstDecoder::<String>::with_limits(fubst::Limits {
                max_bytes: bytes.len() - 1,
                ..Default::default()
            }),
            &fragmented,
            fragmented.len()
        ),
        Err(CodecError::ResourceLimit { .. })
    ));
}

struct HeaderOffsetEncoder;

impl Encoder for HeaderOffsetEncoder {
    type Item = u32;

    fn encode(&mut self, value: u32, dst: &mut EncodeBuf<'_>) -> Result<(), CodecError> {
        let increment = dst
            .headers()
            .effective()
            .action()
            .iter()
            .copied()
            .eq("Increment".encode_utf16());
        FubstEncoder::<u32>::default().encode(value + u32::from(increment), dst)
    }
}

#[tokio::test]
async fn codecs_always_receive_headers_including_empty_standalone_contexts() {
    let body = codec::encode(&mut HeaderOffsetEncoder, 7, 32).unwrap();
    assert_eq!(
        fubst::serde::from_slice::<u32>(body.segments().next().unwrap()).unwrap(),
        7
    );
    let mut decode = |src: &mut DecodeBuf<'_>| {
        let increment = src
            .headers()
            .effective()
            .action()
            .iter()
            .copied()
            .eq("Increment".encode_utf16());
        let value = FubstDecoder::<u32>::default().decode(src)?;
        Ok::<_, CodecError>(value - u32::from(increment))
    };
    assert_eq!(codec::decode(&mut decode, &body, body.len()).unwrap(), 7);

    let mut rpc = RpcClient::new(service(|request: Request<Body>| {
        ready(Ok(Response::new(request.into_body())))
    }));
    let response = rpc
        .call(
            Request::new(7u32)
                .append_header(header::action("Increment").unwrap())
                .unwrap(),
            HeaderOffsetEncoder,
            FubstDecoder::<u32>::default(),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(response.into_body(), 8);
}

#[tokio::test]
async fn header_aware_client_selects_decoding_inline_without_shared_header_state() {
    let mut rpc = RpcClient::new(service(|request: Request<Body>| {
        assert_eq!(
            fubst::serde::from_slice::<String>(request.body().segments().next().unwrap()).unwrap(),
            "typed"
        );
        let mut response = Response::new(request.into_body());
        response
            .headers_mut()
            .append(header::action("NativeText").unwrap())
            .unwrap();
        ready(Ok(response))
    }));
    let selected = Cell::new(false);
    let decoder = move |src: &mut DecodeBuf<'_>| {
        assert!(!selected.replace(true));
        let headers = src.headers();
        assert!(headers
            .effective()
            .action()
            .iter()
            .copied()
            .eq("NativeText".encode_utf16()));
        FubstDecoder::<String>::default().decode(src)
    };
    let response = rpc
        .call(
            Request::new("typed".to_owned()),
            FubstEncoder::<String>::default(),
            decoder,
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(response.body(), "typed");
    assert!(response
        .headers()
        .effective()
        .action()
        .iter()
        .copied()
        .eq("NativeText".encode_utf16()));
}

#[tokio::test]
async fn header_aware_client_preserves_bounds_complete_consumption_and_error_sources() {
    for mode in 0..4 {
        let decoded = AtomicUsize::new(0);
        let calls = AtomicUsize::new(0);
        let counts = Arc::new(Counts::default());
        let decoded_ref = &decoded;
        let decoder = move |src: &mut DecodeBuf<'_>| {
            assert!(src.headers().records().next().is_none());
            decoded_ref.fetch_add(1, Ordering::SeqCst);
            if mode == 2 {
                return Err(CodecError::resource_limit(std::io::Error::other(
                    "profile budget",
                )));
            }
            src.take(1)
        };
        let mut rpc = RpcClient::new(service(|request: Request<Body>| {
            calls.fetch_add(1, Ordering::SeqCst);
            ready(Ok(Response::new(request.into_body())))
        }))
        .codec_limits(ClientCodecLimits {
            max_encoded_body_bytes: 2,
            max_response_body_bytes: if mode == 0 { 1 } else { 2 },
        })
        .unwrap();
        let result = rpc
            .call(
                Request::new(Text::from("ab")),
                TextEncoder(counts.clone(), Cell::new(false)),
                decoder,
                if mode == 3 {
                    Instant::now()
                } else {
                    deadline()
                },
            )
            .await;
        if mode == 3 {
            assert_eq!(result.unwrap_err(), Error::Timeout("RPC"));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        } else {
            let error = result.unwrap_err();
            assert!(matches!(
                error,
                Error::Codec {
                    phase: "decode",
                    ..
                }
            ));
            if mode == 1 {
                assert!(matches!(
                    error,
                    Error::Codec {
                        source: CodecError::InvalidPayload { .. },
                        ..
                    }
                ));
            } else {
                assert!(matches!(
                    error,
                    Error::Codec {
                        source: CodecError::ResourceLimit { .. },
                        ..
                    }
                ));
            }
            if mode == 2 {
                assert!(std::error::Error::source(&error)
                    .unwrap()
                    .source()
                    .unwrap()
                    .is::<std::io::Error>());
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
        assert_eq!(
            decoded.load(Ordering::SeqCst),
            usize::from(mode == 1 || mode == 2)
        );
    }
}

#[tokio::test]
async fn client_is_inert_and_supports_per_call_codecs_on_the_same_borrowed_service() {
    fn send<T: Send>(value: T) -> T {
        value
    }
    let counts = Arc::new(Counts::default());
    let calls = AtomicUsize::new(0);
    let effective = deadline();
    let transport = service(|request: Request<Body>| {
        calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.deadline(), Some(effective));
        assert_eq!(request.headers().effective().action(), [b'E' as u16]);
        let mut response = Response::new(request.into_body());
        response
            .headers_mut()
            .append(header::action("R").unwrap())
            .unwrap();
        ready(Ok(response))
    });
    // The transport borrows stack data, is not Clone/Sync, and is never cloned.
    let mut rpc = RpcClient::new(transport);
    let call = rpc.call(
        Request::new("unpolled".into()),
        TextEncoder(counts.clone(), Cell::new(false)),
        TextDecoder(counts.clone(), Cell::new(false)),
        effective,
    );
    drop(call);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(counts.encoded.load(Ordering::SeqCst), 0);

    let mut request = Request::new("text".into())
        .append_header(header::action("E").unwrap())
        .unwrap();
    request.set_deadline(effective);
    let response = send(rpc.call(
        request,
        TextEncoder(counts.clone(), Cell::new(false)),
        TextDecoder(counts.clone(), Cell::new(false)),
        effective + Duration::from_secs(1),
    ))
    .await
    .unwrap();
    assert_eq!(response.body(), "text");
    assert!(response.source().is_none());
    assert_eq!(response.headers().effective().action(), [b'R' as u16]);

    let response = rpc
        .call(
            Request::new(42u32)
                .append_header(header::action("E").unwrap())
                .unwrap(),
            FubstEncoder::<u32>::default(),
            FubstDecoder::<u32>::default(),
            effective,
        )
        .await
        .unwrap();
    assert_eq!(response.into_body(), 42);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(counts.encoded.load(Ordering::SeqCst), 1);
    assert_eq!(counts.decoded.load(Ordering::SeqCst), 1);
    drop(rpc.into_inner());
}

#[tokio::test]
async fn fubst_unit_bodies_work_with_zero_limits_but_reject_nonempty_responses() {
    let calls = AtomicUsize::new(0);
    let transport = service(|request: Request<Body>| {
        assert!(request.body().is_empty());
        let body = if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Body::empty()
        } else {
            Body::from_bytes(fubst::serde::to_vec(&false).unwrap()).unwrap()
        };
        ready(Ok(Response::new(body)))
    });
    let mut rpc = RpcClient::new(transport)
        .codec_limits(ClientCodecLimits {
            max_encoded_body_bytes: 0,
            max_response_body_bytes: 0,
        })
        .unwrap();
    rpc.call(
        Request::new(()),
        FubstEncoder::<()>::with_limits(fubst::Limits {
            max_bytes: 0,
            max_depth: 0,
        }),
        FubstDecoder::<()>::with_limits(fubst::Limits {
            max_bytes: 0,
            max_depth: 0,
        }),
        deadline(),
    )
    .await
    .unwrap()
    .into_body();
    let mut rpc = rpc
        .codec_limits(ClientCodecLimits {
            max_encoded_body_bytes: 0,
            max_response_body_bytes: 1,
        })
        .unwrap();
    assert!(matches!(
        rpc.call(
            Request::new(()),
            FubstEncoder::<()>::default(),
            FubstDecoder::<()>::default(),
            deadline()
        )
        .await,
        Err(Error::Codec {
            phase: "decode",
            source: CodecError::InvalidPayload { .. },
        })
    ));
}

#[tokio::test]
async fn explicit_body_limits_apply_before_submission_and_before_decode_including_zero() {
    for (out, input, text, expect_encode, expect_call) in [
        (0, 0, "", true, true),
        (0, 10, "x", false, false),
        (2, 1, "xx", true, true),
        (2, 2, "xx", true, true),
    ] {
        let counts = Arc::new(Counts::default());
        let calls = AtomicUsize::new(0);
        let mut rpc = RpcClient::new(service(|request: Request<Body>| {
            calls.fetch_add(1, Ordering::SeqCst);
            ready(Ok(Response::new(request.into_body())))
        }))
        .codec_limits(ClientCodecLimits {
            max_encoded_body_bytes: out,
            max_response_body_bytes: input,
        })
        .unwrap();
        let result = rpc
            .call(
                Request::new(text.into()),
                TextEncoder(counts.clone(), Cell::new(false)),
                TextDecoder(counts.clone(), Cell::new(false)),
                deadline(),
            )
            .await;
        if text.len() <= out && text.len() <= input {
            assert_eq!(result.unwrap().body(), text);
        } else {
            assert!(matches!(
                result,
                Err(Error::Codec {
                    phase,
                    source: CodecError::ResourceLimit { .. }
                }) if phase == if expect_encode { "decode" } else { "encode" }
            ));
        }
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(expect_call));
        assert_eq!(
            counts.decoded.load(Ordering::SeqCst),
            usize::from(expect_call && text.len() <= input)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn deadlines_cover_readiness_and_prevent_late_submission() {
    for readiness_delay in [0, 5, 10] {
        let counts = Arc::new(Counts::default());
        let calls = AtomicUsize::new(0);
        let mut transport = service(|_: Request<Body>| {
            calls.fetch_add(1, Ordering::SeqCst);
            ready(Ok(Response::new(Body::empty())))
        });
        transport.delay = Some(Box::pin(sleep(Duration::from_secs(readiness_delay))));
        let mut rpc = RpcClient::new(transport);
        let mut request = Request::new("".into());
        let local = if readiness_delay == 0 {
            Instant::now()
        } else {
            deadline()
        };
        request.set_deadline(local);
        let result = rpc
            .call(
                request,
                TextEncoder(counts.clone(), Cell::new(false)),
                TextDecoder(counts.clone(), Cell::new(false)),
                local + Duration::from_secs(20),
            )
            .await;
        assert_eq!(result.unwrap_err(), Error::Timeout("RPC"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            counts.encoded.load(Ordering::SeqCst),
            usize::from(readiness_delay != 0)
        );
        assert_eq!(counts.decoded.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn synthetic_results_observed_late_timeout_before_decode() {
    let counts = Arc::new(Counts::default());
    let (respond, response) = oneshot::channel();
    let mut response = Some(response);
    let mut rpc = RpcClient::new(service(move |_: Request<Body>| {
        let response = response.take().unwrap();
        async move { response.await.unwrap() }
    }));
    let mut call = Box::pin(rpc.call(
        Request::new("echo".into()),
        TextEncoder(counts.clone(), Cell::new(false)),
        TextDecoder(counts.clone(), Cell::new(false)),
        deadline(),
    ));
    assert!(call.as_mut().now_or_never().is_none());
    respond
        .send(Ok(Response::new(
            Body::from_bytes(b"echo".to_vec()).unwrap(),
        )))
        .unwrap();
    advance(Duration::from_secs(6)).await;
    assert_eq!(call.await.unwrap_err(), Error::Timeout("RPC"));
    assert_eq!(counts.decoded.load(Ordering::SeqCst), 0);
}

struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn dropping_or_expiring_calls_drops_the_service_future_without_retry() {
    for expire in [false, true] {
        let counts = Arc::new(Counts::default());
        let dropped = Arc::new(AtomicUsize::new(0));
        let calls = AtomicUsize::new(0);
        let mut rpc = RpcClient::new(service(|_: Request<Body>| {
            calls.fetch_add(1, Ordering::SeqCst);
            let guard = DropCount(dropped.clone());
            async move {
                let _guard = guard;
                pending::<furpc::Result<Response<Body>>>().await
            }
        }));
        let mut call = Box::pin(rpc.call(
            Request::new("".into()),
            TextEncoder(counts.clone(), Cell::new(false)),
            TextDecoder(counts.clone(), Cell::new(false)),
            deadline(),
        ));
        assert!(call.as_mut().now_or_never().is_none());
        if expire {
            assert_eq!(call.await.unwrap_err(), Error::Timeout("RPC"));
        } else {
            drop(call);
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(counts.decoded.load(Ordering::SeqCst), 0);
    }
}

struct PartialDecoder;
impl Decoder for PartialDecoder {
    type Item = Body;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Body, CodecError> {
        src.take(1)
    }
}

#[tokio::test]
async fn decoder_errors_and_unconsumed_bytes_are_call_local_with_original_sources() {
    let counts = Arc::new(Counts::default());
    let mut rpc = RpcClient::new(service(|_: Request<Body>| {
        ready(Ok(Response::new(
            Body::from_bytes(vec![0xff, 0xff]).unwrap(),
        )))
    }));
    let error = rpc
        .call(
            Request::new("".into()),
            TextEncoder(counts.clone(), Cell::new(false)),
            TextDecoder(counts.clone(), Cell::new(false)),
            deadline(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Codec {
            phase: "decode",
            source: CodecError::InvalidPayload { .. }
        }
    ));
    let source = std::error::Error::source(&error).unwrap();
    assert!(source.source().unwrap().is::<std::string::FromUtf8Error>());

    assert!(matches!(
        rpc.call(
            Request::new("".into()),
            TextEncoder(counts.clone(), Cell::new(false)),
            PartialDecoder,
            deadline()
        )
        .await,
        Err(Error::Codec {
            phase: "decode",
            source: CodecError::InvalidPayload { .. }
        })
    ));
    let calls = AtomicUsize::new(0);
    let mut rpc = RpcClient::new(service(|_: Request<Body>| {
        calls.fetch_add(1, Ordering::SeqCst);
        ready(Err(Error::Closed))
    }));
    assert_eq!(
        rpc.call(
            Request::new("".into()),
            TextEncoder(counts.clone(), Cell::new(false)),
            TextDecoder(counts.clone(), Cell::new(false)),
            deadline()
        )
        .await
        .unwrap_err(),
        Error::Closed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.decoded.load(Ordering::SeqCst), 1);
}

#[test]
fn public_body_helpers_reject_invalid_limits_and_oversized_input_before_codec_work() {
    let counts = Arc::new(Counts::default());
    let invalid = u32::MAX as usize - 11;
    let mut encoder = TextEncoder(counts.clone(), Cell::new(false));
    let error = codec::encode(&mut encoder, "".into(), invalid).unwrap_err();
    assert!(matches!(error, CodecError::InvalidPayload { .. }));
    assert!(std::error::Error::source(&error).unwrap().is::<Error>());
    assert_eq!(counts.encoded.load(Ordering::SeqCst), 0);

    let body = Body::from_bytes(b"x".to_vec()).unwrap();
    let mut decoder = TextDecoder(counts.clone(), Cell::new(false));
    assert!(matches!(
        codec::decode(&mut decoder, &body, invalid),
        Err(CodecError::InvalidPayload { .. })
    ));
    assert!(matches!(
        codec::decode(&mut decoder, &body, 0),
        Err(CodecError::ResourceLimit { .. })
    ));
    assert_eq!(counts.decoded.load(Ordering::SeqCst), 0);
    let empty = codec::encode(&mut encoder, "".into(), 0).unwrap();
    assert_eq!(codec::decode(&mut decoder, &empty, 0).unwrap(), "");
}

struct SharedEncoder;
impl Encoder for SharedEncoder {
    type Item = Body;

    fn encode(&mut self, item: Body, dst: &mut EncodeBuf<'_>) -> Result<(), CodecError> {
        dst.append_body(item)
    }
}

struct ViewDecoder {
    skip_tail: bool,
}
impl Decoder for ViewDecoder {
    type Item = Body;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Body, CodecError> {
        let value = src.take(1)?;
        if self.skip_tail {
            src.advance(src.remaining())?;
        }
        Ok(value)
    }
}

#[test]
fn public_body_helpers_preserve_segments_and_require_explicit_suffix_consumption() {
    let shared = Body::from_bytes(b"ab".to_vec()).unwrap();
    let pointer = shared.segments().next().unwrap().as_ptr();
    let body =
        Body::from_segments(vec![shared, Body::from_bytes(b"cd".to_vec()).unwrap()]).unwrap();
    let body = codec::encode(&mut SharedEncoder, body, 4).unwrap();
    assert_eq!(
        body.segments().collect::<Vec<_>>(),
        [b"ab".as_slice(), b"cd"]
    );
    assert_eq!(body.segments().next().unwrap().as_ptr(), pointer);
    assert!(matches!(
        codec::decode(&mut ViewDecoder { skip_tail: false }, &body, 4),
        Err(CodecError::InvalidPayload { .. })
    ));
    let view = codec::decode(&mut ViewDecoder { skip_tail: true }, &body, 4).unwrap();
    assert_eq!(view.segments().next().unwrap(), b"a");
    assert_eq!(view.segments().next().unwrap().as_ptr(), pointer);
}
