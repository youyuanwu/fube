use super::*;

#[test]
fn shared_backing_outlives_original_and_is_freed_after_the_last_view() {
    let segment = Segment::from_bytes(b"abcdef".to_vec());
    let backing = Arc::downgrade(&segment.owner);
    let pointer = segment.bytes().as_ptr();
    let body = Body::from_segment(segment).unwrap();
    let slice = body.slice(1..3).unwrap();
    drop(body);
    assert!(backing.upgrade().is_some());
    assert_eq!(slice.segments().next().unwrap(), b"bc");
    assert_eq!(
        slice.segments().next().unwrap().as_ptr(),
        pointer.wrapping_add(1)
    );
    drop(slice);
    assert!(backing.upgrade().is_none());
}

#[test]
fn segmented_views_cross_boundaries_without_copying() {
    let first = Segment::from_bytes(b"abc".to_vec());
    let second = Segment::from_bytes(b"defg".to_vec());
    let pointer = first.bytes().as_ptr();
    let body = Body::from_segments(vec![first, second]).unwrap();
    assert_eq!(body.segments().next().unwrap().as_ptr(), pointer);
    let sliced = body.slice(2..6).unwrap();
    assert_eq!(
        sliced.segments().collect::<Vec<_>>(),
        [b"c".as_slice(), b"def"]
    );
    assert_eq!(
        sliced.slice(1..3).unwrap().segments().next().unwrap(),
        b"de"
    );
    assert_eq!(body.slice(3..3).unwrap().segments().count(), 0);
    assert!(body.slice(6..8).is_err());
    assert!(body.slice(Range { start: 5, end: 4 }).is_err());
}

#[test]
fn assembly_buffers_reject_overflow_and_incomplete_finalization() {
    let mut buffer = SegmentBuffer::new(3).unwrap();
    buffer.extend(b"ab").unwrap();
    assert!(buffer.extend(b"cd").is_err());
    buffer.extend(b"c").unwrap();
    assert_eq!(buffer.finish().unwrap().bytes(), b"abc");
    let mut buffer = SegmentBuffer::new(3).unwrap();
    buffer.extend(b"a").unwrap();
    assert!(buffer.finish().is_err());
    assert!(SegmentBuffer::new(usize::MAX).is_err());
}

#[test]
fn native_valid_bodies_have_no_segment_or_backing_capacity_ceiling() {
    let mut bytes = Vec::with_capacity(4 * 1024 * 1024 + 1);
    bytes.push(7);
    let body = Body::from_bytes(bytes).unwrap();
    let segment = body.owned_segments().next().unwrap();
    let body = Body::from_segments(vec![segment; 1025]).unwrap();
    assert_eq!(body.len(), 1025);
    assert_eq!(body.segments().count(), 1025);
    assert!(body.segments().all(|bytes| bytes == [7]));
}
