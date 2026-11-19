use super::*;
use crate::resource::ByteBudget;
use std::sync::Barrier;

fn limits() -> BodyLimits {
    BodyLimits {
        max_body_bytes: 4096,
        max_segments: 16,
        max_retained_bytes: 64 * 1024,
        max_ledgers: 4,
    }
}

fn budget() -> ByteBudget {
    ByteBudget::new(1024 * 1024).unwrap()
}

fn bytes(body: &Body) -> Vec<u8> {
    body.segments().flatten().copied().collect()
}

fn scratch_bytes(body: &Body) -> usize {
    let owner = body.owner.as_ref().unwrap();
    (owner.data().unique.len() + 1) * size_of::<Option<AllocationAdmission<'_>>>()
}

#[test]
fn construction_adopts_bytes_without_scope_admission_or_flattening() {
    let mut input = Vec::with_capacity(256);
    input.extend_from_slice(b"abc");
    let pointer = input.as_ptr();
    let local = budget();
    let shared = budget();
    let body = Body::from_bytes(input, limits()).unwrap();
    assert_eq!(body.len(), 3);
    assert!(body.retained_bytes() >= 256);
    assert_eq!(body.segments().next().unwrap().as_ptr(), pointer);
    assert_eq!(local.used(), 0);
    assert_eq!(shared.used(), 0);
    body.admit(&ByteScope::new(local.clone(), Some(shared.clone())))
        .unwrap();
    assert_eq!(local.used(), body.retained_bytes());
    assert_eq!(shared.used(), body.retained_bytes());
}

#[test]
fn aliases_created_before_admission_keep_full_charges_until_storage_is_freed() {
    let mut input = Vec::with_capacity(512);
    input.extend_from_slice(b"abc");
    let original = Body::from_bytes(input, limits()).unwrap();
    let alias = original.clone();
    let slice = alias.slice(1..2).unwrap();
    let empty_slice = slice.slice(0..0).unwrap();
    let local = budget();
    let scope = ByteScope::new(local.clone(), None);
    let retained = original.retained_bytes();
    original.admit(&scope).unwrap();
    alias.admit(&scope).unwrap();
    slice.admit(&scope).unwrap();
    assert_eq!(local.used(), retained);
    assert_eq!(bytes(&slice), b"b");
    assert_eq!(slice.retained_bytes(), retained);
    drop(original);
    drop(alias);
    drop(slice);
    drop(scope);
    assert!(empty_slice.is_empty());
    assert_eq!(local.used(), retained);
    drop(empty_slice);
    assert_eq!(local.used(), 0);
}

#[test]
fn segmented_views_cross_boundaries_without_copying_or_detaching_owners() {
    let first = Segment::from_bytes(b"abc".to_vec(), limits()).unwrap();
    let second = Segment::from_bytes(b"defg".to_vec(), limits()).unwrap();
    let pointer = first.bytes().as_ptr();
    let body = Body::from_segments(vec![first, second], limits()).unwrap();
    assert_eq!(
        body.segments().collect::<Vec<_>>(),
        [b"abc".as_slice(), b"defg"]
    );
    assert_eq!(body.segments().next().unwrap().as_ptr(), pointer);
    let sliced = body.slice(2..6).unwrap();
    assert_eq!(
        sliced.segments().collect::<Vec<_>>(),
        [b"c".as_slice(), b"def"]
    );
    assert_eq!(bytes(&sliced.slice(1..3).unwrap()), b"de");
    assert_eq!(body.slice(3..3).unwrap().segments().count(), 0);
    assert!(body.slice(6..8).is_err());
    assert!(body.slice(Range { start: 5, end: 4 }).is_err());
    assert!(body.slice(0..usize::MAX).is_err());
}

#[test]
fn repeated_backing_allocations_and_same_ledger_are_charged_once() {
    let source = Segment::from_bytes(b"abcdef".to_vec(), limits()).unwrap();
    let data_charge = source.owner.retained_bytes();
    let body = Body::from_segments(
        vec![source.slice(0..2).unwrap(), source.slice(3..6).unwrap()],
        limits(),
    )
    .unwrap();
    assert_eq!(bytes(&body), b"abdef");
    assert_eq!(body.owner.as_ref().unwrap().data().unique.len(), 1);
    let metadata_charge = body.owner.as_ref().unwrap().retained_bytes();
    let local = budget();
    let scope = ByteScope::new(local.clone(), Some(local.clone()));
    body.admit(&scope).unwrap();
    assert_eq!(local.used(), data_charge + metadata_charge);
    drop(body);
    assert_eq!(local.used(), data_charge);
    drop(source);
    assert_eq!(local.used(), 0);
}

#[test]
fn cross_scope_reuse_preserves_original_leases_and_deduplicates_shared_bytes() {
    let body = Body::from_bytes(b"data".to_vec(), limits()).unwrap();
    let retained = body.retained_bytes();
    let first_local = budget();
    let second_local = budget();
    let shared = budget();
    let first = ByteScope::new(first_local.clone(), Some(shared.clone()));
    let second = ByteScope::new(second_local.clone(), Some(shared.clone()));
    body.admit(&first).unwrap();
    body.admit(&second).unwrap();
    assert_eq!(first_local.used(), retained);
    assert_eq!(second_local.used(), retained);
    assert_eq!(shared.used(), retained);
    drop(first);
    drop(second);
    assert_eq!(shared.used(), retained);
    drop(body);
    assert_eq!(first_local.used(), 0);
    assert_eq!(second_local.used(), 0);
    assert_eq!(shared.used(), 0);
}

#[test]
fn repackaging_segments_preserves_inherited_leases_and_charges_new_metadata() {
    let segment = Segment::from_bytes(b"abc".to_vec(), limits()).unwrap();
    let backing_charge = segment.owner.retained_bytes();
    let first = Body::from_segments(vec![segment.clone()], limits()).unwrap();
    let local = budget();
    let scope = ByteScope::new(local.clone(), None);
    first.admit(&scope).unwrap();
    let second = Body::from_segments(vec![segment], limits()).unwrap();
    assert_eq!(local.used(), first.retained_bytes());
    second.admit(&scope).unwrap();
    let first_meta = first.owner.as_ref().unwrap().retained_bytes();
    let second_meta = second.owner.as_ref().unwrap().retained_bytes();
    assert_eq!(local.used(), backing_charge + first_meta + second_meta);
    drop(first);
    assert_eq!(local.used(), second.retained_bytes());
    drop(second);
    assert_eq!(local.used(), 0);
}

#[test]
fn partial_multi_allocation_admission_rolls_back_both_ledgers() {
    let body = Body::from_segments(
        vec![
            Segment::from_bytes(vec![1; 16], limits()).unwrap(),
            Segment::from_bytes(vec![2; 32], limits()).unwrap(),
        ],
        limits(),
    )
    .unwrap();
    let insufficient = body.retained_bytes() + scratch_bytes(&body) - 1;
    for shared_fails in [false, true] {
        let local = ByteBudget::new(if shared_fails {
            1024 * 1024
        } else {
            insufficient
        })
        .unwrap();
        let shared = ByteBudget::new(if shared_fails {
            insufficient
        } else {
            1024 * 1024
        })
        .unwrap();
        let scope = ByteScope::new(local.clone(), Some(shared.clone()));
        assert!(matches!(body.admit(&scope), Err(Error::ResourceLimit(_))));
        assert_eq!(local.used(), 0);
        assert_eq!(shared.used(), 0);
    }
    let local = budget();
    body.admit(&ByteScope::new(local.clone(), None)).unwrap();
    assert_eq!(local.used(), body.retained_bytes());
}

#[test]
fn ledger_slot_exhaustion_preserves_existing_charges_and_rolls_back_new_ones() {
    let segment = Segment::from_bytes(
        b"data".to_vec(),
        BodyLimits {
            max_ledgers: 1,
            ..limits()
        },
    )
    .unwrap();
    let body = Body::from_segments(vec![segment], limits()).unwrap();
    let original = budget();
    body.admit(&ByteScope::new(original.clone(), None)).unwrap();
    let retained = body.retained_bytes();
    let other = budget();
    let shared = budget();
    assert_eq!(
        body.admit(&ByteScope::new(other.clone(), Some(shared.clone()))),
        Err(Error::ResourceLimit("allocation ledger slots"))
    );
    assert_eq!(original.used(), retained);
    assert_eq!(other.used(), 0);
    assert_eq!(shared.used(), 0);
    drop(body);
    assert_eq!(original.used(), 0);
}

#[test]
fn construction_limits_check_lengths_backing_capacity_and_segment_metadata() {
    assert!(matches!(
        Body::from_bytes(
            vec![1; 3],
            BodyLimits {
                max_body_bytes: 2,
                ..limits()
            }
        ),
        Err(Error::ResourceLimit(_))
    ));
    let mut bytes = Vec::with_capacity(4096);
    bytes.push(1);
    assert!(matches!(
        Body::from_bytes(
            bytes,
            BodyLimits {
                max_retained_bytes: 512,
                ..limits()
            }
        ),
        Err(Error::ResourceLimit(_))
    ));
    let segment = Segment::from_bytes(vec![1], limits()).unwrap();
    assert!(matches!(
        Body::from_segments(
            vec![segment.clone(), segment.clone()],
            BodyLimits {
                max_segments: 1,
                ..limits()
            }
        ),
        Err(Error::ResourceLimit("body segment count"))
    ));
    let mut segments = Vec::with_capacity(64);
    segments.push(segment);
    assert!(matches!(
        Body::from_segments(
            segments,
            BodyLimits {
                max_retained_bytes: 1024,
                ..limits()
            }
        ),
        Err(Error::ResourceLimit(_))
    ));
    assert!(matches!(
        Body::from_bytes(
            vec![1],
            BodyLimits {
                max_ledgers: usize::MAX - 1,
                ..limits()
            }
        ),
        Err(Error::ResourceLimit(_))
    ));
}

#[test]
fn empty_bodies_need_no_reservation_and_readmission_needs_no_new_headroom() {
    let empty = Body::empty();
    let zero = ByteBudget::new(0).unwrap();
    empty.admit(&ByteScope::new(zero.clone(), None)).unwrap();
    assert_eq!(zero.used(), 0);
    assert_eq!(empty.retained_bytes(), 0);
    assert_eq!(empty.segments().count(), 0);

    let body = Body::from_bytes(vec![1; 8], limits()).unwrap();
    let local = ByteBudget::new(body.retained_bytes() + scratch_bytes(&body)).unwrap();
    let scope = ByteScope::new(local.clone(), None);
    body.admit(&scope).unwrap();
    let headroom = local.try_reserve(scratch_bytes(&body)).unwrap();
    body.clone().admit(&scope).unwrap();
    drop(headroom);
    assert_eq!(local.used(), body.retained_bytes());
}

#[test]
fn concurrent_bodies_share_allocation_charges_without_lock_order_inversion() {
    let first = Segment::from_bytes(vec![1], limits()).unwrap();
    let second = Segment::from_bytes(vec![2], limits()).unwrap();
    let backing = first.owner.retained_bytes() + second.owner.retained_bytes();
    let a = Body::from_segments(vec![first.clone(), second.clone()], limits()).unwrap();
    let b = Body::from_segments(vec![second, first], limits()).unwrap();
    let expected = backing
        + a.owner.as_ref().unwrap().retained_bytes()
        + b.owner.as_ref().unwrap().retained_bytes();
    let local = budget();
    let shared = budget();
    let scope = ByteScope::new(local.clone(), Some(shared.clone()));
    let barrier = Barrier::new(2);
    std::thread::scope(|threads| {
        let left = threads.spawn(|| {
            barrier.wait();
            a.admit(&scope).unwrap();
        });
        let right = threads.spawn(|| {
            barrier.wait();
            b.admit(&scope).unwrap();
        });
        left.join().unwrap();
        right.join().unwrap();
    });
    assert_eq!(local.used(), expected);
    assert_eq!(shared.used(), expected);
    drop(a);
    drop(b);
    assert_eq!(local.used(), 0);
    assert_eq!(shared.used(), 0);
}
