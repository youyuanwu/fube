use std::fmt::Debug;

use mssf_client::{ApplicationDefinitionKindFilter, Error, HealthEventsFilter, NodeStatusFilter};
use mssf_transport::serialization::{
    serde::{from_slice, to_vec},
    Encoder,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

fn assert_native_mask<T: Debug + PartialEq + Serialize + DeserializeOwned>(value: T, bits: u32) {
    let mut native = Encoder::new();
    native.u32(bits);
    let bytes = to_vec(&value).unwrap();
    assert_eq!(bytes, native.finish());
    assert_eq!(from_slice::<T>(&bytes).unwrap(), value);

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Payload<T> {
        filter: T,
        tail: bool,
    }
    let payload = Payload {
        filter: value,
        tail: true,
    };
    let mut native = Encoder::new();
    native
        .object(|e| {
            e.u32(bits);
            e.boolean(true);
            Ok(())
        })
        .unwrap();
    let bytes = to_vec(&payload).unwrap();
    assert_eq!(bytes, native.finish());
    assert_eq!(from_slice::<Payload<T>>(&bytes).unwrap(), payload);
}

#[test]
fn filter_serde_preserves_native_u32_masks_including_unnamed_bits() {
    for bits in [0, 1, 2, 3, 4, 7, 8, 9, 0xffff, 1 << 31, u32::MAX] {
        assert_native_mask(NodeStatusFilter::from_bits_retain(bits), bits);
        assert_native_mask(HealthEventsFilter::from_bits_retain(bits), bits);
        assert_native_mask(
            ApplicationDefinitionKindFilter::from_bits_retain(bits),
            bits,
        );
    }
}

#[test]
fn node_filter_constants_and_operators_preserve_native_masks() {
    type Filter = NodeStatusFilter;
    assert_eq!(Filter::default(), Filter::DEFAULT);
    assert!(Filter::DEFAULT.is_empty());
    assert_eq!(Filter::ALL.bits(), 0xffff);
    assert_eq!(Filter::all().bits(), u32::MAX);
    for (flag, bits) in [
        (Filter::UP, 1),
        (Filter::DOWN, 2),
        (Filter::ENABLING, 4),
        (Filter::DISABLING, 8),
        (Filter::DISABLED, 16),
        (Filter::UNKNOWN, 32),
        (Filter::REMOVED, 64),
    ] {
        assert_eq!(flag.bits(), bits);
    }
    let mut filter = Filter::from_bits_retain(1 << 31) | Filter::UP | Filter::DOWN;
    filter.remove(Filter::DOWN);
    assert!(filter.contains(Filter::UP));
    assert_eq!(filter.bits(), (1 << 31) | 1);
    assert_eq!((!Filter::UP).bits(), !1u32);
    assert_eq!(Filter::from_bits(u32::MAX), Some(Filter::all()));
}

#[test]
fn health_filter_constants_keep_exact_native_sentinels() {
    type Filter = HealthEventsFilter;
    assert_eq!(Filter::default(), Filter::DEFAULT);
    assert!(Filter::DEFAULT.is_empty());
    assert_eq!(Filter::NONE.bits(), 1);
    assert!(!Filter::NONE.is_empty());
    assert_eq!(Filter::OK.bits(), 2);
    assert_eq!(Filter::WARNING.bits(), 4);
    assert_eq!(Filter::ERROR.bits(), 8);
    assert_eq!(Filter::ALL.bits(), 0xffff);
    assert_eq!(Filter::all().bits(), u32::MAX);
    assert_eq!((Filter::NONE | Filter::ERROR).bits(), 9);
    assert_eq!((!Filter::ERROR).bits(), !8u32);
    assert_eq!(Filter::from_bits(u32::MAX), Some(Filter::all()));
}

#[test]
fn application_filter_checked_constructor_preserves_strict_query_profile() {
    type Filter = ApplicationDefinitionKindFilter;
    assert_eq!(Filter::DEFAULT.bits(), 0);
    assert_eq!(Filter::SERVICE_FABRIC_APPLICATION_DESCRIPTION.bits(), 1);
    assert_eq!(Filter::COMPOSE.bits(), 2);
    assert_eq!(Filter::MESH_APPLICATION_DESCRIPTION.bits(), 4);
    assert_eq!(Filter::ALL.bits(), 65535);
    for bits in (0..=7).chain([65535]) {
        assert_eq!(Filter::try_from_bits(bits).unwrap().bits(), bits);
    }
    for bits in [8, 9, 0xfffe, 0x1_0000, u32::MAX] {
        assert!(matches!(
            Filter::try_from_bits(bits),
            Err(Error::InvalidInput(_))
        ));
    }
    assert!(Filter::from_bits(8).is_some());
    assert!(Filter::from_bits(u32::MAX).is_none());
}
