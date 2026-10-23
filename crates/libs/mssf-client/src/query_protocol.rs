//! Bounded external query framing; payload and success policy stay operation-specific.

use std::borrow::Cow;

use mssf_transport::serialization::{
    serde::{to_vec, ObjectArray},
    Decoder, MAX_COLLECTION_COUNT,
};
use serde::Serialize;

use crate::{Error, Result};

pub(crate) fn encode(name: &str, args: &[(&str, Cow<'_, str>)]) -> Result<Vec<u8>> {
    #[derive(Serialize)]
    struct Key<'a> {
        value: &'a str,
    }
    #[derive(Serialize)]
    struct Pair<'a>(Key<'a>, &'a str);
    #[derive(Serialize)]
    struct Arguments<T> {
        values: ObjectArray<T>,
    }
    #[derive(Serialize)]
    struct Request<'a, T>(Arguments<T>, &'a str);
    if args.len() > MAX_COLLECTION_COUNT {
        return Err(Error::InvalidInput("object-array count limit exceeded"));
    }
    let pairs: Vec<_> = args
        .iter()
        .map(|(key, value)| Pair(Key { value: key }, value.as_ref()))
        .collect();
    to_vec(&Request(
        Arguments {
            values: ObjectArray(pairs),
        },
        name,
    ))
}

pub(crate) struct QueryResult<T> {
    pub kind: i64,
    pub payload: Option<T>,
    pub paging: Option<String>,
}

pub(crate) fn decode<T>(
    body: &[u8],
    payload: impl FnOnce(&mut Decoder<'_>, &[u8]) -> Result<T>,
) -> Result<QueryResult<T>> {
    decode_with_paging_policy(body, payload, false)
}

pub(crate) fn decode_with_paging_policy<T>(
    body: &[u8],
    payload: impl FnOnce(&mut Decoder<'_>, &[u8]) -> Result<T>,
    strict_paging: bool,
) -> Result<QueryResult<T>> {
    let mut decoder = Decoder::new(body);
    let (kind, payload, error, paging) = decoder.object(|decoder| {
        Ok((
            decoder.i64()?,
            decoder.pointer(payload)?,
            decoder.i64()?,
            decoder.pointer(|decoder, tag| {
                if strict_paging && !tag.is_empty() {
                    return Err(Error::Protocol("unexpected paging status type"));
                }

                decoder.string()
            })?,
        ))
    })?;
    decoder.finish()?;
    if error != 0 {
        return Err(Error::Remote {
            code: error,
            message: String::new(),
        });
    }
    Ok(QueryResult {
        kind,
        payload,
        paging,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mssf_transport::serialization::Encoder;

    #[test]
    fn derived_query_bytes_match_old_order_and_nesting_including_empty_arguments() {
        for args in [
            vec![],
            vec![
                ("z", Cow::Borrowed("")),
                ("a", Cow::Borrowed("🦀é")),
                ("z", Cow::Borrowed("duplicate")),
            ],
        ] {
            let mut old = Encoder::new();
            old.object(|e| {
                e.object(|e| {
                    e.object_array(&args, |e, (key, value)| {
                        e.object(|e| e.string(key))?;
                        e.string(value)
                    })
                })?;
                e.string("Get🦀")
            })
            .unwrap();
            assert_eq!(encode("Get🦀", &args).unwrap(), old.finish());
        }
        let too_many = vec![("key", Cow::Borrowed("value")); MAX_COLLECTION_COUNT + 1];
        assert!(matches!(
            encode("Query", &too_many),
            Err(Error::InvalidInput(_))
        ));
    }
}
