//! Bounded external query framing; payload and success policy stay operation-specific.

use std::borrow::Cow;

use mssf_transport::serialization::{Decoder, Encoder};

use crate::{Error, Result};

pub(crate) fn encode(name: &str, args: &[(&str, Cow<'_, str>)]) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    encoder.object(|encoder| {
        encoder.object(|encoder| {
            encoder.object_array(args, |encoder, (key, value)| {
                encoder.object(|encoder| encoder.string(key))?;
                encoder.string(value)
            })
        })?;
        encoder.string(name)
    })?;
    Ok(encoder.finish())
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
