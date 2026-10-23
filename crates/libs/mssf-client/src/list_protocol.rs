//! Source-derived list aggregate envelopes and positional record layouts.

use std::{borrow::Cow, collections::BTreeMap, num::NonZeroU64};

use mssf_transport::{
    message::{Message, ACTION, ACTOR},
    serialization::{serde::from_fields, Decoder, MAX_COLLECTION_COUNT},
};
use serde::Deserialize;

use crate::{applications::*, services::*, Error, HealthState, Result};

fn paging<'a>(
    args: &mut Vec<(&'static str, Cow<'a, str>)>,
    token: &'a Option<String>,
    max: Option<NonZeroU64>,
) -> Result<()> {
    if let Some(token) = token.as_deref().filter(|s| !s.is_empty()) {
        args.push(("ContinuationToken", Cow::Borrowed(token)));
    }
    if let Some(max) = max {
        if max.get() > i64::MAX as u64 {
            return Err(Error::InvalidInput(
                "MaxResults exceeds native signed range",
            ));
        }
        args.push(("MaxResults", Cow::Owned(max.to_string())));
    }
    Ok(())
}

fn type_selector<'a>(
    args: &mut Vec<(&'static str, Cow<'a, str>)>,
    key: &'static str,
    value: &'a str,
) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidInput("selected type name must not be empty"));
    }
    args.push((key, Cow::Borrowed(value)));
    Ok(())
}

pub(crate) fn encode_application(query: &ApplicationQueryDescription) -> Result<Vec<u8>> {
    let mut args = Vec::new();
    match &query.filter {
        ApplicationListFilter::All => {}
        ApplicationListFilter::Name(name) => {
            crate::protocol::validate_name(name)?;
            args.push(("ApplicationName", Cow::Borrowed(name.as_str())));
        }
        ApplicationListFilter::TypeName(name) => {
            type_selector(&mut args, "ApplicationTypeName", name)?
        }
        ApplicationListFilter::DefinitionKind(filter) => {
            if filter.bits() != 0 {
                args.push((
                    "ApplicationDefinitionKindFilter",
                    Cow::Owned(filter.bits().to_string()),
                ));
            }
        }
    }
    if query.exclude_application_parameters {
        args.push(("ExcludeApplicationParameters", Cow::Borrowed("true")));
    }
    paging(&mut args, &query.continuation_token, query.max_results)?;
    crate::query_protocol::encode("GetApplicationList", &args)
}

pub(crate) fn encode_service(query: &ServiceQueryDescription) -> Result<Vec<u8>> {
    crate::protocol::validate_name(&query.application_name)?;
    if query.application_name == "fabric:/System" {
        return Err(Error::InvalidInput(
            "System service-list profile is unsupported",
        ));
    }
    let mut args = vec![(
        "ApplicationName",
        Cow::Borrowed(query.application_name.as_str()),
    )];
    match &query.filter {
        ServiceListFilter::All => {}
        ServiceListFilter::Name(name) => {
            crate::protocol::validate_name(name)?;
            args.push(("ServiceName", Cow::Borrowed(name.as_str())));
        }
        ServiceListFilter::TypeName(name) => type_selector(&mut args, "ServiceTypeName", name)?,
    }
    paging(&mut args, &query.continuation_token, query.max_results)?;
    crate::query_protocol::encode("GetApplicationServiceList", &args)
}

fn decode_list<T>(
    message: &Message,
    wrapper: i32,
    record: fn(&mut Decoder<'_>) -> Result<T>,
) -> Result<(Vec<T>, Option<String>)> {
    crate::protocol::check_failure(message)?;
    let properties = message.properties()?;
    // Both native aggregates construct bare Message(QueryResult). Validate
    // actor/action if present; absence is not a fabricated default header.
    if message.headers.iter().any(|h| h.id == ACTOR) && properties.actor != 5 {
        return Err(Error::Protocol("list reply is not from NamingGateway"));
    }
    if message.headers.iter().any(|h| h.id == ACTION) && properties.action != "QueryReply" {
        return Err(Error::Protocol("unexpected list reply action"));
    }
    let result = crate::query_protocol::decode_with_paging_policy(
        &message.body,
        |decoder, tag| {
            if tag != wrapper.to_le_bytes() {
                return Err(Error::Protocol("unexpected native list wrapper type"));
            }
            let rows = decoder.object_array(record)?;
            if decoder.pointer(|_, _| Ok(()))?.is_some() {
                return Err(Error::Protocol("list has a single-item payload"));
            }
            Ok(rows)
        },
        true,
    )?;
    if result.kind != 2 {
        return Err(Error::Protocol("query result is not a List"));
    }
    Ok((
        result
            .payload
            .ok_or(Error::Protocol("list has no typed wrapper"))?,
        result.paging.filter(|s| !s.is_empty()),
    ))
}

pub(crate) fn decode_applications(message: &Message) -> Result<ApplicationListPage> {
    let (applications, continuation_token) = decode_list(message, 4, application)?;
    Ok(ApplicationListPage {
        applications,
        continuation_token,
    })
}

pub(crate) fn decode_services(message: &Message) -> Result<ServiceListPage> {
    let (services, continuation_token) = decode_list(message, 2, service)?;
    Ok(ServiceListPage {
        services,
        continuation_token,
    })
}

fn health(raw: i64) -> Result<HealthState> {
    match HealthState::from_raw(raw) {
        HealthState::Unrecognized(_) => Err(Error::Protocol("unsupported list health state")),
        state => Ok(state),
    }
}

fn name(decoder: &mut Decoder<'_>) -> Result<String> {
    decoder.typed_object(|decoder, tag| {
        if !tag.is_empty()
            || decoder.i64()? != 1
            || decoder.string()? != "fabric"
            || !decoder.string()?.is_empty()
            || decoder.i64()? != 0
            || !decoder.string()?.is_empty()
            || decoder.i32()? != -1
        {
            return Err(Error::Protocol("unsupported native naming URI profile"));
        }
        let path = decoder.string()?;
        if !decoder.string()?.is_empty() || !decoder.string()?.is_empty() {
            return Err(Error::Protocol("naming URI has query or fragment"));
        }
        let count = decoder.u32()? as usize;
        if count > MAX_COLLECTION_COUNT {
            return Err(Error::Protocol("naming URI segment count exceeds bound"));
        }
        let value = format!("fabric:{path}");
        crate::protocol::validate_name(&value)
            .map_err(|_| Error::Protocol("unsupported native naming URI value"))?;
        let mut segments = path[1..].split('/');
        for _ in 0..count {
            if segments.next() != Some(decoder.string()?.as_str()) {
                return Err(Error::Protocol("naming URI segments disagree with path"));
            }
        }
        if segments.next().is_some() {
            return Err(Error::Protocol(
                "naming URI segment count disagrees with path",
            ));
        }
        Ok(value)
    })
}

fn parameters(decoder: &mut Decoder<'_>) -> Result<BTreeMap<String, String>> {
    #[derive(Deserialize)]
    struct Pair(String, String);
    let pairs = decoder.object_array(from_fields::<Pair>)?;
    let mut values = BTreeMap::new();
    for Pair(key, value) in pairs {
        if values.insert(key, value).is_some() {
            return Err(Error::Protocol("duplicate application parameter"));
        }
    }
    Ok(values)
}

fn application(decoder: &mut Decoder<'_>) -> Result<Application> {
    #[derive(Deserialize)]
    struct Fields {
        type_name: String,
        type_version: String,
        status: i64,
        health: i64,
    }
    let name = name(decoder)?;
    let fields: Fields = from_fields(decoder)?;
    let status = match fields.status {
        0 => ApplicationStatus::Invalid,
        1 => ApplicationStatus::Ready,
        2 => ApplicationStatus::Upgrading,
        3 => ApplicationStatus::Creating,
        4 => ApplicationStatus::Deleting,
        5 => ApplicationStatus::Failed,
        _ => return Err(Error::Protocol("unsupported application status")),
    };
    let health_state = health(fields.health)?;
    let parameters = parameters(decoder)?;
    if !decoder.is_scope_end()? && !decoder.string()?.is_empty() {
        return Err(Error::Protocol(
            "nonempty deprecated application upgrade version",
        ));
    }
    if !decoder.is_scope_end()? && !self::parameters(decoder)?.is_empty() {
        return Err(Error::Protocol(
            "nonempty deprecated application upgrade parameters",
        ));
    }
    let definition_kind = if decoder.is_scope_end()? {
        ApplicationDefinitionKind::Invalid
    } else {
        match decoder.i64()? {
            0 => ApplicationDefinitionKind::ServiceFabricApplicationDescription,
            1 => ApplicationDefinitionKind::Compose,
            2 => ApplicationDefinitionKind::MeshApplicationDescription,
            65535 => ApplicationDefinitionKind::Invalid,
            _ => return Err(Error::Protocol("unsupported application definition kind")),
        }
    };
    Ok(Application {
        name,
        type_name: fields.type_name,
        type_version: fields.type_version,
        status,
        health_state,
        parameters,
        definition_kind,
    })
}

fn service(decoder: &mut Decoder<'_>) -> Result<Service> {
    #[derive(Deserialize)]
    struct Fields {
        type_name: String,
        manifest_version: String,
        persisted: bool,
        health: i64,
    }
    let raw_kind = decoder.i64()?;
    let name = name(decoder)?;
    let fields: Fields = from_fields(decoder)?;
    let kind = match (raw_kind, fields.persisted) {
        (1, false) => ServiceKind::Stateless,
        (2, has_persisted_state) => ServiceKind::Stateful {
            has_persisted_state,
        },
        _ => return Err(Error::Protocol("unsupported service kind/persistence")),
    };
    let health_state = health(fields.health)?;
    let status = if decoder.is_scope_end()? {
        ServiceStatus::Unknown
    } else {
        match decoder.i64()? {
            0 => ServiceStatus::Unknown,
            1 => ServiceStatus::Active,
            2 => ServiceStatus::Upgrading,
            3 => ServiceStatus::Deleting,
            4 => ServiceStatus::Creating,
            5 => ServiceStatus::Failed,
            _ => return Err(Error::Protocol("unsupported service status")),
        }
    };
    let is_service_group = if decoder.is_scope_end()? {
        false
    } else {
        decoder.boolean()?
    };
    Ok(Service {
        name,
        type_name: fields.type_name,
        manifest_version: fields.manifest_version,
        kind,
        health_state,
        status,
        is_service_group,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mssf_transport::serialization::Encoder;

    fn application_base(e: &mut Encoder) -> Result<()> {
        uri(e)?;
        e.string("Type")?;
        e.string("1")?;
        e.i64(1);
        e.i64(1);
        empty_map(e)
    }

    fn service_base(e: &mut Encoder) -> Result<()> {
        e.i64(1);
        uri(e)?;
        e.string("Type")?;
        e.string("1")?;
        e.boolean(false);
        e.i64(1);
        Ok(())
    }

    fn uri(e: &mut Encoder) -> Result<()> {
        e.object(|e| {
            e.i64(1);
            e.string("fabric")?;
            e.string("")?;
            e.i64(0);
            e.string("")?;
            e.i32(-1);
            e.string("/App")?;
            e.string("")?;
            e.string("")?;
            e.u32(1);
            e.string("App")
        })
    }

    fn empty_map(e: &mut Encoder) -> Result<()> {
        e.object_array::<()>(&[], |_, _| Ok(()))
    }

    #[test]
    fn optional_suffix_wrong_metadata_is_not_absence_at_valid_record_extent() {
        for position in 0..3 {
            let mut e = Encoder::new();
            e.object(|e| {
                application_base(e)?;
                if position >= 1 {
                    e.string("")?;
                }
                if position >= 2 {
                    empty_map(e)?;
                }
                // Wrong metadata at upgrade version/map/definition respectively.
                e.boolean(false);
                Ok(())
            })
            .unwrap();
            let bytes = e.finish();
            assert!(matches!(
                Decoder::new(&bytes).object(application),
                Err(Error::Protocol(_))
            ));
        }
        for position in 0..2 {
            let mut e = Encoder::new();
            e.object(|e| {
                service_base(e)?;
                if position == 1 {
                    e.i64(1);
                }
                e.string("wrong status/group metadata")
            })
            .unwrap();
            let bytes = e.finish();
            assert!(matches!(
                Decoder::new(&bytes).object(service),
                Err(Error::Protocol(_))
            ));
        }
    }

    #[test]
    fn nonempty_deprecated_upgrade_parameter_map_is_explicitly_unsupported() {
        let mut e = Encoder::new();
        e.object(|e| {
            application_base(e)?;
            e.string("")?;
            e.object_array(&[()], |e, _| {
                e.string("upgrade")?;
                e.string("value")
            })
        })
        .unwrap();
        assert!(matches!(
            Decoder::new(&e.finish()).object(application),
            Err(Error::Protocol(
                "nonempty deprecated application upgrade parameters"
            ))
        ));
    }

    #[test]
    fn truncated_optional_string_with_valid_record_extent_is_not_an_absent_suffix() {
        let mut e = Encoder::new();
        e.object(|e| {
            application_base(e)?;
            e.string("tail")
        })
        .unwrap();
        let mut bytes = e.finish();
        // Keep the record header and all enclosing extents valid. Only the
        // trailing string's advertised unit count exceeds its remaining payload.
        let count = bytes.len() - 11;
        assert_eq!(bytes[count - 1], 0x8d);
        bytes[count] = 20;
        assert!(matches!(
            Decoder::new(&bytes).object(application),
            Err(Error::Protocol(_))
        ));
    }
}
