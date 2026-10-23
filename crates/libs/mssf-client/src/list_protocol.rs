//! Source-derived list aggregate envelopes and positional record layouts.

use mssf_transport::message::Message;
use serde::Serialize;

use crate::{
    applications::*, query_arguments::QueryArguments, query_protocol::Query, services::*, Error,
    HealthState, Paging, Result,
};

fn type_selector(value: &str) -> Result<&str> {
    if value.is_empty() {
        return Err(Error::InvalidInput("selected type name must not be empty"));
    }
    Ok(value)
}

impl Query for ApplicationQueryDescription {
    type Output = ApplicationListPage;
    const NAME: &'static str = "GetApplicationList";

    fn encode_parameters(&self, arguments: &mut QueryArguments) -> Result<()> {
        #[derive(Default, Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct Fields<'a> {
            application_name: Option<&'a str>,
            application_type_name: Option<&'a str>,
            application_definition_kind_filter: Option<ApplicationDefinitionKindFilter>,
            exclude_application_parameters: Option<bool>,
        }
        let mut fields = Fields {
            exclude_application_parameters: self
                .parameters
                .exclude_application_parameters
                .then_some(true),
            ..Default::default()
        };
        match &self.parameters.filter {
            ApplicationListFilter::All => {}
            ApplicationListFilter::Name(name) => {
                fields.application_name = Some(name.as_str());
            }
            ApplicationListFilter::TypeName(name) => {
                fields.application_type_name = Some(type_selector(name)?);
            }
            ApplicationListFilter::DefinitionKind(filter) => {
                let filter = ApplicationDefinitionKindFilter::try_from_bits(filter.bits())?;
                fields.application_definition_kind_filter = (filter.bits() != 0).then_some(filter);
            }
        }
        arguments.append(&fields)
    }

    fn paging(&self) -> Option<&Paging> {
        Some(&self.paging)
    }

    fn decode_reply(message: &Message) -> Result<ApplicationListPage> {
        decode_applications(message)
    }
}

impl Query for ServiceQueryDescription {
    type Output = ServiceListPage;
    const NAME: &'static str = "GetApplicationServiceList";

    fn encode_parameters(&self, arguments: &mut QueryArguments) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct Fields<'a> {
            application_name: &'a str,
            service_name: Option<&'a str>,
            service_type_name: Option<&'a str>,
        }
        let parameters = &self.parameters;
        if parameters.application_name.as_str() == "fabric:/System" {
            return Err(Error::InvalidInput(
                "System service-list profile is unsupported",
            ));
        }
        let mut fields = Fields {
            application_name: parameters.application_name.as_str(),
            service_name: None,
            service_type_name: None,
        };
        match &parameters.filter {
            ServiceListFilter::All => {}
            ServiceListFilter::Name(name) => {
                fields.service_name = Some(name.as_str());
            }
            ServiceListFilter::TypeName(name) => {
                fields.service_type_name = Some(type_selector(name)?);
            }
        }
        arguments.append(&fields)
    }

    fn paging(&self) -> Option<&Paging> {
        Some(&self.paging)
    }

    fn decode_reply(message: &Message) -> Result<ServiceListPage> {
        decode_services(message)
    }
}

pub(crate) fn decode_applications(message: &Message) -> Result<ApplicationListPage> {
    crate::query_protocol::decode_list::<Application, _, 4>(message, true, validate_application)
}

pub(crate) fn decode_services(message: &Message) -> Result<ServiceListPage> {
    crate::query_protocol::decode_list::<Service, _, 2>(message, true, validate_service)
}

fn validate_list_health(value: HealthState) -> Result<HealthState> {
    if matches!(value, HealthState::Unrecognized(_)) {
        return Err(Error::Protocol("unsupported list health state"));
    }
    Ok(value)
}

fn validate_application(application: Application) -> Result<Application> {
    validate_list_health(application.health_state)?;
    if !application.upgrade_type_version.is_empty() {
        return Err(Error::Protocol(
            "nonempty deprecated application upgrade version",
        ));
    }
    if !application.upgrade_parameters.is_empty() {
        return Err(Error::Protocol(
            "nonempty deprecated application upgrade parameters",
        ));
    }
    Ok(application)
}

fn validate_service(service: Service) -> Result<Service> {
    if service.kind == ServiceKind::Invalid
        || (service.kind == ServiceKind::Stateless && service.has_persisted_state)
    {
        return Err(Error::Protocol("unsupported service kind/persistence"));
    }
    validate_list_health(service.health_state)?;
    Ok(service)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mssf_transport::serialization::{serde::from_fields, Decoder, Encoder};

    fn application(decoder: &mut Decoder<'_>) -> Result<Application> {
        validate_application(from_fields::<Application>(decoder)?)
    }
    fn service(decoder: &mut Decoder<'_>) -> Result<Service> {
        validate_service(from_fields::<Service>(decoder)?)
    }

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
