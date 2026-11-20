//! Source-derived list aggregate envelopes and positional record layouts.

use crate::{
    applications::*,
    query_protocol::{result_tags, ListReply, Query},
    services::*,
    HealthState,
};

fn validate_type_selector(value: &str) -> fubst::Result<()> {
    if value.is_empty() {
        return Err(fubst::Error::InvalidInput(
            "selected type name must not be empty",
        ));
    }
    Ok(())
}

impl Query for ApplicationQueryParameters {
    type Output = ApplicationListPage;
    type Reply = ListReply<Application, { result_tags::APPLICATION }>;
    const NAME: &'static str = "GetApplicationList";

    fn validate_parameters(&self) -> fubst::Result<()> {
        match &self.filter {
            ApplicationListFilter::TypeName(name) => {
                validate_type_selector(name)?;
            }
            ApplicationListFilter::DefinitionKind(filter) => {
                filter.validate()?;
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_record(application: Application) -> std::result::Result<Application, &'static str> {
        validate_list_health(application.health_state)?;
        if !application.upgrade_type_version.is_empty() {
            return Err("nonempty deprecated application upgrade version");
        }
        if !application.upgrade_parameters.is_empty() {
            return Err("nonempty deprecated application upgrade parameters");
        }
        Ok(application)
    }
}

impl Query for ServiceQueryParameters {
    type Output = ServiceListPage;
    type Reply = ListReply<Service, { result_tags::SERVICE }>;
    const NAME: &'static str = "GetApplicationServiceList";

    fn validate_parameters(&self) -> fubst::Result<()> {
        if self.application_name.as_str() == "fabric:/System" {
            return Err(fubst::Error::InvalidInput(
                "System service-list profile is unsupported",
            ));
        }
        if let ServiceListFilter::TypeName(name) = &self.filter {
            validate_type_selector(name)?;
        }
        Ok(())
    }

    fn validate_record(service: Service) -> std::result::Result<Service, &'static str> {
        if service.kind == ServiceKind::Invalid
            || (service.kind == ServiceKind::Stateless && service.has_persisted_state)
        {
            return Err("unsupported service kind/persistence");
        }
        validate_list_health(service.health_state)?;
        Ok(service)
    }
}

fn validate_list_health(value: HealthState) -> std::result::Result<HealthState, &'static str> {
    if matches!(value, HealthState::Unrecognized(_)) {
        return Err("unsupported list health state");
    }
    Ok(value)
}
