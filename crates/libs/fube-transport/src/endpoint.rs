//! Reusable native connection configuration, without live session state.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::time::Instant;

use crate::{
    connection::deadline_after,
    native_channel::{NativeEnvelope, SessionEnvelope},
    native_demux::ChannelComposition,
    Channel, ChannelLimits, Error, Result,
};

/// Plain-TCP dial settings and immutable protocol authorization.
///
/// Construction, setters and cloning do no I/O. Each [`Self::connect`] creates a
/// fresh owner, target, ID history and driver; cloning a [`Channel`] instead
/// shares an existing connection. Policies are bounded validated data and
/// stateless function pointers, not live protocol sessions.
#[derive(Clone)]
pub struct Endpoint {
    address: SocketAddr,
    timeout: Duration,
    limits: ChannelLimits,
    composition: ChannelComposition,
}

pub(crate) fn validate_tcp_endpoint(address: SocketAddr) -> Result<()> {
    if address.port() == 0 || address.ip().is_unspecified() || address.ip().is_multicast() {
        return Err(Error::InvalidInput(
            "TCP endpoint requires a unicast address and nonzero port",
        ));
    }
    Ok(())
}

impl Endpoint {
    /// Configure an explicit numeric unicast endpoint with a nonzero port.
    ///
    /// No permissive policy is supplied: adapters choose their composition.
    /// The default connection budget is 30 seconds, independent of the
    /// started-frame watchdog and any typed client's operation defaults.
    pub fn unsecured_tcp(address: SocketAddr, composition: ChannelComposition) -> Result<Self> {
        validate_tcp_endpoint(address)?;
        Ok(Self {
            address,
            timeout: Duration::from_secs(30),
            limits: ChannelLimits::default(),
            composition,
        })
    }

    /// Set the whole-establishment budget; validated before dialing.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set finite transport reservations; validated before dialing.
    pub fn channel_limits(mut self, limits: ChannelLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Eagerly establish one connection with one absolute deadline.
    pub async fn connect(&self) -> Result<Channel> {
        let deadline = deadline_after(self.timeout)?;
        self.connect_until(deadline).await
    }

    /// Establish using a caller-owned absolute deadline instead of the configured
    /// connection timeout. This lets connection setup share an enclosing budget
    /// with subsequent protocol startup without renewing it.
    pub async fn connect_until(&self, deadline: Instant) -> Result<Channel> {
        self.connect_with_envelope_until(deadline, Arc::new(NativeEnvelope))
            .await
    }

    pub(crate) async fn connect_with_envelope_until(
        &self,
        deadline: Instant,
        session: Arc<dyn SessionEnvelope>,
    ) -> Result<Channel> {
        let channel = Channel::connect_with_envelope(
            self.address,
            self.composition.clone(),
            self.limits,
            deadline,
            session,
        )
        .await?;
        Channel::publish_before_deadline(channel, deadline)
    }
}
