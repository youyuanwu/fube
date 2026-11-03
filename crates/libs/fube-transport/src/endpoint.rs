//! Reusable native connection configuration, without live session state.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::time::Instant;

use crate::{
    connection::deadline_after,
    native_channel::{NativeEnvelope, SessionEnvelope},
    native_demux::ChannelComposition,
    Channel, ChannelLimits, Error, Result,
};

/// Explicitly trusted clear-loopback dial settings and immutable authorization.
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

impl Endpoint {
    /// Configure an explicit numeric nonzero-port loopback endpoint.
    ///
    /// No permissive policy is supplied: adapters choose their composition.
    /// The default connection budget is 30 seconds, independent of the
    /// started-frame watchdog and any typed client's operation defaults.
    pub fn unsecured_loopback(
        address: SocketAddr,
        composition: ChannelComposition,
    ) -> Result<Self> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(Error::InvalidInput(
                "unsecured gateway requires explicit loopback endpoint",
            ));
        }
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

    pub(crate) async fn connect_until(&self, deadline: Instant) -> Result<Channel> {
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
