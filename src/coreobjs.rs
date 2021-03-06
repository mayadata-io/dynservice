//use etcdclient::EtcdLease;
use crate::{
    common::{ServiceUnregister, DEFAULT_SERVICE_TIMEOUT},
    store::{StoreError, TimedLease},
    ServiceError,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Service {
    name: String,
    instance_id: String,
    endpoints: Vec<String>,
}

/// Active service instance registered in the Service Registry.
impl Service {
    pub(crate) fn new(options: ServiceConfig) -> Self {
        Self {
            name: options.name,
            instance_id: options.instance_id,
            endpoints: options.endpoints,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn endpoints(&self) -> &Vec<String> {
        &self.endpoints
    }
}

#[derive(Default, Debug)]
pub struct ServiceConfigBuilder {
    name: Option<String>,
    instance_id: Option<String>,
    heartbeat_interval: Option<i64>,
    endpoints: Option<Vec<String>>,
}

/// Builder to construct service config.
impl ServiceConfigBuilder {
    pub fn build(self) -> ServiceConfig {
        let name = self.name.expect("Service name is mandatory");

        let instance_id = self.instance_id.expect("Service instance ID is mandatory");

        let heartbeat_interval = self.heartbeat_interval.unwrap_or(DEFAULT_SERVICE_TIMEOUT);

        let endpoints = self.endpoints.expect("Service endpoints are mandatory");

        ServiceConfig {
            name,
            instance_id,
            heartbeat_interval,
            endpoints,
        }
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_instance_id(mut self, instance_id: impl Into<String>) -> Self {
        self.instance_id = Some(instance_id.into());
        self
    }

    // Set heartbeat update interval for service.
    pub fn with_heartbeat_interval(mut self, interval: i64) -> Self {
        assert!(
            interval > 0,
            "Heartbeat interval must be non-negative: {}",
            interval
        );

        // TODO: Check against the lowest suitable heartbeat interval.
        self.heartbeat_interval = Some(interval);
        self
    }

    pub fn with_endpoints<S: AsRef<str>, E: AsRef<[S]>>(mut self, endpoints: E) -> Self {
        let eps: Vec<String> = endpoints
            .as_ref()
            .iter()
            .map(|e| e.as_ref().to_string())
            .collect();

        assert!(!eps.is_empty(), "Service must have at least one endpoint");
        self.endpoints = Some(eps);
        self
    }
}

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    name: String,
    instance_id: String,
    heartbeat_interval: i64,
    endpoints: Vec<String>,
}

/// Config for a service to be registered within the Service Registry.
impl ServiceConfig {
    pub fn builder() -> ServiceConfigBuilder {
        ServiceConfigBuilder::default()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn heartbeat_interval(&self) -> i64 {
        self.heartbeat_interval
    }

    pub fn endpoints(&self) -> Vec<String> {
        self.endpoints.clone()
    }
}

#[derive(Debug)]
pub struct ServiceDescriptor {
    service: String,
    instance: String,
    lease: TimedLease,
}

impl ServiceDescriptor {
    pub(crate) fn new(service: &Service, lease: TimedLease) -> Self {
        Self {
            service: service.name().to_string(),
            instance: service.instance_id().to_string(),
            lease,
        }
    }

    pub async fn send_heartbeat(&mut self) -> Result<(), ServiceError> {
        // Differentiate between heartbeat lost and heartbeat update errors
        // to simplify
        match self.lease.keep_alive().await {
            Err(StoreError::LeaseLost { .. }) => Err(ServiceError::HeartbeatLost {
                service: self.service.to_string(),
                instance: self.instance.to_string(),
            }),
            Err(e) => Err(ServiceError::HeartbeatError {
                service: self.service.to_string(),
                instance: self.instance.to_string(),
                source: e,
            }),
            _ => Ok(()),
        }
    }

    pub async fn unregister(mut self) -> Result<(), ServiceError> {
        self.lease.revoke().await.context(ServiceUnregister {
            service: self.service.to_string(),
            instance: self.instance.to_string(),
        })?;
        Ok(())
    }

    pub fn heartbeat_interval(&self) -> i64 {
        self.lease.ttl()
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }
}
