use crate::store::StoreError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum ServiceError {
    /// Service does not exist.
    #[snafu(display("Service {} not found", service))]
    NotFound { service: String },

    /// Can't open a connection with the underlying key-value store.
    #[snafu(display("Can't connect to key-value store: {}", source))]
    StoreInitialize {
        source: etcd_client::Error,
        e: String,
    },

    /// Can't register new service
    #[snafu(display(
        "Failed to register service {} with instance {}. Error='{}'",
        service,
        instance,
        source
    ))]
    ServiceRegister {
        service: String,
        instance: String,
        source: StoreError,
    },

    /// Can't unregister existing service
    #[snafu(display(
        "Failed to unregister service {} with instance {}. Error='{}'",
        service,
        instance,
        source
    ))]
    ServiceUnregister {
        service: String,
        instance: String,
        source: StoreError,
    },

    /// Heartbeat lost.
    #[snafu(display("Service {}:{} lost heartbeat", service, instance,))]
    HeartbeatLost { service: String, instance: String },

    /// Can't send heartbeat.
    #[snafu(display(
        "Service {}:{} failed to send heartbeat, error='{}'",
        service,
        instance,
        source
    ))]
    HeartbeatError {
        service: String,
        instance: String,
        source: StoreError,
    },

    /// Invalid argument passed.
    #[snafu(display("Invalid argument: {}", error))]
    InvalidArgument { error: String },

    /// Failed to read entried from the registry.
    #[snafu(display("Failed to read from registry: {}", error))]
    RegistryRead { error: String },

    /// Failed to instantinate service from JSON object.
    #[snafu(display("Failed to deseriazlize service from JSON. Error={}", source))]
    ServiceDeserialize { source: serde_json::Error },

    /// Error while watching the services.
    #[snafu(display("Failed to watch for service events on key {}. Error={}", key, source))]
    WatchService { key: String, source: StoreError },

    /// Watch stream closed, no more events can be received.
    #[snafu(display("Watch stream for key '{}' is closed", key))]
    WatchStreamClosed { key: String },

    /// Failed to shutdown the service
    #[snafu(display("Failed to shutdown service {}:{}. Error={}", service, instance, error))]
    ServiceShutdown {
        service: String,
        instance: String,
        error: String,
    },
}

pub const DEFAULT_SERVICE_TIMEOUT: i64 = 5;
/// Top-level ETCD key prefix for services.
const SERVICES_NAME_DOMAIN: &str = "/mayastor.io/services";

pub const MAX_MISSED_HEARTBEATS: i64 = 3;

#[derive(PartialEq, Debug)]
pub enum HeartbeatFailurePolicy {
    Panic,
    Stop,
    Restart,
}

#[derive(Debug)]
pub enum HeartbeatStatus {
    Lost,
    Restored,
}

#[derive(Debug, Default)]
pub struct ServiceKeyPathBuilder {
    service: Option<String>,
    instance_id: Option<String>,
}

impl ServiceKeyPathBuilder {
    pub fn new(service: impl Into<String>, instance_id: impl Into<String>) -> Self {
        Self {
            service: Some(service.into()),
            instance_id: Some(instance_id.into()),
        }
    }

    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.service = Some(service.into());
        self
    }

    pub fn with_service_instance(mut self, instance_id: impl Into<String>) -> Self {
        self.instance_id = Some(instance_id.into());
        self
    }

    pub fn build(self) -> Result<String, ServiceError> {
        match (self.service, self.instance_id) {
            (Some(s), Some(i)) => Ok(format!("{}/{}/{}", SERVICES_NAME_DOMAIN, s, i)),
            (Some(s), None) => Ok(format!("{}/{}", SERVICES_NAME_DOMAIN, s)),
            (None, Some(_)) => Err(ServiceError::InvalidArgument {
                error: String::from("Service name is mandatory for service instance"),
            }),
            _ => Ok(String::from(SERVICES_NAME_DOMAIN)),
        }
    }
}
