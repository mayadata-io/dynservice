use crate::common::{ServiceKeyPathBuilder, ServiceRegister};
use crate::{
    store::KeyValueStore, Service, ServiceConfig, ServiceDescriptor, ServiceError, ServiceWatcher,
};
use snafu::ResultExt;

pub struct ServiceRegistry {
    pub(crate) kv_store: KeyValueStore,
}

#[derive(Debug)]
pub struct ServiceRegistryOptions {
    endpoints: Vec<String>,
}

#[derive(Default, Debug)]
pub struct ServiceRegistryOptionsBuilder {
    endpoints: Option<Vec<String>>,
}

impl ServiceRegistryOptionsBuilder {
    pub fn build(mut self) -> ServiceRegistryOptions {
        let endpoints = self
            .endpoints
            .take()
            .expect("ETCD endpoints must be configured");

        ServiceRegistryOptions { endpoints }
    }

    pub fn with_endpoints<E: AsRef<str>, S: AsRef<[E]>>(mut self, endpoints: S) -> Self {
        let eps: Vec<String> = endpoints
            .as_ref()
            .iter()
            .map(|e| e.as_ref().to_string())
            .collect();
        self.endpoints = Some(eps);
        self
    }
}

impl ServiceRegistryOptions {
    pub fn builder() -> ServiceRegistryOptionsBuilder {
        ServiceRegistryOptionsBuilder::default()
    }
}

/// Main Service Registry abstraction. Represents a set of services identified by
/// the (name, instance) tuple.
/// Allows registering services, searching for services and watching for added/removed
/// services.
impl ServiceRegistry {
    /// Get service registry instance.
    pub async fn new(options: ServiceRegistryOptions) -> Result<Self, ServiceError> {
        let kv_store = KeyValueStore::new(&options.endpoints).await.unwrap();

        Ok(Self { kv_store })
    }

    pub async fn register_service(
        &mut self,
        options: ServiceConfig,
    ) -> Result<ServiceDescriptor, ServiceError> {
        // Create the lease with heartbeat TTL.
        let lease = self
            .kv_store
            .grant_lease(options.heartbeat_interval())
            .await
            .context(ServiceRegister {
                service: options.name().to_string(),
                instance: options.instance_id().to_string(),
            })?;

        // Populate service instance in object store.
        let service_key = ServiceKeyPathBuilder::new(options.name(), options.instance_id())
            .build()
            .unwrap();

        let service = Service::new(options);
        self.kv_store
            .put(service_key, &service, &lease)
            .await
            .unwrap();

        Ok(ServiceDescriptor::new(&service, lease))
    }

    pub async fn watch(&mut self) -> Result<ServiceWatcher, ServiceError> {
        // Watch all services.
        let key = ServiceKeyPathBuilder::default().build().unwrap();
        let watcher = self.kv_store.get_watcher(key, true).await.unwrap();
        Ok(ServiceWatcher::new(watcher))
    }
}
