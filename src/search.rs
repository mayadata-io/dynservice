use crate::{common::ServiceKeyPathBuilder, Service, ServiceError, ServiceRegistry};

#[derive(Default, Debug)]
pub struct ServiceRegistrySearchRequest {
    service: Option<String>,
    instance_id: Option<String>,
}

impl ServiceRegistrySearchRequest {
    pub fn with_service(mut self, name: impl Into<String>) -> Self {
        self.service = Some(name.into());
        self
    }

    pub fn with_instance_id(mut self, instance_id: impl Into<String>) -> Self {
        self.instance_id = Some(instance_id.into());
        self
    }
}

impl ServiceRegistry {
    pub async fn search(
        &mut self,
        req: ServiceRegistrySearchRequest,
    ) -> Result<Vec<Service>, ServiceError> {
        let mut builder = ServiceKeyPathBuilder::default();

        if let Some(service) = req.service {
            builder = builder.with_service(service);
        }

        if let Some(instance_id) = req.instance_id {
            builder = builder.with_service_instance(instance_id);
        }

        let path = builder.build()?;
        let res =
            self.kv_store
                .get_prefix(path)
                .await
                .map_err(|_e| ServiceError::RegistryRead {
                    error: String::from("Failed to search registry"),
                })?;

        let mut services: Vec<Service> = Vec::new();
        for s in res {
            match serde_json::from_value::<Service>(s) {
                Ok(service) => {
                    services.push(service);
                }
                Err(e) => {
                    log::warn!("Failed to deserialize service: {}", e);
                }
            }
        }

        Ok(services)
    }
}
