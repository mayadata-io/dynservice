use crate::{
    ServiceConfig, ServiceDescriptor, ServiceError, ServiceRegistry, ServiceRegistryOptions,
};
use futures::Future;
use std::pin::Pin;

pub struct Builder {
    config: ServiceConfig,
    entrypoint: Option<Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>>,
    initializer: Option<Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>>,
}

impl Builder {
    pub fn new(config: ServiceConfig) -> Self {
        Self {
            config,
            entrypoint: None,
            initializer: None,
        }
    }

    pub fn with_entrypoint(
        mut self,
        entrypoint: impl Future<Output = Result<(), String>> + Send + 'static,
    ) -> Self {
        self.entrypoint = Some(Box::pin(entrypoint));
        self
    }

    pub fn with_initializer(
        mut self,
        initializer: impl Future<Output = Result<(), String>> + Send + 'static,
    ) -> Self {
        self.initializer = Some(Box::pin(initializer));
        self
    }

    pub fn build(self) -> DynamicService {
        let entrypoint = self
            .entrypoint
            .expect("Service entrypoint must be provided");

        DynamicService {
            config: self.config,
            entrypoint,
            initializer: self.initializer,
        }
    }
}

pub struct DynamicService {
    entrypoint: Pin<Box<dyn Future<Output = Result<(), String>> + 'static>>,
    initializer: Option<Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>>,
    config: ServiceConfig,
}

impl DynamicService {
    pub async fn start(self) -> Result<(), ServiceError> {
        // Initialize service registry.
        let options = ServiceRegistryOptions::builder()
            .with_endpoints(["192.168.178.13:32379"])
            .build();

        println!("{:?}", options);
        let mut registry = ServiceRegistry::new(options).await?;

        // Invoke service-specific initializer, if specified.
        if let Some(initializer) = self.initializer {
            println!("--> Calling service initializer.");
            initializer.await.unwrap();
            println!(" <- Service initializer successfully completed");
        }

        // Register the service.
        let service_descriptor = registry.register_service(self.config).await?;

        // Start the heartbeat loop.
        let mut controller = ServiceController::new(service_descriptor);

        // Wait till heartbeat loop ends or service exits.
        tokio::select! {
            _ = controller.start_heartbeat() => {
                println!("@@ HB lost");
            },
            _ = self.entrypoint => {
                println!("@@ Service complete");
                controller.stop_heartbeat().await;
            }
        }

        // Unregister the service.
        controller.unregister().await;
        Ok(())
    }
}

struct ServiceController {
    descriptor: ServiceDescriptor,
}

impl ServiceController {
    fn new(descriptor: ServiceDescriptor) -> Self {
        Self { descriptor }
    }

    async fn start_heartbeat(&mut self) {
        // TODO: Start heartbeat loop.
        println!("* Starting heartbeat loop ...");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        println!("* Heartbeat done !");
    }

    async fn stop_heartbeat(&mut self) {
        // TODO: Stop heartbeat loop.
    }

    async fn unregister(self) {
        self.descriptor.unregister();
    }
}
