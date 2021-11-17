use crate::{
    ServiceConfig, ServiceDescriptor, ServiceError, ServiceRegistry, ServiceRegistryOptions,
};
use futures::Future;
use std::pin::Pin;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::Mutex;
use std::sync::Arc;
use crate::common::HeartbeatFailurePolicy;

pub struct Builder {
    config: ServiceConfig,
    entrypoint: Option<Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>>,
    initializer: Option<Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>>,
    hb_failure_policy: HeartbeatFailurePolicy,
}

impl Builder {
    pub fn new(config: ServiceConfig) -> Self {
        Self {
            config,
            entrypoint: None,
            initializer: None,
            hb_failure_policy: HeartbeatFailurePolicy::Reconnect,
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

    pub fn with_heartbeat_failure_policy(mut self, policy: HeartbeatFailurePolicy) -> Self {
        self.hb_failure_policy = policy;
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
            initializer.await.unwrap();
        }

        // Register the service.
        let service_descriptor = registry.register_service(self.config).await?;

        // Start the heartbeat loop.
        let mut controller = ServiceController::new(service_descriptor);

        // Wait till heartbeat loop ends or service exits.
        tokio::select! {
            _ = controller.start_heartbeat() => {
                println!("** ZZZ !");
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

#[derive(Debug)]
enum HeartbeatLoopCommand {
    Ok,
    StopHeartbeat(tokio::sync::oneshot::Sender<HeartbeatLoopCommand>),
}

struct ServiceController {
    descriptor: Arc<Mutex<Option<ServiceDescriptor>>>,
    hb_channel: Option<Sender<HeartbeatLoopCommand>>,
    ttl: i64,
    service: String,
    instance: String,
}

impl ServiceController {
    fn new(descriptor: ServiceDescriptor) -> Self {
        let ttl = descriptor.heartbeat_interval();
        let service = descriptor.service().to_string();
        let instance = descriptor.instance().to_string();

        Self {
            ttl,
            descriptor: Arc::new(Mutex::new(Some(descriptor))),
            hb_channel: None,
            instance,
            service,
        }
    }

    /// Start heartbeat loop for the service based on TTL of the service.
    async fn start_heartbeat(&mut self) -> Result<(), ServiceError> {
        assert!(self.hb_channel.is_none(), "Heartbeat loop already started");

        let (sender, mut receiver) = channel::<HeartbeatLoopCommand>(1);

        self.hb_channel = Some(sender);

        let ttl = self.ttl;
        let service = self.service.clone();
        let instance = self.instance.clone();
        let descr = Arc::clone(&self.descriptor);

        tracing::info!(
            "Service {}:{} starting heartbeat loop with interval {} secs.",
            self.service, self.instance, ttl
        );

        let h = tokio::spawn(async move {
            loop {
                {
                    match descr.lock().await.as_mut() {
                        Some(d) => {
                            println!("->");
                            if let Err(e) = d.send_heartbeat().await {
                                tracing::error!(
                                    "Service {}:{} failed to send heartbeat, error={}",
                                    service, instance, e,
                                );
                                return Err(ServiceError::InvalidArgument {
                                    error: "".to_string()
                                });
                            }
                        },
                        // Service is being unregistered, break the heartbeat loop.
                        None => {
                            tracing::info!(
                                "Service {}:{} is being unregistering, stopping heartbeat loop",
                                service, instance,
                            );
                            receiver.close();
                            break;
                        }
                    }
                }

                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(ttl as u64 / 2)) => {
                        println!(">");
                    },
                    cmd = receiver.recv() => {
                        match cmd.unwrap() {
                            HeartbeatLoopCommand::StopHeartbeat(tx) => {
                                tracing::info!(
                                    "Service {}:{} stopped heartbeat loop",
                                    service, instance,
                                );
                                tx.send(HeartbeatLoopCommand::Ok).expect("Command sender disappeared");
                                break;
                            },
                            c => panic!("Unexpected command for heartbeat loop control: {:?}", c),
                        }
                    }
                }
            }
            Ok(())
        });

        // Wait till heartbeat loop is explicitly stopped or error occurs during heartbeat update.
        h.await.unwrap()
    }

    /// Stop the heartbeat loop. This operation is idempotent, so it's possible to stop
    /// the heartbeat loop which is already stopped.
    async fn stop_heartbeat(&mut self) {
        match self.hb_channel.take() {
            Some(channel) => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                // Take into account simultaneous heartbeat loop closure, which might happen either
                // in response to explicit service unregistration request or implicutly upon
                // heartbeat delivery failures.
                if channel.send(HeartbeatLoopCommand::StopHeartbeat(tx)).await.is_ok() {
                    // Channel might have been closed straight after we have sent the message,
                    // so be careful.
                    if rx.await.is_err() {
                        tracing::warn!("Heartbeat loop already stopped");
                    }
                }
            },
            None => {},
        }
    }

    /// Unregister the service and free the service descriptor.
    async fn unregister(self) {
        self.descriptor.lock().await.take();
    }
}
