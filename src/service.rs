use crate::{
    common::HeartbeatFailurePolicy, HeartbeatStatus, ServiceConfig, ServiceDescriptor,
    ServiceError, ServiceRegistry, ServiceRegistryOptions,
};
use futures::Future;
use std::{pin::Pin, sync::Arc};
use tokio::sync::{
    mpsc::{channel, Sender},
    Mutex,
};

use tracing::instrument;

type AsyncHandler = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>;
type ShutdownHandler = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>;
type HeartbeatStatusHandler = Box<dyn FnMut(HeartbeatStatus) + Send + 'static>;

pub struct Builder {
    service_config: ServiceConfig,
    registry_options: ServiceRegistryOptions,
    entrypoint: Option<AsyncHandler>,
    initializer: Option<AsyncHandler>,
    shutdown_handler: Option<ShutdownHandler>,
    hb_status_handler: Option<HeartbeatStatusHandler>,
    hb_failure_policy: HeartbeatFailurePolicy,
}

enum ServiceState {
    New(NewState),
    Running(RunningState),
    Reinitializing(ReinitializingState),
    Done,
}

impl ServiceState {
    fn name(&self) -> &'static str {
        match self {
            Self::New(_) => "New",
            Self::Running(_) => "Running",
            Self::Reinitializing(_) => "Reinitializing",
            Self::Done => "Done",
        }
    }
}

struct NewState {}
struct RunningState {
    service_descriptor: ServiceDescriptor,
}
struct ReinitializingState {}

impl Builder {
    pub fn new(service_config: ServiceConfig, registry_options: ServiceRegistryOptions) -> Self {
        Self {
            service_config,
            registry_options,
            entrypoint: None,
            initializer: None,
            shutdown_handler: None,
            hb_status_handler: None,
            hb_failure_policy: HeartbeatFailurePolicy::Panic,
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

    pub fn with_shutdown_handler(
        mut self,
        shutdown_handler: impl Future<Output = Result<(), String>> + Send + 'static,
    ) -> Self {
        self.shutdown_handler = Some(Box::pin(shutdown_handler));
        self
    }

    pub fn with_heartbeat_status_handler(
        mut self,
        heartbeat_handler: impl FnMut(HeartbeatStatus) + Send + 'static,
    ) -> Self {
        self.hb_status_handler = Some(Box::new(heartbeat_handler));
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

        if self.hb_failure_policy == HeartbeatFailurePolicy::Stop {
            assert!(
                self.shutdown_handler.is_some(),
                "Shutdown handler is mandatory for 'Stop' heartbeat failure policy"
            );
        }

        DynamicService {
            service_config: self.service_config,
            registry_options: self.registry_options,
            entrypoint,
            initializer: self.initializer,
            hb_failure_policy: self.hb_failure_policy,
            shutdown_handler: self.shutdown_handler,
            hb_status_handler: self.hb_status_handler,
            current_state: Some(ServiceState::New(NewState {})),
            service_registrator: None,
        }
    }
}

pub struct DynamicService {
    entrypoint: AsyncHandler,
    initializer: Option<AsyncHandler>,
    shutdown_handler: Option<ShutdownHandler>,
    hb_status_handler: Option<HeartbeatStatusHandler>,
    service_config: ServiceConfig,
    registry_options: ServiceRegistryOptions,
    hb_failure_policy: HeartbeatFailurePolicy,
    current_state: Option<ServiceState>,
    service_registrator: Option<ServiceRegistrator>,
}

struct ServiceRegistrator {
    registry: ServiceRegistry,
    config: ServiceConfig,
}

impl ServiceRegistrator {
    fn new(registry: ServiceRegistry, config: ServiceConfig) -> Self {
        Self { registry, config }
    }

    async fn register_service(
        &mut self,
        register_on_faults: bool,
    ) -> Result<ServiceDescriptor, ServiceError> {
        loop {
            match self.registry.register_service(self.config.clone()).await {
                Ok(descriptor) => return Ok(descriptor),
                Err(e) => {
                    if !register_on_faults {
                        return Err(e);
                    } else {
                        // Repeat service registration.
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
trait ServiceStateStep<S> {
    async fn state_step(&mut self, state: S) -> Result<ServiceState, ServiceError>;
}

#[async_trait::async_trait]
impl ServiceStateStep<NewState> for DynamicService {
    async fn state_step(&mut self, _state: NewState) -> Result<ServiceState, ServiceError> {
        let registry = ServiceRegistry::new(self.registry_options.clone()).await?;
        let mut service_registrator =
            ServiceRegistrator::new(registry, self.service_config.clone());

        // Invoke service-specific initializer once before registering the service.
        if let Some(initializer) = self.initializer.take() {
            initializer.await.unwrap();
        }

        // Prepare service descriptor for the next step.
        let service_descriptor = service_registrator.register_service(false).await?;
        self.service_registrator = Some(service_registrator);

        Ok(ServiceState::Running(RunningState { service_descriptor }))
    }
}

#[async_trait::async_trait]
impl ServiceStateStep<RunningState> for DynamicService {
    async fn state_step(&mut self, state: RunningState) -> Result<ServiceState, ServiceError> {
        let mut controller = ServiceController::new(state.service_descriptor);

        tokio::select! {
            // Wait for heartbeat loos and apply recovery action.
            r = controller.start_heartbeat() => {
                // Service loop must complete only due to errors.
                r.expect_err("Abnormal heartbeat loop completion");

                self.notify_heartbeat_status(HeartbeatStatus::Lost);

                // Apply heartbeat loss policy.
                match self.hb_failure_policy {
                    HeartbeatFailurePolicy::Panic => panic!(
                        "Service {}:{} has lost heartbeat.",
                        self.service_config.name(),
                        self.service_config.instance_id()
                    ),
                    HeartbeatFailurePolicy::Stop => {
                        self.shutdown_service().await?;
                        Ok(ServiceState::Done)
                    },
                    HeartbeatFailurePolicy::Restart => Ok(
                        ServiceState::Reinitializing(ReinitializingState {})
                    ),
                }
            },
            // Wait for the completion of service's main loop.
            _ = &mut self.entrypoint => {
                controller.stop_heartbeat().await;
                controller.unregister().await;

                Ok(ServiceState::Done)
            }
        }
    }
}

#[async_trait::async_trait]
impl ServiceStateStep<ReinitializingState> for DynamicService {
    async fn state_step(
        &mut self,
        _state: ReinitializingState,
    ) -> Result<ServiceState, ServiceError> {
        // We have encountered errors during heartbeat loop, so take into
        // account possible ETCd accessibility problems when re-registering the service.
        let service_descriptor = self
            .service_registrator
            .as_mut()
            .unwrap()
            .register_service(true)
            .await?;

        // Service successfully re-registered, notify heartbeat handler.
        self.notify_heartbeat_status(HeartbeatStatus::Restored);

        Ok(ServiceState::Running(RunningState { service_descriptor }))
    }
}

impl DynamicService {
    pub async fn start(mut self) -> Result<(), ServiceError> {
        loop {
            let curr_state = self
                .current_state
                .take()
                .expect("Current state must be defined");
            let curr_state_name = curr_state.name();

            let new_state = match curr_state {
                ServiceState::New(state) => self.state_step(state).await,
                ServiceState::Running(state) => self.state_step(state).await,
                ServiceState::Reinitializing(state) => self.state_step(state).await,
                ServiceState::Done => break,
            }?;

            if curr_state_name != new_state.name() {
                tracing::debug!(
                    "Service {}:{} changed state {} => {}",
                    self.service_config.name(),
                    self.service_config.instance_id(),
                    curr_state_name,
                    new_state.name()
                )
            }

            self.current_state = Some(new_state);
        }

        Ok(())
    }

    fn notify_heartbeat_status(&mut self, status: HeartbeatStatus) {
        self.hb_status_handler.as_mut().map(|f| f(status));
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn shutdown_service(&mut self) -> Result<(), ServiceError> {
        if self.shutdown_handler.is_none() {
            Ok(())
        } else {
            self.shutdown_handler
                .take()
                .unwrap()
                .await
                .map_err(|e| ServiceError::ServiceShutdown {
                    service: self.service_config.name().to_string(),
                    instance: self.service_config.instance_id().to_string(),
                    error: e,
                })
        }
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

    /// Start heartbeat loop for the service.
    /// In case this function returns error, it's always an indication of lost heartbet
    /// and the caller must take an appropriate action in response to heartbeat loss.
    async fn start_heartbeat(&mut self) -> Result<(), ServiceError> {
        assert!(self.hb_channel.is_none(), "Heartbeat loop already started");

        let (sender, mut receiver) = channel::<HeartbeatLoopCommand>(1);

        self.hb_channel = Some(sender);

        let ttl: u64 = self.ttl as u64;
        let service = self.service.clone();
        let instance = self.instance.clone();
        let descr = Arc::clone(&self.descriptor);

        tracing::info!(
            "Service {}:{} starting heartbeat loop with interval {} secs.",
            self.service,
            self.instance,
            ttl
        );

        let mut heartbeat_lost_time = None;

        let h = tokio::spawn(async move {
            loop {
                match descr.lock().await.as_mut() {
                    Some(d) => {
                        // We care only about TTL loss reported explicitly, as it's
                        // the only reliable trigger for heartbeat-loss related actions.
                        // Direct I/O errors upon heartbeat update don't count as a valid
                        // reason to judge about heartbeat loss unless all attempts to report
                        // heartbeat failed within TTL time (measured since the moment of
                        // the first heartbeat update failure).
                        if let Err(e) = d.send_heartbeat().await {
                            match e {
                                ServiceError::HeartbeatLost { .. } => {
                                    tracing::error!(
                                        "Service {}:{} lost heartbeat",
                                        service,
                                        instance,
                                    );
                                    return Err(e);
                                }
                                _ => {
                                    if heartbeat_lost_time.is_none() {
                                        heartbeat_lost_time = Some(std::time::Instant::now());

                                        tracing::error!(
                                            "Service {}:{} failed to send heartbeat, error={}",
                                            service,
                                            instance,
                                            e,
                                        );
                                    } else {
                                        // Check whether TTL is elapsed and report error in case
                                        // all heartbeat update attemps failed within TTL interval.
                                        let elapsed =
                                            heartbeat_lost_time.as_ref().unwrap().elapsed();
                                        if elapsed.as_secs() > ttl {
                                            tracing::error!(
                                                "Service {}:{} lost heartbeat due to exceeding heartbeat TTL ({} sec)",
                                                service,
                                                instance,
                                                ttl,
                                            );
                                            return Err(ServiceError::HeartbeatLost {
                                                service,
                                                instance,
                                            });
                                        }
                                    }
                                }
                            }
                        } else {
                            // Heartbeat succeeded, reset the failure time.
                            if heartbeat_lost_time.is_some() {
                                heartbeat_lost_time.take();
                                tracing::info!(
                                    "Service {}:{} successfully recovered heartbeat within TTL interval",
                                    service,
                                    instance,
                                );
                            }
                        }
                    }
                    // Service is being unregistered, break the heartbeat loop.
                    None => {
                        tracing::info!(
                            "Service {}:{} is being unregistered, stopping heartbeat loop",
                            service,
                            instance,
                        );
                        receiver.close();
                        break;
                    }
                }

                // Waiti till either TTL update timeout triggers or request to stop
                // heartbeat loop is received.
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(ttl as u64 / 2)) => {},

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
        if let Some(channel) = self.hb_channel.take() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            // Take into account simultaneous heartbeat loop closure, which might happen either
            // in response to explicit service unregistration request or implicutly upon
            // heartbeat delivery failures.
            if channel
                .send(HeartbeatLoopCommand::StopHeartbeat(tx))
                .await
                .is_ok()
            {
                // Channel might have been closed straight after we have sent the message,
                // so be careful.
                if rx.await.is_err() {
                    tracing::warn!("Heartbeat loop already stopped");
                }
            }
        }
    }

    /// Unregister the service and free the service descriptor.
    async fn unregister(self) {
        self.descriptor.lock().await.take();
    }
}
