use crate::{
    common::{ServiceDeserialize, WatchService},
    store::{StoreWatchEvent, StoreWatcher},
    Service, ServiceError,
};
use snafu::ResultExt;
use std::{collections::VecDeque, convert::TryFrom};

pub struct ServiceWatcher {
    watcher: StoreWatcher,
    active: bool,
    pending_events: VecDeque<StoreWatchEvent>,
}

impl ServiceWatcher {
    pub(crate) fn new(watcher: StoreWatcher) -> Self {
        Self {
            watcher,
            active: true,
            pending_events: VecDeque::new(),
        }
    }
}

#[derive(Debug)]
pub enum ServiceEvent {
    ServiceAdded { service: Service },
    ServiceRemoved { key: String },
}

impl TryFrom<StoreWatchEvent> for ServiceEvent {
    type Error = ServiceError;

    fn try_from(e: StoreWatchEvent) -> Result<Self, Self::Error> {
        match e {
            StoreWatchEvent::Put { key: _, value } => {
                let service =
                    serde_json::from_value::<Service>(value).context(ServiceDeserialize {})?;
                Ok(ServiceEvent::ServiceAdded { service })
            }
            StoreWatchEvent::Delete { key } => Ok(ServiceEvent::ServiceRemoved { key }),
        }
    }
}

impl ServiceWatcher {
    pub async fn watch(&mut self) -> Result<ServiceEvent, ServiceError> {
        // Check if there are any pending events to be delivered.
        if !self.pending_events.is_empty() {
            let watch_event = self.pending_events.pop_front().unwrap();
            return Ok(ServiceEvent::try_from(watch_event)?);
        }

        // Check if the watcher is still active.
        if !self.active {
            return Err(ServiceError::WatchStreamClosed {
                key: self.watcher.key().to_string(),
            });
        }

        let mut watch_events = self.watcher.watch().await.context(WatchService {
            key: self.watcher.key().to_string(),
        })?;

        // Check whether watch stream is cancelled.
        if watch_events.is_empty() {
            tracing::warn!(
                "Store watcher's stream is cancelled, no more service events can be received"
            );
            self.active = false;
            return Err(ServiceError::WatchStreamClosed {
                key: self.watcher.key().to_string(),
            });
        }

        // In case there are more than 1 event observed, enqueue the remaining events.
        if watch_events.len() > 1 {
            watch_events.drain(1 .. watch_events.len()).for_each(|w| {
                self.pending_events.push_back(w);
            })
        }

        let watch_event = watch_events.remove(0);
        Ok(ServiceEvent::try_from(watch_event)?)
    }
}
