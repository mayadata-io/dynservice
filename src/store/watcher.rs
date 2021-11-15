use crate::store::StoreWatchEvent;
use etcd_client::{EventType, KeyValue, WatchStream};
use serde_json::Value;
use snafu::ResultExt;

use super::StoreError;
use crate::store::common::{KeyXform, SerializeValue};

pub struct StoreWatcher {
    key: String,
    watch_stream: WatchStream,
}

impl StoreWatcher {
    pub fn new(key: String, watch_stream: WatchStream) -> Self {
        Self { key, watch_stream }
    }

    pub async fn watch(&mut self) -> Result<Vec<StoreWatchEvent>, StoreError> {
        loop {
            let response = match self.watch_stream.message().await {
                Ok(msg) => {
                    match msg {
                        Some(r) => r,
                        None => {
                            // Stream cancelled, notify the upper-layer listener.
                            return Ok(Vec::new());
                        }
                    }
                }
                Err(e) => {
                    return Err(StoreError::Watch {
                        key: self.key.to_string(),
                        source: e,
                    })
                }
            };

            let mut events: Vec<StoreWatchEvent> = Vec::new();
            for e in response.events() {
                match e.event_type() {
                    EventType::Put => {
                        if let Some(kv) = e.kv() {
                            match parse_kv(kv, true) {
                                Ok((k, v)) => {
                                    events.push(StoreWatchEvent::Put {
                                        key: k,
                                        value: v.unwrap(),
                                    });
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Failed to deserialize value in PUT event: {}",
                                        e
                                    );
                                }
                            };
                        }
                    }
                    EventType::Delete => {
                        if let Some(kv) = e.kv() {
                            match parse_kv(kv, false) {
                                Ok((key, _)) => events.push(StoreWatchEvent::Delete { key }),
                                Err(e) => {
                                    tracing::error!(
                                        "Failed to deserialize value in DELETE event: {}",
                                        e
                                    );
                                }
                            };
                        }
                    }
                }
            }

            // Report at least one successfully received event.
            if !events.is_empty() {
                return Ok(events);
            }
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

fn parse_kv(kv: &KeyValue, parse_value: bool) -> Result<(String, Option<Value>), StoreError> {
    let key = kv.key_str().context(KeyXform)?.to_string();
    let value = if parse_value {
        Some(serde_json::from_slice::<Value>(kv.value()).context(SerializeValue)?)
    } else {
        None
    };

    Ok((key, value))
}
