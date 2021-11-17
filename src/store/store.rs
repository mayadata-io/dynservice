use etcd_client::{Client, GetOptions, PutOptions, WatchOptions};

use crate::store::{
    common::{Connect, GetPrefix, LeaseGrant, Put, SerializeValue, WatchCreate},
    StoreError, StoreWatcher, TimedLease,
};
use serde::Serialize;
use serde_json::Value;
use snafu::ResultExt;

pub struct KeyValueStore {
    client: Client,
}

/// Abstraction for a key-value store that allows storing/loading JSON-encoded values
/// and supports events watching.
impl KeyValueStore {
    pub async fn new<E: AsRef<str>, S: AsRef<[E]>>(endpoints: S) -> Result<Self, StoreError> {
        let client = Client::connect(endpoints, None).await.context(Connect {})?;
        Ok(Self { client })
    }

    pub async fn put<V: Serialize>(
        &mut self,
        key: impl Into<String>,
        value: &V,
        lease: &TimedLease,
    ) -> Result<(), StoreError> {
        let val = serde_json::to_value(value).context(SerializeValue {})?;
        let payload = serde_json::to_vec(&val).context(SerializeValue {})?;
        let options = PutOptions::new().with_lease(lease.id());
        let key = key.into();

        self.client
            .put(key.to_string(), payload, Some(options))
            .await
            .context(Put {
                key,
                value: serde_json::to_string(&val).context(SerializeValue {})?,
            })?;
        Ok(())
    }

    pub async fn grant_lease(&mut self, ttl: i64) -> Result<TimedLease, StoreError> {
        if ttl <= 0 {
            return Err(StoreError::InvalidLeaseTTL { ttl });
        }

        let lease = self
            .client
            .lease_grant(ttl, None)
            .await
            .context(LeaseGrant { ttl })?;

        Ok(TimedLease::new(lease.id(), self.client.lease_client()))
    }

    pub async fn get_prefix(&mut self, prefix: String) -> Result<Vec<Value>, StoreError> {
        let opts = GetOptions::new().with_prefix();

        let response = self
            .client
            .get(prefix.to_string(), Some(opts))
            .await
            .context(GetPrefix {
                prefix: prefix.to_string(),
            })?;

        let mut values: Vec<Value> = Vec::new();
        for kv in response.kvs() {
            match serde_json::from_slice(kv.value()) {
                Ok(v) => {
                    values.push(v);
                }
                Err(e) => {
                    tracing::error!("Failed to deserialize object. Error = {}", e);
                }
            };
        }

        Ok(values)
    }

    pub async fn get_watcher(
        &mut self,
        key: String,
        prefixed_watch: bool,
    ) -> Result<StoreWatcher, StoreError> {
        let options = if prefixed_watch {
            let options = WatchOptions::new().with_prefix();
            Some(options)
        } else {
            None
        };

        let (_, stream) = self
            .client
            .watch(key.clone(), options)
            .await
            .context(WatchCreate { key: key.clone() })?;

        Ok(StoreWatcher::new(key, stream))
    }
}
