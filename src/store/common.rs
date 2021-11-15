use serde_json::Value;
use snafu::Snafu;

#[derive(Snafu, Debug)]
#[snafu(visibility = "pub")]
pub enum StoreError {
    /// Failed to connect to object store.
    #[snafu(display("Failed to connect to object store store. Error={}", source))]
    Connect { source: etcd_client::Error },

    /// Failed to serialize value to JSON.
    #[snafu(display("Failed to serialize value. Error={}", source))]
    SerializeValue { source: serde_json::Error },

    /// Failed to transform the key into string.
    #[snafu(display("Failed to transform key into string"))]
    KeyXform { source: etcd_client::Error },

    /// Failed to store value.
    #[snafu(display(
        "Failed to store entry with key '{}' and value '{}'. Error = {}",
        key,
        value,
        source,
    ))]
    Put {
        key: String,
        value: String,
        source: etcd_client::Error,
    },

    /// Failed to get value(s).
    #[snafu(display("Failed to get values for prefix {}. Error={}", prefix, source))]
    GetPrefix {
        prefix: String,
        source: etcd_client::Error,
    },

    /// Failed to grant lease.
    #[snafu(display("Failed to grant lease with TTL {}. Error = {}", ttl, source))]
    LeaseGrant {
        ttl: i64,
        source: etcd_client::Error,
    },

    /// Failed to update lease keep-alive.
    #[snafu(display("Failed to update keep alive for lease {}. Error={}", lease, source))]
    LeaseKeepAlive {
        lease: i64,
        source: etcd_client::Error,
    },

    /// Failed to revoke lease.
    #[snafu(display("Failed to revoke lease {}. Error={}", lease, source))]
    LeaseRevoke {
        lease: i64,
        source: etcd_client::Error,
    },

    /// Invalid lease TTL.
    #[snafu(display("Invalid lease TTL: {}", ttl))]
    InvalidLeaseTTL { ttl: i64 },

    /// Failed to create a key watcher.
    #[snafu(display("Failed to create watcher for a key '{}'. Error={}", key, source))]
    WatchCreate {
        key: String,
        source: etcd_client::Error,
    },

    /// Error occurred during event watching.
    #[snafu(display("Failed to watch events for a key '{}'. Error={}", key, source))]
    Watch {
        key: String,
        source: etcd_client::Error,
    },
}

#[derive(Debug)]
pub enum StoreWatchEvent {
    Put { key: String, value: Value },
    Delete { key: String },
}
