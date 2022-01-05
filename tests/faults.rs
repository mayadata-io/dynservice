use dynservice::{service::Builder, HeartbeatFailurePolicy, ServiceConfig};
use std::sync::atomic::{AtomicU64, Ordering};

mod common;
use common::etcd::EtcdCluster;

const SERVICE_NAME: &str = "test_service";

async fn get_etcd_cluster() -> EtcdCluster {
    let mut cluster = common::etcd::Builder::new(3).build().await;
    cluster.start().await;
    cluster
}

#[tokio::test]
async fn test_etcd_node_failure() {
    let cluster = get_etcd_cluster().await;

    // Service entrypoint that simulates some short work.
    async fn service_main() -> Result<(), String> {
        for _i in 1..7 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        Ok(())
    }

    // Helper to stop ETCD node.
    async fn stop_node(mut cluster: EtcdCluster) -> EtcdCluster {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        cluster.stop_node(0).await;
        cluster
    }

    let service_config = ServiceConfig::builder()
        .with_name(SERVICE_NAME)
        .with_instance_id("i1")
        .with_endpoints(["http://1.2.3.4:8080"])
        .with_heartbeat_interval(1)
        .build();

    let service = Builder::new(service_config, cluster.as_service_registry_options())
        .with_entrypoint(service_main())
        .build();

    let (res, _) = tokio::join!(service.start(), stop_node(cluster),);

    res.expect("Service did not complete gracefully after ETCD node failure");
}

#[tokio::test]
async fn test_etcd_cluster_failure() {
    let cluster = get_etcd_cluster().await;

    static SHUTDOWN_CALLS: AtomicU64 = AtomicU64::new(0);

    // Service entrypoint that simulates some short work.
    async fn service_main() -> Result<(), String> {
        for _i in 1..10 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        Ok(())
    }

    // Service entrypoint that simulates some short work.
    async fn service_shutdown() -> Result<(), String> {
        SHUTDOWN_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    // Helper to stop ETCD cluster.
    async fn stop_cluster(mut cluster: EtcdCluster) -> EtcdCluster {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        cluster.stop().await;
        cluster
    }

    let service_config = ServiceConfig::builder()
        .with_name(SERVICE_NAME)
        .with_instance_id("i1")
        .with_endpoints(["http://1.2.3.4:8080"])
        .with_heartbeat_interval(1)
        .build();

    let service = Builder::new(service_config, cluster.as_service_registry_options())
        .with_entrypoint(service_main())
        .with_shutdown_handler(service_shutdown())
        .with_heartbeat_failure_policy(HeartbeatFailurePolicy::Stop)
        .build();

    let (res, _) = tokio::join!(service.start(), stop_cluster(cluster),);

    // Service should complete gracefully and shutdown handler must have been called.
    res.expect("Service did not complete gracefully after ETCD cluster failure");

    assert_eq!(
        1,
        SHUTDOWN_CALLS.load(Ordering::Relaxed),
        "Incorrect number of service entry point invocations"
    );
}
