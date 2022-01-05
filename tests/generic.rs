use dynservice::{service::Builder, ServiceConfig};
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
async fn test_service_registration() {
    let cluster = get_etcd_cluster().await;

    static MAIN_CALLS: AtomicU64 = AtomicU64::new(0);
    static INIT_CALLS: AtomicU64 = AtomicU64::new(0);

    // Service entrypoint that simulates some short work.
    async fn service_main() -> Result<(), String> {
        MAIN_CALLS.fetch_add(1, Ordering::SeqCst);
        for _i in 1..3 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        Ok(())
    }

    // Service initializer.
    async fn service_init() -> Result<(), String> {
        INIT_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    // Start a service and check that entrypoint is called.
    let service_config = ServiceConfig::builder()
        .with_name(SERVICE_NAME)
        .with_instance_id("i1")
        .with_endpoints(["http://1.2.3.4:8080"])
        .build();

    let service = Builder::new(service_config, cluster.as_service_registry_options())
        .with_initializer(service_init())
        .with_entrypoint(service_main())
        .build();

    service.start().await.unwrap();

    // Make sure main routine and initializer were called.
    assert_eq!(
        1,
        MAIN_CALLS.load(Ordering::Relaxed),
        "Incorrect number of service entry point invocations"
    );

    assert_eq!(
        1,
        INIT_CALLS.load(Ordering::Relaxed),
        "Incorrect number of service initializer invocations"
    );
}
