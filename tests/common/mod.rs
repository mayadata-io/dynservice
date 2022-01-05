pub mod etcd;

use dynservice::ServiceRegistryOptions;
use etcd::EtcdCluster;

impl EtcdCluster {
    // Helper function to generate ServiceRegistry config for this ETCD cluster.
    pub fn as_service_registry_options(&self) -> ServiceRegistryOptions {
        let etcd_endpoints = self
            .get_nodes()
            .into_iter()
            .map(|n| n.ip_address)
            .collect::<Vec<_>>();

        ServiceRegistryOptions::builder()
            .with_endpoints(&etcd_endpoints)
            .build()
    }
}
