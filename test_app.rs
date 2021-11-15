use dynservice::{
    ServiceConfig, ServiceEvent, ServiceRegistry, ServiceRegistryOptions,
    ServiceRegistrySearchRequest,
};

const SERVICE_NAME: &str = "testService";

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let options = ServiceRegistryOptions::builder()
        .with_endpoints(["localhost:32379"])
        .build();

    println!("Opening Service Registry");
    let mut registry = ServiceRegistry::new(options)
        .await
        .expect("Failed to initialize Service Registry");
    println!("Registry opened !!!");

    // Start watcher loop.
    let mut service_watcher = registry.watch().await.unwrap();
    tokio::spawn(async move {
        println!("-> Watching for registry events...");
        loop {
            let event = service_watcher.watch().await.unwrap();
            match event {
                ServiceEvent::ServiceAdded { service } => {
                    println!(
                        "# Service instance registered: {}/{}",
                        service.name(),
                        service.instance_id()
                    )
                }
                ServiceEvent::ServiceRemoved { key } => {
                    println!("# Service unergistered: {}", key);
                }
            }
        }
    });

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let opts = ServiceConfig::builder()
        .with_name(SERVICE_NAME)
        .with_instance_id("i1")
        .with_endpioints(["http://1.2.3.4:8080"])
        .build();
    println!("{:?}", opts);
    let mut service_descriptor = registry.register_service(opts).await.unwrap();
    println!("Initialized !");

    // Start heartbeat loop.
    let h = tokio::spawn(async move {
        for _i in 1..5 {
            println!("*");
            service_descriptor
                .send_heartbeat()
                .await
                .expect("Heartbeat update failed");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        println!("> Unregistering service");
        service_descriptor
            .unregister()
            .await
            .expect("Faield to unregister service");
    });

    // Search the registry and lookup our service.
    let req = ServiceRegistrySearchRequest::default().with_service(SERVICE_NAME);
    let mut services = registry.search(req).await.unwrap();
    assert_eq!(services.len(), 1, "No registered service found");
    let service = services.get(0).unwrap();
    assert_eq!(service.name(), SERVICE_NAME, "Service name mismatches");

    // Wait for the heartbeat loop to complete and check if the service is removed.
    println!("* Waiting for heartbeat loop to complete");
    h.await.unwrap();
    println!("* Heartbeat loop completed");
    let req = ServiceRegistrySearchRequest::default().with_service(SERVICE_NAME);
    services = registry.search(req).await.unwrap();
    assert!(services.is_empty(), "Service still exists after removal");
}
