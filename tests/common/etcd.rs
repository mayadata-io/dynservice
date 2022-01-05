use composer::{Builder as ComposeBuilder, ComposeTest, ContainerSpec};
use std::collections::HashMap;
use tempfile::tempdir;

pub struct EtcdCluster {
    compose_test: ComposeTest,
    nodes: Vec<EtcdNode>,
}

#[derive(Clone, Debug)]
pub struct EtcdNode {
    pub id: u32,
    pub name: String,
    pub ip_address: String,
}

pub struct Builder {
    num_nodes: u32,
}

const ETCD_IMAGE: &str = "quay.io/coreos/etcd:v3.5.1";
const PORT_RANGE_START: u32 = 32379; //55379;

fn get_container_mount() -> String {
    tempdir()
        .unwrap()
        .into_path()
        .as_path()
        .to_string_lossy()
        .to_string()
}

impl Builder {
    pub fn new(num_nodes: u32) -> Self {
        assert!(
            num_nodes > 0 && num_nodes <= 5,
            "Invalid number of ETCD nodes"
        );

        Self { num_nodes }
    }

    pub async fn build(self) -> EtcdCluster {
        let mut builder = ComposeBuilder::new();
        let mut nodes: Vec<EtcdNode> = Vec::with_capacity(self.num_nodes as usize);
        builder = builder.autorun(false);

        let mut port_num = PORT_RANGE_START;

        // Calculate overall cluster topology before configuring ETCD nodes.
        let initial_cluster = (1..self.num_nodes + 1)
            .map(|n| format!("node{}=http://10.1.0.{}:2380", n, n + 1))
            .collect::<Vec<String>>()
            .join(",");

        for n in 1..self.num_nodes + 1 {
            let port1 = port_num;
            let port2 = port_num + 1;
            let node_name = format!("node{}", n);

            let mut spec = { ContainerSpec::from_image(&node_name, ETCD_IMAGE) };

            spec = spec.with_portmap("2379", &port1.to_string());
            spec = spec.with_portmap("2380", &port2.to_string());
            spec = spec.with_cmd("/usr/local/bin/etcd");
            //spec = spec.with_bind("/tmp/etcd", "/etcd-data");

            spec = spec.with_bind(&get_container_mount(), "/etcd-data");
            spec = spec.with_arg("--data-dir");
            spec = spec.with_arg("/etcd-data");

            spec = spec.with_arg("--name");
            spec = spec.with_arg(&node_name);

            spec = spec.with_arg("--initial-advertise-peer-urls");
            spec = spec.with_arg(&format!("http://10.1.0.{}:2380", n + 1));

            spec = spec.with_arg("--listen-peer-urls");
            spec = spec.with_arg("http://0.0.0.0:2380");

            spec = spec.with_arg("--advertise-client-urls");
            spec = spec.with_arg(&format!(
                "http://10.1.0.{}:2379,http://10.1.0.{}:4001",
                n + 1,
                n + 1
            ));

            spec = spec.with_arg("--listen-client-urls");
            spec = spec.with_arg("http://0.0.0.0:2379,http://0.0.0.0:4001");

            spec = spec.with_arg("--initial-cluster-token");
            spec = spec.with_arg("etcd-test-cluster-1");

            spec = spec.with_arg("--initial-cluster-state");
            spec = spec.with_arg("new");

            spec = spec.with_arg("--initial-cluster");
            spec = spec.with_arg(&initial_cluster);

            builder = builder.add_container_spec(spec);
            nodes.push(EtcdNode {
                id: n - 1,
                name: node_name,
                ip_address: format!("127.0.0.1:{}", port1),
            });

            port_num = port_num + 2;
        }

        let compose_test = builder.build().await.unwrap();

        EtcdCluster {
            compose_test,
            nodes,
        }
    }
}

impl EtcdCluster {
    pub async fn start(&mut self) {
        // Start all containers.
        for node in self.nodes.iter() {
            self.compose_test
                .start(&node.name)
                .await
                .expect("Failed to start ETCD node");
        }

        // Wait till all ETCD nodes are up and running.
        for node in self.nodes.iter() {
            self.wait_node(node).await;
        }
    }

    pub async fn stop(&mut self) {
        // Stop all containers.
        for n in self.nodes.clone().iter() {
            self.stop_node(n.id).await;
        }
    }

    pub async fn stop_node(&mut self, node_id: u32) {
        assert!(
            (node_id as usize) < self.nodes.len(),
            "Unknown ETCD node ID: {}",
            node_id
        );

        self.compose_test
            .stop(&self.nodes[node_id as usize].name)
            .await
            .expect("Failed to stop ETCD node");
    }

    async fn wait_node(&self, node: &EtcdNode) {
        let mut payload = HashMap::new();
        payload.insert("key", "Zm9v"); // Zm9v is 'foo' in Base64
        let client = reqwest::Client::new();
        let a = format!("http://{}/v3/kv/range", node.ip_address);

        for _i in 0..5 {
            let u = reqwest::Url::parse(&a).unwrap();
            match client.post(u).json(&payload).send().await {
                Ok(_) => return,
                Err(_) => {
                    // In case of error sleep for 1 sec.
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
        // Panic if the node didn't start.
        panic!("ETCD node {} did not start within 5 seconds", node.name);
    }

    pub async fn start_node(&mut self, node_id: u32) {
        assert!(
            (node_id as usize) < self.nodes.len(),
            "Unknown ETCD node ID: {}",
            node_id
        );

        let node = &self.nodes[node_id as usize];

        // Start ETCD container.
        self.compose_test
            .start(&node.name)
            .await
            .expect("Failed to start ETCD node");

        // Make sure ETCD node started successfully.
        self.wait_node(node).await;
    }

    pub fn get_nodes(&self) -> Vec<EtcdNode> {
        self.nodes.clone()
    }
}
