use crate::store::common::{LeaseKeepAlive, LeaseRevoke};
use crate::store::StoreError;
use etcd_client::LeaseClient;
use snafu::ResultExt;

pub struct TimedLease {
    id: i64,
    client: LeaseClient,
}

impl TimedLease {
    pub fn new(id: i64, client: LeaseClient) -> Self {
        Self { id, client }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub async fn keep_alive(&mut self) -> Result<(), StoreError> {
        self.client
            .keep_alive(self.id)
            .await
            .context(LeaseKeepAlive { lease: self.id })?;
        Ok(())
    }

    pub async fn revoke(&mut self) -> Result<(), StoreError> {
        self.client
            .revoke(self.id)
            .await
            .context(LeaseRevoke { lease: self.id })?;
        Ok(())
    }
}
