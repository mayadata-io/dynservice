use crate::store::common::{LeaseKeepAlive, LeaseRevoke, StoreError};
use etcd_client::{LeaseClient, LeaseKeepAliveStream, LeaseKeeper};
use snafu::ResultExt;
use std::fmt;

pub struct TimedLease {
    id: i64,
    client: LeaseClient,
    keeper: LeaseKeeper,
    response_stream: LeaseKeepAliveStream,
    ttl: i64,
}

impl fmt::Debug for TimedLease {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TimedLease")
            .field("id", &self.id)
            .field("ttl", &self.ttl)
            .finish()
    }
}

impl TimedLease {
    pub async fn new(id: i64, ttl: i64, mut client: LeaseClient) -> Result<Self, StoreError> {
        let (keeper, response_stream) = client
            .keep_alive(id)
            .await
            .context(LeaseKeepAlive { lease: id })?;

        Ok(Self {
            id,
            ttl,
            client,
            keeper,
            response_stream,
        })
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn ttl(&self) -> i64 {
        self.ttl
    }

    pub async fn keep_alive(&mut self) -> Result<(), StoreError> {
        // Phase 1: update lease keep-alive.
        self.keeper
            .keep_alive()
            .await
            .context(LeaseKeepAlive { lease: self.id })?;

        // Phase 2: Check the response stream to validate if the lease stil exists.
        let m = self
            .response_stream
            .message()
            .await
            .context(LeaseKeepAlive { lease: self.id })?;

        // Check lease's current TTL.
        if let Some(resp) = m {
            if resp.ttl() == 0 {
                return Err(StoreError::LeaseLost { lease: self.id });
            }
        }

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
