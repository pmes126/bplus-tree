pub mod service;
pub use service::{KvService, RawClient, TreeMeta};

#[cfg(feature = "transport-grpc")]
pub mod grpc;
#[cfg(feature = "transport-grpc")]
pub use grpc::GrpcService;

#[cfg(feature = "transport-inproc")]
pub mod inproc;
#[cfg(feature = "transport-inproc")]
pub use inproc::InprocService;


use std::{pin::Pin, sync::Arc};
use futures::Stream;

use crate::api::encoding::{KeyConstraints, KeyEncodingId};
use crate::api::errors::ApiError;

#[derive(Debug, Clone)]
pub struct TreeMeta {
    pub id: Vec<u8>,
    pub name: String,
    pub key_encoding: KeyEncodingId,
    pub constraints: KeyConstraints,
}

#[async_trait::async_trait]
pub trait KvService: Send + Sync + 'static {
    async fn create_tree(&self, name: &str, enc: KeyEncodingId, kc: KeyConstraints)
        -> Result<TreeMeta, ApiError>;
    async fn describe_tree(&self, name: &str) -> Result<TreeMeta, ApiError>;
    async fn put(&self, tree_id: &[u8], key: Vec<u8>, val: Vec<u8>) -> Result<(), ApiError>;
    async fn get(&self, tree_id: &[u8], key: Vec<u8>) -> Result<Option<Vec<u8>>, ApiError>;
    async fn del(&self, tree_id: &[u8], key: Vec<u8>) -> Result<bool, ApiError>;
    async fn range(
        &self,
        tree_id: &[u8],
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        reverse: bool,
        limit: u32,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Vec<u8>, Vec<u8>), ApiError>> + Send>>, ApiError>;
}
