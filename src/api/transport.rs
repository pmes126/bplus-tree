pub mod service;

use bytes::Bytes;
use futures::Stream;

use crate::api::{ApiError, KeyConstraints, KeyEncodingId, KeyRange, Order};
use crate::database::catalog::TreeMeta;

/// Transport-layer tree identifier — a name or opaque handle encoded as bytes.
/// Distinct from the internal numeric `crate::api::TreeId`.
pub type TreeId = Bytes;
pub type ResumeToken = Bytes;

pub trait KvService: Send + Sync + 'static {
    // Associated stream type so implementors can avoid boxing
    type RangeStream<'a>: Stream<Item = Result<(Bytes, Bytes), ApiError>> + Send + 'a
    where
        Self: 'a;

    async fn create_tree(
        &self,
        name: &str,
        enc: KeyEncodingId,
        kc: KeyConstraints,
    ) -> Result<TreeMeta, ApiError>;

    async fn describe_tree(&self, name: &str) -> Result<TreeMeta, ApiError>;

    async fn put(&self, tree: &TreeId, key: &[u8], val: &[u8]) -> Result<(), ApiError>;

    async fn get(&self, tree: &TreeId, key: &[u8]) -> Result<Option<Bytes>, ApiError>;

    async fn del(&self, tree: &TreeId, key: &[u8]) -> Result<bool, ApiError>;

    // range scan with pagination
    async fn range(
        &self,
        tree: &TreeId,
        range: KeyRange<'_>,
        order: Order,
        limit: u32,
        resume: Option<ResumeToken>,
    ) -> Result<
        (
            // streaming out rows (zero-copy `Bytes`)
            std::pin::Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes), ApiError>> + Send>>,
            // next page token (None if done)
            Option<ResumeToken>,
        ),
        ApiError,
    >;
}
