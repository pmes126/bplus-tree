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

#[derive(Clone)]
pub struct RawClient<T: KvService> {
    svc: Arc<T>,
}
impl<T: KvService> RawClient<T> {
    pub fn new(svc: Arc<T>) -> Self { Self { svc } }
    pub async fn create_tree(&self, n:&str, e:KeyEncodingId, kc:KeyConstraints)->Result<TreeMeta,ApiError>{ 
        self.svc.create_tree(n,e,kc).await 
    }
    pub async fn describe_tree(&self, n:&str)->Result<TreeMeta,ApiError>{ self.svc.describe_tree(n).await }
    pub async fn put(&self, id:&[u8], k:Vec<u8>, v:Vec<u8>)->Result<(),ApiError>{ self.svc.put(id,k,v).await }
    pub async fn get(&self, id:&[u8], k:Vec<u8>)->Result<Option<Vec<u8>>,ApiError>{ self.svc.get(id,k).await }
    pub async fn del(&self, id:&[u8], k:Vec<u8>)->Result<bool,ApiError>{ self.svc.del(id,k).await }
    pub async fn range(
        &self, id:&[u8], s:Option<Vec<u8>>, e:Option<Vec<u8>>, r:bool, lim:u32
    )->Result<Pin<Box<dyn Stream<Item=Result<(Vec<u8>,Vec<u8>),ApiError>> + Send>>, ApiError>{
        self.svc.range(id,s,e,r,lim).await
    }
}
