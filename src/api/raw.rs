// ==============================================
// FILE: src/api/raw.rs
// ==============================================
//! Thin bytes-only gRPC wrapper. Adjust `crate::pb` paths to your generated code.


use futures::{Stream, TryStreamExt};
use tonic::transport::{Channel, Endpoint};


use super::encoding::{KeyConstraints, KeyEncodingId};
use super::errors::ApiError;


// --- GENERATED FROM YOUR .proto ---
// Adjust this to whatever module `tonic-build` produced in your crate.
pub mod pb {
tonic::include_proto!("kv");
}


use pb::{CreateTreeRequest, DelRequest, DescribeTreeRequest, GetRequest, PutRequest, RangeRequest, Tree};

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
