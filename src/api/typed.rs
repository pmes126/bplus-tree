// ==============================================
// FILE: src/api/typed.rs
// ==============================================
//

// ==============================================
use std::sync::Arc;


use futures::{Stream, TryStreamExt};


use super::encoding::{KeyConstraints, KeyEncodingId};
use super::errors::ApiError;
use super::transport::{KvService, RawClient, TreeMeta};


// ================= Public clients =================


#[derive(Clone)]
pub struct DbClient<T: KvService> {
pub(crate) raw: RawClient<T>,
}


impl<T: KvService> DbClient<T> {
/// Build a DbClient from a transport service instance.
pub fn from_service(svc: Arc<T>) -> Self {
Self { raw: RawClient::new(svc) }
}


/// Bind a typed client to a named tree. Fails fast if `K` is incompatible
/// with the tree's pinned key encoding / constraints.
pub async fn bind<K, V>(&self, tree_name: &str) -> Result<TypedClient<K, V, T>, ApiError>
where
K: KeyArg,
V: ValueArg,
{
let meta = self.raw.describe_tree(tree_name).await?;
if !K::supports(meta.key_encoding, meta.constraints) {
return Err(ApiError::IncompatibleKeyType { expected: meta.key_encoding });
}
Ok(TypedClient {
raw: self.raw.clone(),
meta: Arc::new(meta),
_k: std::marker::PhantomData,
_v: std::marker::PhantomData,
})
}


/// Optional helper to create a new tree with a pinned encoding.
pub async fn create_tree(
&self,
name: &str,
enc: KeyEncodingId,
constraints: KeyConstraints,
) -> Result<TreeMeta, ApiError> {
self.raw.create_tree(name, enc, constraints).await
}
}


// ---- Convenience constructors per transport (feature-gated) ----


#[cfg(feature = "transport-grpc")]
impl DbClient<crate::api::transport::GrpcService> {
/// Connect to a remote server via gRPC.
pub async fn connect(uri: &str) -> Result<Self, ApiError> {
let svc = crate::api::transport::GrpcService::connect(uri).await?;
Ok(Self::from_service(Arc::new(svc)))
}
}


#[cfg(feature = "transport-inproc")]
impl<E> DbClient<crate::api::transport::InprocService<E>>
where
crate::api::transport::InprocService<E>: KvService,
{
/// Wrap an in-process engine. Your `InprocService` should expose `new(engine)`
/// that adapts the engine to `KvService`.
pub fn inproc(engine: Arc<E>) -> Self {
let svc = crate::api::transport::InprocService::new(engine);
Self::from_service(Arc::new(svc))
}
}


// ================= Typed handle =================

pub struct TypedClient<K, V, T: KvService> {
pub(crate) raw: RawClient<T>,
pub(crate) meta: Arc<TreeMeta>,
_k: std::marker::PhantomData<K>,
_v: std::marker::PhantomData<V>,
}

impl<K, V, T> core::fmt::Debug for TypedClient<K, V, T>
where
T: KvService,
{
fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
f.debug_struct("TypedClient")
.field("tree", &self.meta.name)
.field("encoding", &self.meta.key_encoding.to_string())
.finish()
}
}


impl<K, V, T> TypedClient<K, V, T>
where
K: KeyArg,
V: ValueArg,
T: KvService,
{
#[inline]
pub fn encoding(&self) -> KeyEncodingId { self.meta.key_encoding }


#[inline]
pub fn constraints(&self) -> KeyConstraints { self.meta.constraints }


/// Put a value by typed key.
pub async fn put(&self, key: K, val: V) -> Result<(), ApiError> {
let kb = key.encode_key(self.meta.key_encoding, self.meta.constraints)?;
let vb = val.encode_value();
self.raw.put(&self.meta.id, kb, vb).await
}


/// Get by typed key.
pub async fn get(&self, key: K) -> Result<Option<V>, ApiError> {
let kb = key.encode_key(self.meta.key_encoding, self.meta.constraints)?;
let out = self.raw.get(&self.meta.id, kb).await?;
out.map(|b| V::decode_value(&b)).transpose().map_err(Into::into)
}


/// Delete by typed key.
pub async fn del(&self, key: K) -> Result<bool, ApiError> {
let kb = key.encode_key(self.meta.key_encoding, self.meta.constraints)?;
self.raw.del(&self.meta.id, kb).await
}


/// Range scan. Returns a stream of `(raw_key_bytes, decoded_value)` to avoid
/// lying about key decode for arbitrary encodings.
pub async fn range(
&self,
start: Option<K>,
end: Option<K>,
reverse: bool,
limit: u32,
) -> Result<impl Stream<Item = Result<(Vec<u8>, V), ApiError>>, ApiError> {
let s_enc = match start { Some(k) => Some(k.encode_key(self.meta.key_encoding, self.meta.constraints)?), None => None };
let e_enc = match end { Some(k) => Some(k.encode_key(self.meta.key_encoding, self.meta.constraints)?), None => None };


let stream = self
.raw
.range(&self.meta.id, s_enc, e_enc, reverse, limit)
.await?
.and_then(|(k, v)| async move { Ok((k, V::decode_value(&v)?)) });
Ok(stream)
}
}

// ================= Key/Value traits & impls =================
pub trait ValueArg: Sized {
fn encode_value(self) -> Vec<u8>;
fn decode_value(b: &[u8]) -> Result<Self, ApiError>;
}


// ---- Built-in KeyArg impls ----


impl KeyArg for u64 {
fn supports(enc: KeyEncodingId, kc: KeyConstraints) -> bool {
matches!(enc, KeyEncodingId::BeU64) && (!kc.fixed_key_len || kc.key_len == 8)
}
fn encode_key(self, enc: KeyEncodingId, kc: KeyConstraints) -> Result<Vec<u8>, ApiError> {
if !Self::supports(enc, kc) { return Err(ApiError::IncompatibleKeyType { expected: enc }); }
Ok(self.to_be_bytes().to_vec())
}
}


impl KeyArg for i64 {
fn supports(enc: KeyEncodingId, kc: KeyConstraints) -> bool {
matches!(enc, KeyEncodingId::ZigZagI64) && (!kc.fixed_key_len || kc.key_len == 8)
}
fn encode_key(self, enc: KeyEncodingId, kc: KeyConstraints) -> Result<Vec<u8>, ApiError> {
if !Self::supports(enc, kc) { return Err(ApiError::IncompatibleKeyType { expected: enc }); }
let zz = ((self << 1) ^ (self >> 63)) as u64;
Ok(zz.to_be_bytes().to_vec())
}
}


impl KeyArg for String {
fn supports(enc: KeyEncodingId, _kc: KeyConstraints) -> bool { matches!(enc, KeyEncodingId::Utf8) }
fn encode_key(self, enc: KeyEncodingId, kc: KeyConstraints) -> Result<Vec<u8>, ApiError> {
if !Self::supports(enc, kc) { return Err(ApiError::IncompatibleKeyType { expected: enc }); }
Ok(self.into_bytes())
}
}


impl KeyArg for Vec<u8> {
fn supports(_enc: KeyEncodingId, _kc: KeyConstraints) -> bool { true }
fn encode_key(self, _enc: KeyEncodingId, _kc: KeyConstraints) -> Result<Vec<u8>, ApiError> { Ok(self) }
}


// ---- Built-in ValueArg impls ----


impl ValueArg for Vec<u8> {
fn encode_value(self) -> Vec<u8> { self }
fn decode_value(b: &[u8]) -> Result<Self, ApiError> { Ok(b.to_vec()) }
}


impl ValueArg for String {
fn encode_value(self) -> Vec<u8> { self.into_bytes() }
fn decode_value(b: &[u8]) -> Result<Self, ApiError> {
String::from_utf8(b.to_vec()).map_err(|e| ApiError::Decode(e.to_string()))
}
}


#[cfg(feature = "serde")]
impl<T> ValueArg for T
where
T: serde::Serialize + serde::de::DeserializeOwned,
{
fn encode_value(self) -> Vec<u8> {
bincode::serialize(&self).expect("bincode serialize")
}
fn decode_value(b: &[u8]) -> Result<Self, ApiError> {
bincode::deserialize(b).map_err(|e| ApiError::Decode(e.to_string()))
}
}
