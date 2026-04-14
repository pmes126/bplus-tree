#[derive(Clone)]
pub struct EmbeddedService {
    store: std::sync::Arc<Database>,
}

impl EmbeddedService {
    pub async fn open(path: &std::path::Path) -> Result<Self, ApiError> {
        let store = crate::database::open(path).map_err(|e| ApiError::Internal(e.to_string()))?;
        Ok(Self { store: std::sync::Arc::new(store) })
    }
}

#[async_trait::async_trait]
impl KvService for EmbeddedService {
    async fn create_tree(&self, name: &str, enc: KeyEncodingId, limits: Option<KeyLimits>)
        -> Result<TreeMeta, ApiError>
    {
        self.store.create_tree(name, enc, limits).map_err(|e| ApiError::Internal(e.to_string()))
    }
    async fn describe_tree(&self, name: &str) -> Result<TreeMeta, ApiError> {
        self.store.describe_tree(name).map_err(|e| ApiError::Internal(e.to_string()))
    }
    async fn put(&self, id: &TreeId, key: &[u8], val: &[u8]) -> Result<(), ApiError> {
        self.store.put(id, key, val).map_err(|e| ApiError::Internal(e.to_string()))
    }
    // get/del/range similar…
}
