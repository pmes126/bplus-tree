use self::storage::file_store::FileStore;
use crate::api::DbBuilder;
use ::bplustree::*;

use reqwest::Error;
use storage::page_store::PageStore;
use tempfile::TempDir;
use tokio::time::{Duration, interval};

#[tokio::main]
async fn main() {
    let dir = TempDir::new().unwrap();

    let file_path = dir.path().join("tree.data");
    let store = FileStore::<PageStore>::new(&file_path).unwrap();

    let db = DbBuilder::new(store)
        .order(64)
        .build_typed::<u64, String>()
        .unwrap();

    let mut tx = db.begin_write().unwrap();

    let mut ticker = interval(Duration::from_secs(2));
    let url = "https://httpbin.org/get";
    for i in 1..=50 {
        ticker.tick().await;

        match fetch_url(url).await {
            Ok(body) => {
                println!("Request #{}:\n{}", i, body);
                tx.insert(i, body).unwrap();
            }
            Err(err) => eprintln!("Error on request #{}: {}", i, err),
        }
    }
    let _ = tx.commit(db.get_inner());

    let res = db.get_inner().traverse().unwrap();
    for (k, v) in &res {
        println!("key {:?}, value: {:?}", k, v)
    }
}

async fn fetch_url(url: &str) -> Result<String, Error> {
    let response = reqwest::get(url).await?;
    let body = response.text().await?;
    Ok(body)
}
