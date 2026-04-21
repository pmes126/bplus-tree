//! End-to-end example: fetch a URL in a loop and store responses by request number.

use bplus_tree::api::Db;
use reqwest::Error;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::interval;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path())?;
    let tree = db.create_tree::<u64, String>("requests", 64)?;

    let mut ticker = interval(Duration::from_secs(2));
    let url = "https://httpbin.org/get";
    let mut txn = tree.txn();
    for i in 1u64..=5 {
        ticker.tick().await;
        match fetch_url(url).await {
            Ok(body) => {
                println!("Request #{i}");
                txn.insert(&i, &body);
            }
            Err(err) => eprintln!("Error on request #{i}: {err}"),
        }
    }
    txn.commit()?;
    println!("stored {} entries", tree.len());
    Ok(())
}

async fn fetch_url(url: &str) -> Result<String, Error> {
    reqwest::get(url).await?.text().await
}
