use std::sync::{Arc, Mutex};

use crate::bplustree::epoch::EpochManager;

#[derive(Default, Debug)]
struct EpochState {
    advances: u64,
    oldest_active: u64,
    to_reclaim: Vec<u64>,
}

/// Simple mock for EpochManager:
/// - count `advance()` calls
/// - configurable `oldest_active`
/// - returns a preset reclaim list
#[derive(Clone)]
pub struct TestEpoch {
    state: Arc<Mutex<EpochState>>,
}

impl TestEpoch {
    pub fn new() -> Self {
        Self { state: Arc::new(Mutex::new(EpochState::default())) }
    }

    pub fn set_oldest_active(&self, e: u64) {
        self.state.lock().unwrap().oldest_active = e;
    }

    pub fn set_reclaim_list(&self, pages: Vec<u64>) {
        self.state.lock().unwrap().to_reclaim = pages;
    }

    pub fn advance_count(&self) -> u64 {
        self.state.lock().unwrap().advances
    }
}

impl TestEpoch {
    fn advance(&self) {
        self.state.lock().unwrap().advances += 1;
    }

    fn oldest_active(&self) -> u64 {
        self.state.lock().unwrap().oldest_active
    }

    fn reclaim(&self, _safe_epoch: u64) -> Vec<u64> {
        self.state.lock().unwrap().to_reclaim.clone()
    }
}
