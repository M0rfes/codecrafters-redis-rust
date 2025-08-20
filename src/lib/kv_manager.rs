use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Error, Debug)]
pub enum KvManagerError {
    #[error("")]
    ListNotFound,
}


pub struct KvManager {
    // kv for str str
    strings: DashMap<Arc<str>, Arc<str>>,
    // list for str list
    lists: DashMap<Arc<str>, Arc<RwLock<Vec<Arc<str>>>>>,
    // expiry for str expiry
    expires: DashMap<Arc<str>, u64>,
}

impl KvManager {
    pub fn new() -> Self {
        Self {
            strings: DashMap::new(),
            lists: DashMap::new(),
            expires: DashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<Arc<str>> {
       let val = self.strings.get(key).map(|v| v.value().clone());
       if let Some(val) = val {
        if let Some(expiry) = self.get_expiry(key) {
            let now = Utc::now().timestamp_millis();
            if now > (expiry as i64) {
                self.remove(key);
                return None;
            }
        }
        Some(val)
       } else {
        None
       } 
    }
    
    pub fn set(&self, key: Arc<str>, value: Arc<str>, expiry: Option<u64>) {
        self.strings.insert(key.clone(), value.clone());
        if let Some(expiry) = expiry {
            let now = Utc::now().timestamp_millis();
            let expiry = expiry as i64 + now;
            self.expires.insert(key.clone(), expiry as u64);
        }
    }

    pub fn get_expiry(&self, key: &str) -> Option<u64> {
        self.expires.get(key).map(|v| v.value().clone())
    }

    pub async fn rpush(&self, key: &str, value: Arc<str>) -> usize {
        let Some(list) = self.lists.get_mut(key) else {
            let list = Arc::new(RwLock::new(vec![value.into()]));
            self.lists.insert(key.into(), list);
            return 1;
        };
        let mut list = list.write().await;
        list.push(value.into());
        list.len()
    }

    pub fn remove(&self, key: &str) {
        self.strings.remove(key);
        self.lists.remove(key);
        self.expires.remove(key);
    }

    pub async fn evect_expired(&self){
        let now = Utc::now().timestamp_millis();
        for entry in self.expires.iter() {
            if now > (*entry.value() as i64) {
                self.remove(entry.key());
            }
        }
    }


}