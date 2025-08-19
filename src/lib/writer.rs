use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use thiserror::Error;
use tracing::info;

use crate::command::{Command, Response};




pub struct Writer {
    kv: Arc<(DashMap<Arc<str>, Arc<str>>,DashMap<Arc<str>, u64>)>,
}

impl Writer {
    pub fn new(
        kv: Arc<(DashMap<Arc<str>, Arc<str>>,DashMap<Arc<str>, u64>)>,
    ) -> Self {
        Self { kv }
    }

   pub async fn process(
        &mut self,
        command: Command,
    ) -> Response {
        match command {
            Command::GET(key) => {
                    let mut response = if let Some(value) = self.kv.0.get(&key) {
                        info!("Value: {:?}", value.value());
                        Response::GET(value.value().clone())
                    } else {
                        Response::NULL
                    };

                    let mut expired = false;
                    if let Response::GET(_) = response {
                        if let Some(expiry) = self.kv.1.get(&key) {
                            let now = Utc::now().timestamp_millis();
                            if now > (*expiry as i64) {
                                expired = true;
                                response = Response::NULL;
                            }
                        }
                    }

                    if expired {
                        self.kv.0.remove(&key);
                        self.kv.1.remove(&key);
                    }

                  response
                }

                Command::SET(key, value, expiry) => {
                    self.kv.0.insert(key.clone(), value.clone());
                    if let Some(expiry) = expiry {
                        let now = Utc::now().timestamp_millis();
                        let expiry = expiry as i64 + now;
                        info!("Setting expiry for key {} to {}", key, expiry);
                        self.kv.1.insert(key.clone(), expiry as u64);
                    }
                    Response::OK
                }

                _ => {
                    Response::OK
                }
        }
    }
}