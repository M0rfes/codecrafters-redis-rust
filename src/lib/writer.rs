use std::{rc::Rc, sync::Arc};

use chrono::Utc;
use dashmap::DashMap;
use tracing::info;

use crate::{
    command::{Command, Response}, kv_manager::KvManager,
};

pub struct Writer {
    kv: Arc<KvManager>,
}

impl Writer {
    pub fn new(kv: Arc<KvManager>) -> Self {
        Self { kv }
    }

    pub async fn process(&mut self, command: Command) -> Response {
        match command {
            Command::GET(key) => {
                if let Some(value) = self.kv.get(&key) {
                    Response::GET(value)
                } else {
                    Response::NULL
                }
            }

            Command::SET(key, value, expiry) => {
                self.kv.set(key, value, expiry);
                Response::OK
            }

            Command::RPUSH(key, value) => {
                let len = self.kv.rpush(&key, value).await;
                Response::LEN(len)
            }

            Command::PING => Response::PONG,
            Command::ECHO(s) => Response::ECHO(s),
            Command::LRANGE(key, start, stop) => {
                let list = self.kv.lrange(&key, start, stop).await;
                Response::LIST(list)
            }
            Command::LPUSH(key, value) => {
                let len = self.kv.lpush(&key, value).await;
                Response::LEN(len)
            }
            Command::LLEN(key) => {
                let len = self.kv.llen(&key).await;
                Response::LEN(len)
            }
            Command::LPOP(key) => {
                let value = self.kv.lpop(&key).await;
                if let Some(value) = value {
                    Response::GET(value)
                } else {
                    Response::NULL
                }
            }
            Command::LPOP_WITH_COUNT(key, count) => {
                let values = self.kv.lpop_with_count(&key, count).await;
                if let Some(values) = values {
                    Response::LIST(values)
                } else {
                    Response::NULL
                }
            }
            _ => Response::OK,
        }
    }
}
