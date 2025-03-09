use crate::ClientId;
use crate::ttl::{Tracker, TtlKey};
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Instant;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key {
    data: Rc<Vec<u8>>,
}

impl Key {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: Rc::new(data),
        }
    }
}

pub struct Value {
    value: Vec<u8>,
    ttl: Option<TtlKey>,
}

pub struct Cache {
    items: HashMap<Key, Value>,
    expiries: Tracker,
    tracked: HashMap<Key, Vec<ClientId>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
            expiries: Tracker::new(),
            tracked: HashMap::new(),
        }
    }

    pub fn expired(&mut self, now: Instant) -> impl Iterator<Item = (Key, Vec<ClientId>)> + '_ {
        self.expiries.drain_expired(now).map(|key| {
            let clients = self.tracked.remove(&key).unwrap_or_default();
            (key, clients)
        })
    }
}
