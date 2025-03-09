use crate::ClientId;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Instant;
use std::collections::VecDeque;

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
}

pub struct Cache {
    items: HashMap<Key, Value>,
    expiries: VecDeque<(Key, Instant)>,
    tracked: HashMap<Key, Vec<ClientId>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
            expiries: VecDeque::new(),
            tracked: HashMap::new(),
        }
    }

    fn add_expiry(&mut self, key: Key, expires_at: Instant) {
        let idx = self.search_expiries_by_time(expires_at);
        debug_assert!(self.expiries.iter().find(|(k, _)| k == &key).is_none());
        self.expiries.insert(idx, (key, expires_at));
    }

    fn update_expiry(&mut self, key: &Key, expires_at: Instant) {
        let idx = self.search_expiries_by_key(key).unwrap();
        let (_, time) = &mut self.expiries[idx];
        *time = expires_at;
    }

    fn remove__expiry(&mut self, key: &Key) {
        let idx = self.search_expiries_by_key(key).unwrap();
        self.expiries.remove(idx);
    }

    fn search_expiries_by_key(&mut self, key:&Key) -> Option<usize> {
        self.expiries.iter_mut().position(|(k, _)| k == key)
    }

    fn search_expiries_by_time(&self, needle: Instant) -> usize {
        match self.expiries.binary_search_by_key(&needle,|(_, expires_at)| *expires_at) {
            Ok(idx) => idx,
            Err(idx) => idx,
        }
    }

    pub fn expired(&mut self) -> impl Iterator<Item = (Key, Vec<ClientId>)> + '_ {
        let now  = Instant::now();
        let top = self.search_expiries_by_time(now);

        self.expiries.drain(0..top).map(|(key, _)| {
            let clients = self.tracked.remove(&key).unwrap();
            (key, clients)
        })
    }
}
