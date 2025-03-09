use crate::ClientId;
use crate::ttl::{Tracker, TtlKey};
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::rc::Rc;
use std::sync::LazyLock;
use std::time::Instant;

#[derive(Clone, Copy)]
pub struct Generation(u32);

impl Generation {
    const ZERO: Self = Self(0);

    fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Invalidation(u32);

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

pub struct Entry {
    value: Vec<u8>,

    ttl: Option<TtlKey>,
}

pub struct Cache {
    items: HashMap<Key, Entry, foldhash::fast::RandomState>,
    tracked: HashMap<Invalidation, Vec<ClientId>, foldhash::fast::RandomState>,
    expiries: Tracker,
}

impl Cache {
    fn hash_key(key: &Key) -> u64 {
        static STATE: LazyLock<foldhash::fast::RandomState> =
            LazyLock::new(foldhash::fast::RandomState::default);

        let mut hasher = STATE.build_hasher();
        key.data.hash(&mut hasher);
        hasher.finish()
    }

    fn invalidation(hash: u64) -> Invalidation {
        Invalidation((hash & 0xFFFFFF) as u32)
    }

    pub fn new() -> Self {
        Self {
            items: HashMap::default(),
            expiries: Tracker::new(),
            tracked: HashMap::default(),
        }
    }

    pub fn expired(
        &mut self,
        now: Instant,
    ) -> impl Iterator<Item = (Invalidation, Vec<ClientId>)> + '_ {
        let keys = self.expiries.drain_expired(now);
        let hashes = keys.map(|key| Self::hash_key(&key));
        let invalidations = hashes.map(|hash| Self::invalidation(hash));

        invalidations.map(|invalidation| {
            self.tracked
                .remove_entry(&invalidation)
                .unwrap_or_else(|| (invalidation, vec![]))
        })
    }
}
