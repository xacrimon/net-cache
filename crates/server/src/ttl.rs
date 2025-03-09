use crate::cache::Key;
use slab::Slab;
use std::iter;
use std::sync::LazyLock;
use std::{collections::BTreeMap, num::NonZeroUsize, time::Instant};

static EPOCH: LazyLock<Instant> = LazyLock::new(|| Instant::now());

fn millis_since_epoch(time: Instant) -> u64 {
    time.duration_since(*EPOCH).as_millis() as u64
}

struct Entry {
    key: Key,
    expires_at: Instant,
}

#[repr(transparent)]
pub struct TtlKey(NonZeroUsize);

impl TtlKey {
    fn from_slab_key(key: usize) -> Self {
        Self(NonZeroUsize::new(key + 1).unwrap())
    }

    fn to_slab_key(self) -> usize {
        self.0.get() - 1
    }
}

pub struct Tracker {
    entries: Slab<Entry>,
    by_time: BTreeMap<u64, Vec<usize>>,
    key_list_pool: Vec<Vec<usize>>,
}

impl Tracker {
    pub fn new() -> Self {
        Self {
            entries: Slab::new(),
            by_time: BTreeMap::new(),
            key_list_pool: Vec::new(),
        }
    }

    fn group_mut(&mut self, group: u64) -> &mut Vec<usize> {
        self.by_time.entry(group).or_insert_with(|| {
            let mut list = self.key_list_pool.pop().unwrap_or_default();
            list.clear();
            list
        })
    }

    pub fn add(&mut self, key: Key, expires_at: Instant) -> TtlKey {
        let group = millis_since_epoch(expires_at);
        let entry = Entry { key, expires_at };
        let key = self.entries.insert(entry);
        self.group_mut(group).push(key);
        TtlKey::from_slab_key(key)
    }

    pub fn update(&mut self, key: TtlKey, expires_at: Instant) {
        let key = key.to_slab_key();
        let entry = self.entries.get_mut(key).unwrap();

        let current_group = millis_since_epoch(entry.expires_at);
        let new_group = millis_since_epoch(expires_at);

        entry.expires_at = expires_at;

        self.group_mut(current_group).swap_remove(key);
        self.group_mut(new_group).push(key);
    }

    pub fn remove(&mut self, key: TtlKey) {
        let key = key.to_slab_key();
        let entry = self.entries.remove(key);
        let group = millis_since_epoch(entry.expires_at);
        self.group_mut(group).swap_remove(key);
    }

    pub fn drain_expired(&mut self, now: Instant) -> impl Iterator<Item = Key> + '_ {
        let cutoff = millis_since_epoch(now);
        let prev_top = self.key_list_pool.len();

        while let Some(entry) = self.by_time.first_entry() {
            if *entry.key() > cutoff {
                break;
            }

            let keys = entry.remove();
            self.key_list_pool.push(keys);
        }

        self.key_list_pool[prev_top..]
            .iter()
            .flatten()
            .map(|&key| self.entries.remove(key).key)
    }
}
