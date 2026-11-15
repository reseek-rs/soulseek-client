//! A map with a size bound and an age limit, for what the client remembers on behalf of remote
//! users, such as their addresses and the search results it could not deliver to them. A full map
//! makes room by dropping what has gone longest without being read or written.

use std::collections::HashMap;
use std::hash::Hash;
use std::time::Duration;

use tokio::time::Instant;

struct Entry<V> {
  value: V,
  stored: Instant,
  used: Instant,
}

/// A map whose entries expire `ttl` after they were stored and which holds at most `capacity`
pub(crate) struct BoundedCache<K, V> {
  ttl: Duration,
  capacity: usize,
  entries: HashMap<K, Entry<V>>,
}

impl<K: Eq + Hash + Clone, V> BoundedCache<K, V> {
  pub(crate) fn new(capacity: usize, ttl: Duration) -> Self {
    Self {
      ttl,
      capacity,
      entries: HashMap::new(),
    }
  }

  /// changes how long entries live, counted from when each was stored
  pub(crate) fn set_ttl(&mut self, ttl: Duration) {
    self.ttl = ttl;
  }

  /// changes the bound, dropping the least recently used entries that no longer fit
  pub(crate) fn set_capacity(&mut self, capacity: usize) {
    self.set_capacity_unless_held(capacity, |_| false);
  }

  /// [`BoundedCache::set_capacity`] that drops only entries `held` does not claim, and keeps
  /// more than the bound while they are held
  pub(crate) fn set_capacity_unless_held(&mut self, capacity: usize, held: impl Fn(&K) -> bool) {
    self.capacity = capacity;
    while self.entries.len() > self.capacity && self.evict(&held) {}
  }

  /// the value under `key`, unless it has expired
  pub(crate) fn get(&mut self, key: &K) -> Option<&V> {
    let now = Instant::now();
    if self.expired(key, now) {
      self.entries.remove(key);
      return None;
    }
    let entry = self.entries.get_mut(key)?;
    entry.used = now;
    Some(&entry.value)
  }

  /// the value under `key`, unless it has expired, without counting as a use: what the map
  /// drops to make room stays as it was
  pub(crate) fn peek(&self, key: &K) -> Option<&V> {
    if self.expired(key, Instant::now()) {
      return None;
    }
    self.entries.get(key).map(|entry| &entry.value)
  }

  /// the value under `key` when it was stored at `since` or later and has not expired, without
  /// counting as a use
  pub(crate) fn stored_since(&self, key: &K, since: Instant) -> Option<&V> {
    let entry = self.entries.get(key)?;
    (entry.stored >= since && !self.expired(key, Instant::now())).then_some(&entry.value)
  }

  /// stores `value` under `key`, replacing what was there and making room if the map is full
  pub(crate) fn insert(&mut self, key: K, value: V) {
    self.insert_unless_held(key, value, |_| false);
  }

  /// [`BoundedCache::insert`] that makes room only by dropping the least recently used entry
  /// `held` does not claim, and goes over the bound while every entry is held. One entry makes
  /// room for one, so a map held over its bound comes back down as its entries expire.
  pub(crate) fn insert_unless_held(&mut self, key: K, value: V, held: impl Fn(&K) -> bool) {
    let now = Instant::now();
    if !self.entries.contains_key(&key) {
      self
        .entries
        .retain(|_, entry| now.duration_since(entry.stored) < self.ttl);
      if self.entries.len() >= self.capacity {
        self.evict(&held);
      }
    }
    self.entries.insert(
      key,
      Entry {
        value,
        stored: now,
        used: now,
      },
    );
  }

  /// takes the value under `key` out, unless it has expired
  pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
    let expired = self.expired(key, Instant::now());
    let entry = self.entries.remove(key)?;
    (!expired).then_some(entry.value)
  }

  /// a value whose key `pick` accepts and that has not expired, without counting as a use
  pub(crate) fn find(&self, pick: impl Fn(&K) -> bool) -> Option<&V> {
    let now = Instant::now();
    self
      .entries
      .iter()
      .find(|(key, entry)| pick(key) && now.duration_since(entry.stored) < self.ttl)
      .map(|(_, entry)| &entry.value)
  }

  /// takes out every entry whose key `pick` accepts, with its key; the expired ones are dropped
  pub(crate) fn remove_where(&mut self, pick: impl Fn(&K) -> bool) -> Vec<(K, V)> {
    let now = Instant::now();
    let picked: Vec<K> = self
      .entries
      .keys()
      .filter(|key| pick(key))
      .cloned()
      .collect();
    picked
      .into_iter()
      .filter_map(|key| {
        let entry = self.entries.remove(&key)?;
        (now.duration_since(entry.stored) < self.ttl).then_some((key, entry.value))
      })
      .collect()
  }

  /// the keys of the entries that have not expired
  pub(crate) fn keys(&self) -> impl Iterator<Item = &K> {
    let now = Instant::now();
    self
      .entries
      .iter()
      .filter(move |(_, entry)| now.duration_since(entry.stored) < self.ttl)
      .map(|(key, _)| key)
  }

  /// how many entries have not expired
  pub(crate) fn len(&mut self) -> usize {
    let now = Instant::now();
    self
      .entries
      .retain(|_, entry| now.duration_since(entry.stored) < self.ttl);
    self.entries.len()
  }

  fn expired(&self, key: &K, now: Instant) -> bool {
    self
      .entries
      .get(key)
      .is_some_and(|entry| now.duration_since(entry.stored) >= self.ttl)
  }

  /// drops the least recently used entry `held` does not claim; false when there is none
  fn evict(&mut self, held: impl Fn(&K) -> bool) -> bool {
    let oldest = self
      .entries
      .iter()
      .filter(|(key, _)| !held(key))
      .min_by_key(|(_, entry)| entry.used)
      .map(|(key, _)| key.clone());
    let Some(key) = oldest else {
      return false;
    };
    self.entries.remove(&key);
    true
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test(start_paused = true)]
  async fn a_ttl_too_long_to_add_to_the_clock_keeps_entries() {
    let mut cache = BoundedCache::new(2, Duration::MAX);
    cache.insert("alice", 1);
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert_eq!(cache.get(&"alice"), Some(&1));
    assert_eq!(cache.len(), 1);
  }

  #[tokio::test(start_paused = true)]
  async fn entries_expire_after_the_ttl() {
    let mut cache = BoundedCache::new(10, Duration::from_secs(60));
    cache.insert("alice", 1);
    tokio::time::sleep(Duration::from_secs(59)).await;
    assert_eq!(cache.get(&"alice"), Some(&1));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(cache.get(&"alice"), None);
    cache.insert("bob", 2);
    assert_eq!(cache.len(), 1);
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(cache.len(), 0, "the size counts only what has not expired");
    cache.insert("bob", 2);
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(
      cache.remove(&"bob"),
      None,
      "an expired entry is not handed out"
    );
  }

  #[tokio::test(start_paused = true)]
  async fn a_full_cache_drops_the_least_recently_used() {
    let mut cache = BoundedCache::new(2, Duration::from_secs(60));
    cache.insert("alice", 1);
    tokio::time::sleep(Duration::from_secs(1)).await;
    cache.insert("bob", 2);
    tokio::time::sleep(Duration::from_secs(1)).await;
    // reading alice makes bob the least recently used
    cache.get(&"alice");
    cache.insert("carol", 3);
    assert_eq!(cache.get(&"bob"), None);
    assert_eq!(cache.get(&"alice"), Some(&1));
    assert_eq!(cache.get(&"carol"), Some(&3));
    // replacing an entry needs no room
    cache.insert("carol", 4);
    assert_eq!(cache.get(&"alice"), Some(&1));
    assert_eq!(cache.remove(&"carol"), Some(4));
  }

  #[tokio::test(start_paused = true)]
  async fn a_peek_is_not_a_use() {
    let mut cache = BoundedCache::new(2, Duration::from_secs(60));
    cache.insert("alice", 1);
    tokio::time::sleep(Duration::from_secs(1)).await;
    cache.insert("bob", 2);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(cache.peek(&"alice"), Some(&1));
    cache.insert("carol", 3);
    assert_eq!(
      cache.peek(&"alice"),
      None,
      "alice was still the least recently used"
    );
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(
      cache.peek(&"bob"),
      None,
      "an expired entry is not handed out"
    );
  }

  #[tokio::test(start_paused = true)]
  async fn expired_entries_make_room_before_live_ones_are_dropped() {
    let mut cache = BoundedCache::new(2, Duration::from_secs(10));
    cache.insert("alice", 1);
    tokio::time::sleep(Duration::from_secs(5)).await;
    cache.insert("bob", 2);
    cache.get(&"alice");
    tokio::time::sleep(Duration::from_secs(6)).await;
    // alice has expired though she was read last; bob stays
    cache.insert("carol", 3);
    assert_eq!(cache.get(&"bob"), Some(&2));
    assert_eq!(cache.get(&"carol"), Some(&3));
  }

  #[tokio::test(start_paused = true)]
  async fn a_held_entry_is_never_dropped_to_make_room() {
    let mut cache = BoundedCache::new(2, Duration::from_secs(60));
    cache.insert("alice", 1);
    tokio::time::sleep(Duration::from_secs(1)).await;
    cache.insert("bob", 2);
    let held = |key: &&str| *key == "alice" || *key == "carol";
    cache.insert_unless_held("carol", 3, held);
    assert_eq!(cache.get(&"bob"), None, "the oldest not held made room");
    tokio::time::sleep(Duration::from_secs(1)).await;
    cache.insert_unless_held("dave", 4, |_| true);
    assert_eq!(cache.len(), 3, "over the bound while every entry is held");
    cache.insert_unless_held("erin", 5, |key| *key == "alice");
    assert_eq!(cache.len(), 3, "one entry made room for one");
    assert_eq!(
      cache.peek(&"carol"),
      None,
      "the least recently used not held"
    );
    assert_eq!(cache.peek(&"dave"), Some(&4));
    cache.set_capacity_unless_held(1, |key| *key != "dave");
    assert_eq!(cache.peek(&"dave"), None);
    assert_eq!(cache.len(), 2);
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(cache.len(), 0, "held entries still expire");
  }

  #[test]
  fn shrinking_drops_what_no_longer_fits() {
    let mut cache = BoundedCache::new(3, Duration::from_secs(60));
    for (key, value) in [("a", 1), ("b", 2), ("c", 3)] {
      cache.insert(key, value);
    }
    cache.set_capacity(1);
    assert_eq!(cache.entries.len(), 1);
  }
}
