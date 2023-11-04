use fasthash::{FarmHasher, FastHasher};
use std::hash::Hasher;

pub fn compute_hash(key: &str) -> u64 {
    let mut hasher = FarmHasher::new();
    hasher.write(key.as_bytes());
    hasher.finish()
}
