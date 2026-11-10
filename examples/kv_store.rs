//! An in-memory key-value store split across shards by key.
//!
//! Every key belongs to exactly one shard, chosen by hashing it. That
//! shard's map is the only place the key ever lives, so the maps need no
//! locks. A request from any shard is sent to the owner.
//!
//! Run with `cargo run --example kv_store`.

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use starfish::{shard_count, Runtime, Sharded};

#[derive(Default)]
struct Partition {
    map: RefCell<HashMap<String, String>>,
}

#[derive(Clone, Copy)]
struct Store {
    partitions: Sharded<Partition>,
    shards: usize,
}

impl Store {
    async fn start() -> starfish::Result<Store> {
        Ok(Store {
            partitions: Sharded::start(|_shard| Partition::default()).await?,
            shards: shard_count()?,
        })
    }

    fn owner(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % self.shards as u64) as usize
    }

    async fn put(&self, key: String, value: String) -> starfish::Result<Option<String>> {
        self.partitions
            .invoke_on(self.owner(&key), move |partition| async move {
                partition.map.borrow_mut().insert(key, value)
            })
            .await
    }

    async fn get(&self, key: String) -> starfish::Result<Option<String>> {
        self.partitions
            .invoke_on(self.owner(&key), move |partition| async move {
                partition.map.borrow().get(&key).cloned()
            })
            .await
    }

    /// How many keys each shard holds.
    async fn sizes(&self) -> starfish::Result<Vec<usize>> {
        self.partitions
            .invoke_on_all(|partition| async move { partition.map.borrow().len() })
            .await
    }
}

async fn app() -> starfish::Result<()> {
    let store = Store::start().await?;

    for n in 0..1000 {
        store.put(format!("user:{n}"), format!("name-{n}")).await?;
    }
    let previous = store.put(String::from("user:7"), String::from("renamed")).await?;
    println!("user:7 was {previous:?}");
    println!("user:7 is {:?}", store.get(String::from("user:7")).await?);
    println!("user:x is {:?}", store.get(String::from("user:x")).await?);

    let sizes = store.sizes().await?;
    println!("{} keys over {} shards: {sizes:?}", sizes.iter().sum::<usize>(), sizes.len());
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().run(app)?
}
