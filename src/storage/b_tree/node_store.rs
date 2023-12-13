use crate::util::delete_file_if_exists::delete_file_if_exists;

use super::metrics::BTreeMetrics;
use super::node::{BTreeNode, NodeId};
use async_trait::async_trait;
use dashmap::DashMap;
use lazy_static::lazy_static;
use prometheus::core::Atomic;
use prometheus::{
    exponential_buckets, register_histogram, register_int_gauge, Histogram, HistogramOpts,
    IntCounter, IntGauge, Opts, Registry,
};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::{
    fmt::{Debug, Display},
    io::SeekFrom,
    sync::{atomic::AtomicUsize, Arc},
    time::Instant,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
    sync::{Mutex, RwLock, RwLockWriteGuard},
};

#[derive(Clone)]
pub enum NodeStore<K, V, S>
where
    K: Ord + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Display + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    Memory(MemoryNodeStore<K, V>),
    Disk(DiskNodeStore<K, V, S>),
}

impl<K, V, S> NodeStore<K, V, S>
where
    K: Ord + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Display + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    pub fn new_memory() -> Self {
        NodeStore::Memory(MemoryNodeStore::new())
    }
    pub async fn new_disk(
        path: &str,
        serializer: S,
        key_size: usize,
        value_size: usize,
        flush_every: usize,
        flush_buffer_max_size: usize,
        metrics: BTreeMetrics,
    ) -> Self {
        NodeStore::Disk(
            DiskNodeStore::open(
                path,
                serializer,
                key_size,
                value_size,
                flush_every,
                flush_buffer_max_size,
                metrics,
            )
            .await,
        )
    }

    pub async fn get(&self, id: NodeId) -> Option<Arc<RwLock<BTreeNode<K, V>>>> {
        match self {
            NodeStore::Memory(store) => store.get(id),
            NodeStore::Disk(store) => store.get(id).await,
        }
    }

    pub async fn insert(&self, node: BTreeNode<K, V>) {
        match self {
            NodeStore::Memory(store) => store.insert(node),
            NodeStore::Disk(store) => store.insert(node).await,
        }
    }

    pub async fn remove<'a>(&self, node: RwLockWriteGuard<'a, BTreeNode<K, V>>) {
        match self {
            NodeStore::Memory(store) => store.remove(node),
            NodeStore::Disk(store) => store.remove(node).await,
        };
    }

    pub async fn persist<'a>(&self, node: RwLockWriteGuard<'a, BTreeNode<K, V>>) {
        match self {
            NodeStore::Memory(store) => store.persist(node),
            NodeStore::Disk(store) => store.persist(node).await,
        }
    }

    pub async fn flush(&self) {
        match self {
            NodeStore::Memory(store) => store.flush(),
            NodeStore::Disk(store) => store.flush().await,
        }
    }

    // pub async fn flush_required(&self) -> bool {
    //     match self {
    //         NodeStore::Memory(store) => store.flush_required(),
    //         NodeStore::Disk(store) => store.flush_required().await,
    //     }
    // }

    pub async fn flush_buffer_size(&self) -> usize {
        match self {
            NodeStore::Memory(store) => store.flush_buffer_size(),
            NodeStore::Disk(store) => store.flush_buffer_size().await,
        }
    }

    pub async fn flush_buffer_full(&self) -> bool {
        match self {
            NodeStore::Memory(store) => store.flush_buffer_full(),
            NodeStore::Disk(store) => store.flush_buffer_full().await,
        }
    }

    pub fn clear_cache(&self) {
        match self {
            NodeStore::Memory(store) => store.clear_cache(),
            NodeStore::Disk(store) => store.clear_cache(),
        }
    }
}

type GuardedNode<K, V> = RwLock<BTreeNode<K, V>>;

#[derive(Clone)]
pub struct MemoryNodeStore<K, V>
where
    K: Ord + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Display + Send + Sync,
{
    nodes: Arc<DashMap<NodeId, Arc<GuardedNode<K, V>>>>,
}

impl<K, V> PartialEq for MemoryNodeStore<K, V>
where
    K: Ord + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Display + Send + Sync,
{
    fn eq(&self, other: &Self) -> bool {
        let mut a: Vec<_> = self.nodes.iter().map(|node| *node.key()).collect();
        a.sort();
        let mut b: Vec<_> = other.nodes.iter().map(|node| *node.key()).collect();
        b.sort();
        a == b
    }
}

impl<K, V> MemoryNodeStore<K, V>
where
    K: Ord + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Display + Send + Sync,
{
    fn new() -> Self {
        MemoryNodeStore {
            nodes: Arc::new(DashMap::new()),
        }
    }

    fn get(&self, id: NodeId) -> Option<Arc<RwLock<BTreeNode<K, V>>>> {
        self.nodes.get(&id).map(|node| node.value().clone())
    }

    fn insert(&self, node: BTreeNode<K, V>) {
        // There should never be multiple workers attempting to insert the same node
        // because the node_id allocation is atomic.
        self.nodes
            .entry(node.node_id)
            .and_modify(|_| panic!("Btree node inserted multiple times into memory store"))
            .or_insert(Arc::new(RwLock::new(node.clone())));
    }

    fn persist<'a>(&self, node: RwLockWriteGuard<'a, BTreeNode<K, V>>) {
        // No persistence for memory store
    }

    fn flush(&self) {
        // No persistence for memory store
    }

    fn flush_required(&self) -> bool {
        false
    }

    fn flush_buffer_size(&self) -> usize {
        0
    }

    fn flush_buffer_full(&self) -> bool {
        false
    }

    fn remove<'a>(&self, node: RwLockWriteGuard<'a, BTreeNode<K, V>>) {
        self.nodes.remove(&node.node_id);
    }

    fn clear_cache(&self) {}
}

const MAX_NODE_SIZE_BYTES: u32 = 4096;
const NODE_INDEX_BYTES: u32 = MAX_NODE_SIZE_BYTES;

pub static PAGE_LOAD_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static PAGE_LOAD_MICROSECONDS_DURATION_TOTAL: AtomicUsize = AtomicUsize::new(0);

pub static NODE_GET_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static NODE_GET_MICROSECONDS_DURATION_TOTAL: AtomicUsize = AtomicUsize::new(0);

pub static NODE_INSERT_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static NODE_INSERT_MICROSECONDS_DURATION_TOTAL: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct DiskNodeStore<K, V, S>
where
    K: Ord + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Display + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    reader: Arc<RwLock<File>>,
    writer: Arc<RwLock<File>>,
    //persist_writer: Arc<RwLock<File>>,
    nodes: Arc<DashMap<NodeId, Arc<GuardedNode<K, V>>>>,
    serializer: S,
    key_size: usize,
    value_size: usize,
    flush_every: usize,
    flush_buffer_max_size: usize,
    start: Instant,
    last_flush: Arc<AtomicU64>,
    persist_node_ids: Arc<Mutex<HashSet<NodeId>>>, //node_buffer: Arc<RwLock<HashMap<NodeId, BTreeNode<K, V>>>>,
    //persist_nodes: Arc<Mutex<Vec<BTreeNode<K, V>>>>,
    metrics: BTreeMetrics,
}

impl<K, V, S> DiskNodeStore<K, V, S>
where
    K: Ord + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Display + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    async fn open(
        path: &str,
        serializer: S,
        key_size: usize,
        value_size: usize,
        flush_every: usize,
        flush_buffer_max_size: usize,
        metrics: BTreeMetrics,
    ) -> Self {
        let writer = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)
            .await
            .unwrap();

        // delete_file_if_exists("scratch/persist");
        // let persist_writer = OpenOptions::new()
        //     .write(true)
        //     .read(true)
        //     .create(true)
        //     .open("scratch/persist")
        //     .await
        //     .unwrap();

        let reader = OpenOptions::new().read(true).open(path).await.unwrap();

        let persist_node_ids = Arc::new(Mutex::new(HashSet::new()));

        DiskNodeStore {
            reader: Arc::new(RwLock::new(reader)),
            writer: Arc::new(RwLock::new(writer)),
            nodes: Arc::new(DashMap::new()),
            //persist_writer: Arc::new(RwLock::new(persist_writer)),
            serializer,
            key_size,
            value_size,
            start: Instant::now(),
            last_flush: Arc::new(AtomicU64::new(0)),
            flush_every,
            flush_buffer_max_size,
            persist_node_ids,
            metrics,
        }
    }
    async fn get(&self, node_id: NodeId) -> Option<Arc<RwLock<BTreeNode<K, V>>>> {
        // Try and get the node entry from the cache
        if let Some(node) = self.nodes.get(&node_id).map(|node| node.value().clone()) {
            // If it's there, return the node. The caller can take a read or write lock
            // on the node using the Arc<RwLock<_>> it's wrapped with.
            return Some(node);
        }

        let _start_page_load = Instant::now();

        let mut file = self.reader.write().await;

        // It looks like we are going to be responsible for loading the node
        // from disk, putting it in the cache and returning the node to the caller.
        let mut node = BTreeNode {
            node_id,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
        };

        let node_offset = (NODE_INDEX_BYTES + node_id * MAX_NODE_SIZE_BYTES) as u64;

        // Is the node in the file?
        file.seek(SeekFrom::End(0)).await.unwrap();

        let max_offset = file.stream_position().await.unwrap();
        if max_offset < node_offset {
            return None;
        }

        // Seek to the node
        file.seek(SeekFrom::Start(node_offset)).await.unwrap();

        let mut reader = BufReader::new(&mut *file);

        // Read the node ID
        node.node_id = reader.read_u32().await.unwrap();

        // Read the keys
        let num_keys = reader.read_u16().await.unwrap();
        for _ in 0..num_keys {
            let (key, len) = self.serializer.deserialize_key(&mut reader).await;
            reader
                .seek(SeekFrom::Current(self.key_size as i64 - len as i64))
                .await
                .unwrap();
            node.keys.push(key);
        }

        // Read the values
        let num_values = reader.read_u16().await.unwrap();
        for _ in 0..num_values {
            let (value, len) = self.serializer.deserialize_value(&mut reader).await;
            reader
                .seek(SeekFrom::Current(self.value_size as i64 - len as i64))
                .await
                .unwrap();
            node.values.push(value);
        }

        // Read the children
        let num_children = reader.read_u16().await.unwrap();
        for _ in 0..num_children {
            node.children.push(reader.read_u32().await.unwrap());
        }

        let node = Arc::new(RwLock::new(node));

        self.nodes.entry(node_id).or_insert(node.clone());

        Some(node)
    }

    async fn insert(&self, node: BTreeNode<K, V>) {
        let node_id = node.node_id;
        self.nodes
            .insert(node_id, Arc::new(RwLock::new(node.clone())));
        self.persist_node_id(node_id, node).await;
    }

    async fn persist<'a>(&self, node: RwLockWriteGuard<'a, BTreeNode<K, V>>) {
        let node_id = node.node_id;
        //drop(node); // Ensure we release the lock before persisting

        self.persist_node_id(node_id, node.clone()).await;
    }

    async fn persist_node_id<'a>(&self, node_id: NodeId, node: BTreeNode<K, V>) {
        let mut persist_node_ids_guard = self.persist_node_ids.lock().await;
        persist_node_ids_guard.insert(node_id);

        self.metrics
            .buffer_size_gauge
            .set(persist_node_ids_guard.len() as i64);
        drop(persist_node_ids_guard);

        // self.persist_writer
        //     .write()
        //     .await
        //     .write_all(&bincode::serialize(&node).unwrap());
        //self.persist_nodes.lock().await.push(node);

        // TEMP
        // let node_offset = (NODE_INDEX_BYTES + node.node_id * MAX_NODE_SIZE_BYTES) as u64;

        // let mut buffer = Vec::new();

        // // Write the node ID
        // buffer.write_u32(node.node_id).await.unwrap();
        // // Write the keys
        // buffer.write_u16(node.keys.len() as u16).await.unwrap();
        // for key in &node.keys {
        //     let len = self.serializer.serialize_key(key, &mut buffer).await;
        //     if len > self.key_size {
        //         panic!("Key size {} is larger than key_size {}", len, self.key_size);
        //     }
        //     let buf = vec![0u8; self.key_size - len];
        //     buffer.write_all(&buf).await.unwrap();
        // }

        // // Write the values
        // buffer.write_u16(node.values.len() as u16).await.unwrap();
        // for value in &node.values {
        //     let len = self.serializer.serialize_value(value, &mut buffer).await;
        //     if len > self.value_size {
        //         panic!(
        //             "Value size {} is larger than value_size {}",
        //             len, self.value_size
        //         );
        //     }
        //     let buf = vec![0u8; self.value_size - len];
        //     buffer.write_all(&buf).await.unwrap();
        // }

        // // Write the children
        // buffer.write_u16(node.children.len() as u16).await.unwrap();
        // for child in &node.children {
        //     buffer.write_u32(*child).await.unwrap();
        // }

        // let mut file = self.persist_writer.write().await;
        // file.write_u64(node_offset).await.unwrap();
        // file.write_u32(buffer.len() as u32).await.unwrap();
        // file.write_all(&buffer).await.unwrap();
        // file.flush().await.unwrap();
        // TEMP
    }

    pub async fn flush(&self) {
        let mut persist_node_ids_guard: tokio::sync::MutexGuard<'_, HashSet<u32>> =
            self.persist_node_ids.lock().await;

        let persist_node_ids = std::mem::take(&mut *persist_node_ids_guard);
        drop(persist_node_ids_guard);

        if persist_node_ids.is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
            return;
        }

        let start = Instant::now();
        let mut persist_node_ids = persist_node_ids.into_iter().collect::<Vec<_>>();
        persist_node_ids.sort();
        self.metrics
            .flush_sort_duration_histogram
            .observe(start.elapsed().as_secs_f64());

        let start_get = Instant::now();
        let persist_nodes = persist_node_ids
            .iter()
            .filter_map(|node_id| self.nodes.get(node_id).map(|node| node.value().clone()))
            .collect::<Vec<_>>();
        self.metrics
            .flush_get_duration_histogram
            .observe(start_get.elapsed().as_secs_f64());

        self.write_nodes(persist_nodes).await;
        let elapsed = (Instant::now() - self.start).as_millis() as u64;
        self.last_flush.store(elapsed, Ordering::Relaxed);
        self.metrics
            .flush_duration_histogram
            .observe(start.elapsed().as_secs_f64());
    }

    // pub async fn flush_required(&self) -> bool {
    //     let persist_node_ids_guard = self.persist_node_ids.lock().await;
    //     let elapsed = (Instant::now() - self.start).as_millis() as u64
    //         - self.last_flush.load(Ordering::Relaxed);
    //     persist_node_ids_guard.len() >= self.flush_every || elapsed > 5
    // }

    async fn flush_buffer_size(&self) -> usize {
        let persist_node_ids_guard = self.persist_node_ids.lock().await;
        persist_node_ids_guard.len()
    }

    pub async fn flush_buffer_full(&self) -> bool {
        let persist_node_ids_guard = self.persist_node_ids.lock().await;
        persist_node_ids_guard.len() >= self.flush_buffer_max_size
    }

    async fn write_nodes(&self, nodes: Vec<Arc<RwLock<BTreeNode<K, V>>>>) {
        let mut file = self.writer.write().await;
        let start_position = file.stream_position().await.unwrap();
        let mut position = start_position;

        let mut written_bytes = 0;
        let mut writer = BufWriter::new(&mut *file);

        for node in nodes {
            self.write_node(
                node,
                &mut writer,
                &self.serializer,
                self.key_size,
                self.value_size,
                &mut position,
                &mut written_bytes,
            )
            .await;
        }

        writer.flush().await.unwrap();

        self.metrics
            .flush_size_histogram
            .observe(written_bytes as f64);

        self.metrics.write_bytes_counter.inc_by(written_bytes);
    }

    async fn write_node(
        &self,
        node: Arc<RwLock<BTreeNode<K, V>>>,
        writer: &mut BufWriter<&mut File>,
        serializer: &S,
        key_size: usize,
        value_size: usize,
        position: &mut u64,
        written_bytes: &mut u64,
    ) {
        let node = node.read().await;

        let start = Instant::now();
        let node_offset = (NODE_INDEX_BYTES + node.node_id * MAX_NODE_SIZE_BYTES) as u64;

        // Seek to the node
        if *position != node_offset {
            writer.flush().await.unwrap();
            writer.seek(SeekFrom::Start(node_offset)).await.unwrap();
            self.metrics
                .seek_size_histogram
                .observe(node_offset as f64 - *position as f64);
            *position = node_offset;
        }

        // Write the node ID
        let mut size = 0;
        writer.write_u32(node.node_id).await.unwrap();
        size += 4;

        // Write the keys
        writer.write_u16(node.keys.len() as u16).await.unwrap();
        size += 2;
        for key in &node.keys {
            let len = serializer.serialize_key(key, writer).await;
            if len > key_size {
                panic!("Key size {} is larger than key_size {}", len, key_size);
            }
            let buf = vec![0u8; key_size - len];
            writer.write_all(&buf).await.unwrap();
            size += key_size as u64;
        }

        // Write the values
        writer.write_u16(node.values.len() as u16).await.unwrap();
        size += 2;
        for value in &node.values {
            let len = serializer.serialize_value(value, writer).await;
            if len > value_size {
                panic!(
                    "Value size {} is larger than value_size {}",
                    len, value_size
                );
            }
            let buf = vec![0u8; value_size - len];
            writer.write_all(&buf).await.unwrap();
            size += value_size as u64;
        }

        // Write the children
        writer.write_u16(node.children.len() as u16).await.unwrap();
        size += 2;
        for child in &node.children {
            writer.write_u32(*child).await.unwrap();
            size += 4;
        }

        *position += size;
        *written_bytes += size;

        // let fill = vec![0u8; MAX_NODE_SIZE_BYTES as usize - size as usize];
        // writer.write_all(&fill).await.unwrap();
        // *position += fill.len() as u64;

        drop(node);

        //println!("Write node in {} microseconds", duration_micros);
        self.metrics
            .writes_histogram
            .observe(start.elapsed().as_secs_f64());
    }

    async fn remove<'a>(&self, node: RwLockWriteGuard<'a, BTreeNode<K, V>>) {
        self.nodes.remove(&node.node_id);
        drop(node);
    }

    fn clear_cache(&self) {
        self.nodes.clear();
    }
}

pub fn calculate_max_keys_per_node(key_size: usize, value_size: usize) -> usize {
    const NODE_ID_SIZE: u32 = 4; // size of node_id (u32)
    const NUM_KEYS_SIZE: u32 = 2; // size of num_keys (u16)
    const NUM_VALUES_SIZE: u32 = 2; // size of num_values (u16)
    const NUM_CHILDREN_SIZE: u32 = 2; // size of num_children (u16)
    const CHILD_SIZE: u32 = 4; // size of each child reference (u32)

    // Total size available (4KB page)
    const TOTAL_SIZE: u32 = MAX_NODE_SIZE_BYTES;

    // Fixed size taken by the node
    let fixed_size = NODE_ID_SIZE + NUM_KEYS_SIZE + NUM_VALUES_SIZE + NUM_CHILDREN_SIZE;

    // Calculate the maximum number of keys
    // Equation: fixed_size + (KEY_SIZE + VALUE_SIZE) * K + CHILD_SIZE * (K + 1) <= TOTAL_SIZE
    let max_keys =
        (TOTAL_SIZE - fixed_size - CHILD_SIZE) / (key_size as u32 + value_size as u32 + CHILD_SIZE);

    max_keys as usize
}

#[async_trait]
pub trait SerializeNode<K, V>
where
    K: Ord + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Display + Send + Sync,
{
    async fn serialize_key<W: AsyncWrite + Unpin + Send>(&self, node: &K, writer: &mut W) -> usize;
    async fn serialize_value<W: AsyncWrite + Unpin + Send>(
        &self,
        node: &V,
        writer: &mut W,
    ) -> usize;
}

#[async_trait]
pub trait DeserializeNode<K, V>
where
    K: Ord + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Display + Send + Sync,
{
    async fn deserialize_key<R: AsyncRead + Unpin + Send>(&self, reader: &mut R) -> (K, usize);
    async fn deserialize_value<R: AsyncRead + Unpin + Send>(&self, reader: &mut R) -> (V, usize);
}

#[derive(Clone)]
pub struct StringNodeSerializer {}

impl StringNodeSerializer {
    pub fn new() -> Self {
        StringNodeSerializer {}
    }
}

#[async_trait]
impl SerializeNode<String, String> for StringNodeSerializer {
    async fn serialize_key<W: AsyncWrite + Unpin + Send>(
        &self,
        key: &String,
        writer: &mut W,
    ) -> usize {
        let bytes = key.as_bytes();
        let len = bytes.len();
        writer.write_u32(len.try_into().unwrap()).await.unwrap();
        writer.write_all(bytes).await.unwrap();
        len + 4
    }
    async fn serialize_value<W: AsyncWrite + Unpin + Send>(
        &self,
        value: &String,
        writer: &mut W,
    ) -> usize {
        let bytes = value.as_bytes();
        let len = bytes.len();
        writer.write_u32(len.try_into().unwrap()).await.unwrap();
        writer.write_all(bytes).await.unwrap();
        len + 4
    }
}

#[async_trait]
impl DeserializeNode<String, String> for StringNodeSerializer {
    async fn deserialize_key<R: AsyncRead + Unpin + Send>(
        &self,
        reader: &mut R,
    ) -> (String, usize) {
        let len = reader.read_u32().await.unwrap() as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await.unwrap();
        (String::from_utf8(buf).unwrap(), len + 4)
    }
    async fn deserialize_value<R: AsyncRead + Unpin + Send>(
        &self,
        reader: &mut R,
    ) -> (String, usize) {
        let len = reader.read_u32().await.unwrap() as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await.unwrap();
        (String::from_utf8(buf).unwrap(), len + 4)
    }
}

#[derive(Clone)]
pub struct I32NodeSerializer {}

impl I32NodeSerializer {
    pub fn new() -> Self {
        I32NodeSerializer {}
    }
}

#[async_trait]
impl SerializeNode<i32, i32> for I32NodeSerializer {
    async fn serialize_key<W: AsyncWrite + Unpin + Send>(
        &self,
        key: &i32,
        writer: &mut W,
    ) -> usize {
        writer.write_i32(*key).await.unwrap();
        4
    }
    async fn serialize_value<W: AsyncWrite + Unpin + Send>(
        &self,
        value: &i32,
        writer: &mut W,
    ) -> usize {
        writer.write_i32(*value).await.unwrap();
        4
    }
}

#[async_trait]
impl DeserializeNode<i32, i32> for I32NodeSerializer {
    async fn deserialize_key<R: AsyncRead + Unpin + Send>(&self, reader: &mut R) -> (i32, usize) {
        (reader.read_i32().await.unwrap(), 4)
    }
    async fn deserialize_value<R: AsyncRead + Unpin + Send>(&self, reader: &mut R) -> (i32, usize) {
        (reader.read_i32().await.unwrap(), 4)
    }
}
