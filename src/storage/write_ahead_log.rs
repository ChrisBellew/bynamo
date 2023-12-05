use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets, register_histogram, Gauge, Histogram, HistogramOpts, Registry,
};
use prometheus::{IntGauge, Opts};
use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;
use std::{path::Path, sync::Arc};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
    sync::Mutex,
};

// lazy_static! {
//     static ref WAL_SIZE_GAUGE: IntGauge =
//         register_int_gauge!("wal_size", "Size of write ahead log in bytes").unwrap();
//     static ref WAL_WRITE_HISTOGRAM: Histogram = register_histogram!(
//         "wal_writes_histogram",
//         "Write ahead log write durations in microseconds",
//         exponential_buckets(20.0, 3.0, 15).unwrap()
//     )
//     .unwrap();
// }
#[derive(Clone)]
pub struct WriteAheadLog {
    writer: Arc<Mutex<BufWriter<File>>>,
    //index: Arc<Mutex<WriteAheadLogIndex>>,
    wal_size_gauge: IntGauge,
    wal_write_histogram: Histogram,
    wal_buffer_lock_wait_histogram: Histogram,
    wal_buffer_flush_histogram: Histogram,
    offset: Arc<AtomicU64>,
    position: Arc<AtomicU64>,
    buffer: Arc<Mutex<Vec<u8>>>,
    start: Instant,
    last_flush: Arc<AtomicU64>,
}

impl WriteAheadLog {
    pub async fn create(
        log_path: &str,
        index_path: &str,
        registry: &Registry,
    ) -> Result<Self, CreateError> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .await?;
        let wal_size = file.metadata().await?.len();
        let (max_position, offset) = if wal_size > 0 {
            file.seek(std::io::SeekFrom::End(-4)).await?;
            let last_item_length = file.read_u32().await?;
            file.seek(std::io::SeekFrom::End(-(last_item_length as i64 + 4)))
                .await?;
            (
                file.read_u64().await?,
                file.seek(std::io::SeekFrom::End(0)).await?,
            )
        } else {
            (0, 0)
        };

        //let writer = BufWriter::new(file);
        //let index = WriteAheadLogIndex::create_or_open(index_path).await?;
        //writer.seek(std::io::SeekFrom::Start(index.offset)).await?;

        let wal_size_gauge =
            IntGauge::with_opts(Opts::new("wal_size", "Size of write ahead log in bytes")).unwrap();
        registry.register(Box::new(wal_size_gauge.clone())).unwrap();

        let wal_write_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "wal_writes_histogram",
                "Write ahead log write durations in microseconds",
            )
            .buckets(exponential_buckets(20.0, 3.0, 15).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(wal_write_histogram.clone()))
            .unwrap();

        let wal_buffer_lock_wait_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "wal_buffer_lock_wait_histogram",
                "Write ahead log buffer lock wait durations in microseconds",
            )
            .buckets(exponential_buckets(20.0, 3.0, 15).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(wal_buffer_lock_wait_histogram.clone()))
            .unwrap();

        let wal_buffer_flush_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "wal_buffer_flush_histogram",
                "Write ahead log buffer flush durations in microseconds",
            )
            .buckets(exponential_buckets(20.0, 3.0, 15).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(wal_buffer_flush_histogram.clone()))
            .unwrap();

        Ok(Self {
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
            //index: Arc::new(Mutex::new(index)),
            wal_size_gauge,
            wal_write_histogram,
            wal_buffer_lock_wait_histogram,
            wal_buffer_flush_histogram,
            offset: Arc::new(AtomicU64::new(offset)),
            position: Arc::new(AtomicU64::new(max_position)),
            buffer: Arc::new(Mutex::new(Vec::new())),
            start: Instant::now(),
            last_flush: Arc::new(0.into()),
        })
    }
    pub async fn write_new(&mut self, key: &str, value: &str) -> Result<u64, WriteError> {
        let position = self.position.fetch_add(1, Ordering::Relaxed);
        self.write(position, key, value).await?;
        Ok(position)
    }
    pub async fn write(&mut self, position: u64, key: &str, value: &str) -> Result<(), WriteError> {
        let start = Instant::now();
        let mut len = 0u32;
        let mut writer = self.writer.lock().await;
        let lock_wait = start.elapsed().as_micros() as f64;
        writer.write_u64(position).await?;
        len += 8;
        writer.write_u16(key.len() as u16).await?;
        len += 2;
        writer.write_all(key.as_bytes()).await?;
        len += key.len() as u32;
        writer.write_u16(value.len() as u16).await?;
        len += 2;
        writer.write_all(value.as_bytes()).await?;
        len += value.len() as u32;
        writer.write_u32(len).await?;
        len += 4;

        let start_flush = Instant::now();
        writer.flush().await?;
        let flush_duration = start_flush.elapsed().as_micros() as f64;
        drop(writer);

        self.wal_buffer_lock_wait_histogram.observe(lock_wait);
        self.wal_buffer_flush_histogram.observe(flush_duration);

        let offset = self.offset.fetch_add(len as u64, Ordering::Relaxed);
        self.wal_write_histogram
            .observe(start.elapsed().as_micros() as f64);
        self.wal_size_gauge.set(offset as i64);

        // if buffer.len() > 10_000_000 {
        //     let mut writer = self.writer.lock().await;
        //     writer.write_all(&buffer).await?;
        //     writer.flush().await?;
        //     buffer.clear();
        // }

        // drop(buffer);

        //writer.flush().await?;

        // let now = Instant::now();
        // let elapsed = (now - self.start).as_millis() as u64;
        // if buffer.len() > 1 || (elapsed - self.last_flush.load(Ordering::Relaxed)) >= 5 {
        //     let start_flush = Instant::now();
        //     let mut writer = self.writer.lock().await;
        //     writer.write_all(&buffer).await?;
        //     writer.flush().await?;
        //     buffer.clear();
        //     self.last_flush.store(elapsed, Ordering::Relaxed);
        //     drop(buffer);
        //     self.wal_buffer_flush_histogram
        //         .observe(start_flush.elapsed().as_micros() as f64);
        // } else {
        //     drop(buffer);
        // }

        //let offset: u64 = writer.stream_position().await.unwrap(); //.seek(std::io::SeekFrom::Current(0)).await?;

        // let mut index = self.index.lock().await;
        // index.write(position, offset).await?;
        // index.position = position;

        //println!("Written {} to WAL", position);

        Ok(())
    }
    pub fn position(&self) -> u64 {
        self.position.load(Ordering::SeqCst)
    }
    // pub fn next_position(&self) -> u64 {
    //     self.position.fetch_add(1, Ordering::SeqCst)
    // }
}

#[derive(thiserror::Error, Debug)]
pub enum CreateError {
    #[error("failed to write because of an IO error")]
    CreateError(#[from] std::io::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[error("failed to write because of an IO error")]
    WriteError(#[from] std::io::Error),
}

#[derive(Debug)]
struct WriteAheadLogIndex {
    file: File,
    position: u64,
    offset: u64,
}

impl WriteAheadLogIndex {
    pub async fn create_or_open(path: &str) -> Result<Self, CreateError> {
        let (position, offset) = match Path::exists(Path::new(path)) {
            true => {
                let mut file = File::open(path.clone()).await?;
                let position = file.read_u64_le().await?;
                let offset = file.read_u64_le().await?;
                (position, offset)
            }
            false => {
                let mut file = File::create(path.clone()).await?;
                file.write_u64_le(0).await?;
                file.write_u64_le(0).await?;
                (0, 0)
            }
        };
        let file = OpenOptions::new().write(true).open(path).await?;
        Ok(Self {
            file,
            position,
            offset,
        })
    }
    pub async fn write(&mut self, position: u64, offset: u64) -> Result<(), WriteError> {
        self.position = position;
        self.position = offset;

        self.file.seek(std::io::SeekFrom::Start(0)).await?;
        self.file.write_u64_le(position).await?;
        self.file.write_u64_le(offset).await?;
        Ok(())
    }
}
