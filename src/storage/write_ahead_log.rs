use prometheus::{exponential_buckets, Histogram, HistogramOpts, Registry};
use prometheus::{IntGauge, Opts};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::{path::Path, sync::Arc};
use tokio::sync::Notify;
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
    //writer: Arc<Mutex<BufWriter<File>>>,
    //index: Arc<Mutex<WriteAheadLogIndex>>,
    //wal_size_gauge: IntGauge,
    wal_write_histogram: Histogram,
    wal_buffer_lock_wait_histogram: Histogram,
    //wal_buffer_flush_histogram: Histogram,
    //offset: Arc<AtomicU64>,
    position: Arc<AtomicU64>,
    buffer: Arc<Mutex<Vec<u8>>>,
    //buffer_start: Arc<AtomicU64>,
    start: Instant,
    last_flush: Arc<AtomicU64>,
    buffer_notify: Arc<Mutex<(Vec<Vec<u8>>, Arc<Notify>)>>,
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
        let (max_position, mut offset) = if wal_size > 0 {
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

        let buffer_notify = Arc::new(Mutex::new((Vec::<Vec<u8>>::new(), Arc::new(Notify::new()))));

        {
            let buffer_notify = buffer_notify.clone();
            //println!("Spawning writer");
            tokio::task::spawn(async move {
                let mut file = BufWriter::new(file);

                //println!("Starting writer");

                loop {
                    //tokio::time::sleep(Duration::from_micros(500)).await;
                    let start_flush = Instant::now();
                    //println!("Getting buffer_notify lock");
                    let (buf, not) = {
                        let mut guard = buffer_notify.lock().await;
                        //println!("Got buffer_notify lock");
                        let buf = std::mem::take(&mut guard.0);
                        let not = std::mem::replace(&mut guard.1, Arc::new(Notify::new()));
                        (buf, not)
                    };
                    //drop(guard);
                    //println!("Got vec {}", buf.len());

                    if buf.is_empty() {
                        continue;
                    }

                    //writer.flush().await?;

                    let mut wrote = 0;
                    for vec in buf {
                        file.write_all(&vec).await.unwrap();
                        wrote += vec.len();
                    }
                    file.flush().await.unwrap();
                    //drop(writer);
                    //println!("Wrote {} bytes", wrote);
                    //println!("Flushed");

                    not.notify_waiters();

                    //self.wal_buffer_lock_wait_histogram.observe(lock_wait);
                    let flush_duration = start_flush.elapsed().as_micros() as f64;
                    wal_buffer_flush_histogram.observe(flush_duration);

                    offset += wrote as u64;
                    //let offset = offset.fetch_add(wrote as u64, Ordering::Relaxed);
                    wal_size_gauge.set(offset as i64);
                }
            });
        }

        Ok(Self {
            //writer: Arc::new(Mutex::new(BufWriter::new(file))),
            //index: Arc::new(Mutex::new(index)),
            //wal_size_gauge,
            wal_write_histogram,
            wal_buffer_lock_wait_histogram,
            //wal_buffer_flush_histogram,
            //offset: Arc::new(AtomicU64::new(offset)),
            position: Arc::new(AtomicU64::new(max_position)),
            buffer: Arc::new(Mutex::new(Vec::new())),
            start: Instant::now(),
            last_flush: Arc::new(0.into()),
            buffer_notify,
        })
    }
    pub async fn write_new(&mut self, key: &str, value: &str) -> Result<u64, WriteError> {
        let position = self.position.fetch_add(1, Ordering::Relaxed);
        self.write(position, key, value).await?;
        Ok(position)
    }
    pub async fn write(&mut self, position: u64, key: &str, value: &str) -> Result<(), WriteError> {
        let start = Instant::now();

        //let mut len = 0u32;
        //let mut writer = self.writer.lock().await;
        //let lock_wait = start.elapsed().as_micros() as f64;
        let mut buf = Vec::new();

        byteorder::WriteBytesExt::write_u64::<byteorder::BigEndian>(&mut buf, position).unwrap();
        byteorder::WriteBytesExt::write_u16::<byteorder::BigEndian>(&mut buf, key.len() as u16)
            .unwrap();
        std::io::Write::write_all(&mut buf, key.as_bytes()).unwrap();
        byteorder::WriteBytesExt::write_u16::<byteorder::BigEndian>(&mut buf, value.len() as u16)
            .unwrap();
        std::io::Write::write_all(&mut buf, value.as_bytes()).unwrap();
        let len = buf.len();
        byteorder::WriteBytesExt::write_u32::<byteorder::BigEndian>(&mut buf, len as u32).unwrap();

        let notify = {
            let mut guard = self.buffer_notify.lock().await;
            let notify = guard.1.clone();
            let buffer = &mut guard.0;
            buffer.push(buf);
            notify
        };
        //println!("Waiting");
        notify.notified().await;

        self.wal_write_histogram
            .observe(start.elapsed().as_micros() as f64);

        //let handle = tokio::spawn(async {});

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
