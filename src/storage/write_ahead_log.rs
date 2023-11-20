use std::{path::PathBuf, sync::Arc};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
    sync::Mutex,
};

#[derive(Clone)]
pub struct WriteAheadLog {
    writer: Arc<Mutex<BufWriter<File>>>,
    index: Arc<Mutex<WriteAheadLogIndex>>,
}

impl WriteAheadLog {
    pub async fn create(log_path: PathBuf, index_path: PathBuf) -> Result<Self, CreateError> {
        let mut writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path.clone())
                .await?,
        );
        let index = WriteAheadLogIndex::create_or_open(index_path).await?;
        writer.seek(std::io::SeekFrom::Start(index.offset)).await?;
        println!("index: {:?}", &index);
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            index: Arc::new(Mutex::new(index)),
        })
    }
    pub async fn write(&mut self, position: u64, key: &str, value: &str) -> Result<(), WriteError> {
        let mut writer = self.writer.lock().await;
        writer.write_u64_le(position).await?;
        writer.write_u64_le(key.len() as u64).await?;
        writer.write_all(key.as_bytes()).await?;
        writer.write_u64_le(value.len() as u64).await?;
        writer.write_all(value.as_bytes()).await?;
        writer.flush().await?;

        let offset = writer.seek(std::io::SeekFrom::Current(0)).await?;

        let mut index = self.index.lock().await;
        index.write(position, offset).await?;
        index.position = position;

        Ok(())
    }
    pub async fn position(&self) -> u64 {
        self.index.lock().await.position
    }
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
    pub async fn create_or_open(path: PathBuf) -> Result<Self, CreateError> {
        let (position, offset) = match path.exists() {
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
