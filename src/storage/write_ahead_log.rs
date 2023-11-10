use anyhow::Result;
use std::path::PathBuf;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
};

pub struct WriteAheadLog {
    writer: BufWriter<File>,
    index: WriteAheadLogIndex,
}

impl WriteAheadLog {
    pub async fn create_or_open_for_append(log_path: PathBuf, index_path: PathBuf) -> Result<Self> {
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
        Ok(Self { writer, index })
    }
    pub async fn write_entry(&mut self, position: u64, key: &str, value: &str) -> Result<()> {
        self.writer.write_u64_le(position).await?;
        self.writer.write_u64_le(key.len() as u64).await?;
        self.writer.write_all(key.as_bytes()).await?;
        self.writer.write_u64_le(value.len() as u64).await?;
        self.writer.write_all(value.as_bytes()).await?;
        self.writer.flush().await?;

        let offset = self.writer.seek(std::io::SeekFrom::Current(0)).await?;
        self.index.update(position, offset).await?;
        self.index.position = position;

        Ok(())
    }
    pub fn position(&self) -> u64 {
        self.index.position
    }
}

#[derive(Debug)]
struct WriteAheadLogIndex {
    file: File,
    position: u64,
    offset: u64,
}

impl WriteAheadLogIndex {
    pub async fn create_or_open(path: PathBuf) -> Result<Self> {
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
    pub async fn update(&mut self, position: u64, offset: u64) -> Result<()> {
        self.position = position;
        self.position = offset;
        self.file.seek(std::io::SeekFrom::Start(0)).await?;
        self.file.write_u64_le(position).await?;
        self.file.write_u64_le(offset).await?;
        Ok(())
    }
}
