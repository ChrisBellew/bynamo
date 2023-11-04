use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Seek, Write},
    path::PathBuf,
};

pub struct WriteAheadLog {
    writer: BufWriter<File>,
    index: WriteAheadLogIndex,
}

impl WriteAheadLog {
    pub fn create_or_open_for_append(log_path: PathBuf, index_path: PathBuf) -> Result<Self> {
        let mut writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path.clone())?,
        );
        let index = WriteAheadLogIndex::create_or_open(index_path)?;
        writer.seek(std::io::SeekFrom::Start(index.offset))?;
        println!("index: {:?}", &index);
        Ok(Self { writer, index })
    }
    pub fn write_entry(&mut self, position: u64, key: &str, value: &str) -> Result<()> {
        self.writer.write_u64::<LittleEndian>(position)?;
        self.writer.write_u64::<LittleEndian>(key.len() as u64)?;
        self.writer.write_all(key.as_bytes())?;
        self.writer.write_u64::<LittleEndian>(value.len() as u64)?;
        self.writer.write_all(value.as_bytes())?;
        self.writer.flush()?;

        let offset = self.writer.seek(std::io::SeekFrom::Current(0))?;
        self.index.update(position, offset)?;
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
    pub fn create_or_open(path: PathBuf) -> Result<Self> {
        let (position, offset) = match path.exists() {
            true => {
                let mut file = File::open(path.clone())?;
                let position = file.read_u64::<LittleEndian>()?;
                let offset = file.read_u64::<LittleEndian>()?;
                (position, offset)
            }
            false => {
                let mut file = File::create(path.clone())?;
                file.write_u64::<LittleEndian>(0)?;
                file.write_u64::<LittleEndian>(0)?;
                (0, 0)
            }
        };
        let file = OpenOptions::new().write(true).open(path)?;
        Ok(Self {
            file,
            position,
            offset,
        })
    }
    pub fn update(&mut self, position: u64, offset: u64) -> Result<()> {
        self.position = position;
        self.position = offset;
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write_u64::<LittleEndian>(position)?;
        self.file.write_u64::<LittleEndian>(offset)?;
        Ok(())
    }
}
