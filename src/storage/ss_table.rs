use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::logger;
use std::{
    fs::{read_dir, File},
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
    thread::sleep,
    time::{Duration, SystemTime},
};
use stopwatch::Stopwatch;

use super::{
    key_value::{merge_sorted_key_value_iters, KeyValue},
    skip_list::SkipList,
};

pub struct SSTable {
    path: PathBuf,
}

const SSTABLES_DIRECTORY: &str = "sstables";

impl SSTable {
    pub fn write_from_iter(iter: impl Iterator<Item = KeyValue>) -> Result<SSTable> {
        let file = PathBuf::from(format!(
            "{}.sstable",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_nanos()
        ));
        let path: PathBuf = PathBuf::from(SSTABLES_DIRECTORY).join(file);
        let mut file = BufWriter::new(File::create(&path)?);
        for item in iter {
            let key_bytes = item.key.as_bytes();
            let value_bytes = item.value.as_bytes();
            file.write_u16::<LittleEndian>(
                key_bytes
                    .len()
                    .try_into()
                    .expect("Key length exceeds maximum size of u16"),
            )?;
            file.write(&key_bytes)?;
            file.write_u16::<LittleEndian>(
                value_bytes
                    .len()
                    .try_into()
                    .expect("Value length exceeds maximum size of u16"),
            )?;
            file.write(&value_bytes)?;
        }
        Ok(SSTable { path })
    }
    pub fn iter(&self) -> Result<SSTableIterator> {
        Ok(SSTableIterator {
            reader: BufReader::new(File::open(&self.path)?),
        })
    }
    pub fn remove(self) -> Result<()> {
        std::fs::remove_file(&self.path)?;
        Ok(())
    }
}

pub struct SSTableIterator {
    reader: BufReader<File>,
}

impl Iterator for SSTableIterator {
    type Item = KeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        let key_bytes_len = match self.reader.read_u16::<LittleEndian>() {
            Ok(len) => len,
            Err(_) => return None,
        };
        let mut key_bytes = vec![0; key_bytes_len as usize];
        self.reader
            .read_exact(&mut key_bytes)
            .expect("Unable to read key in SS table");
        let key = String::from_utf8(key_bytes).expect("Invalid UTF-8 key bytes in SS table");
        let value_bytes_len = self
            .reader
            .read_u16::<LittleEndian>()
            .expect("Unable to read value length in SS table");
        let mut value_bytes = vec![0; value_bytes_len as usize];
        self.reader
            .read_exact(&mut value_bytes)
            .expect("Unable to read value in SS table");
        let value = String::from_utf8(value_bytes).expect("Invalid UTF-8 value bytes in SS table");
        Some(KeyValue { key, value })
    }
    // pub fn read_as_iter(&self) -> Result<impl Iterator<Item = KeyValue>> {
    //     loop {
    //         let key_bytes_len = match file.read_u16::<LittleEndian>() {
    //             Ok(len) => len,
    //             Err(_) => break,
    //         };
    //         let mut key_bytes = vec![0; key_bytes_len as usize];
    //         file.read_exact(&mut key_bytes)?;
    //         let value_bytes_len = file.read_u16::<LittleEndian>()?;
    //         let mut value_bytes = vec![0; value_bytes_len as usize];
    //         file.read_exact(&mut value_bytes)?;
    //     }

    //     Ok(())
    //     // while let Some(str) = file.read
    //     // for item in iter {
    //     //     file.write(&item.as_bytes())?;
    //     // }
    //     // Ok(SSTable { file })
    // }
}

pub fn compact_ss_tables() -> Result<()> {
    // Check if there are any SSTables to compact
    let files = read_dir(SSTABLES_DIRECTORY)?;
    let files = files.collect::<Vec<_>>();

    // Get the two most recent table paths
    //let files = files.iter().map(|file| file.path()).collect::<Vec<_>>();
    let mut files = files
        .into_iter()
        .filter_map(|file| file.ok())
        .collect::<Vec<_>>();
    files.sort_by_key(|file| {
        file.metadata()
            .expect("Unable to file SS table file metadata")
            .modified()
            .expect("Unable to file SS table file modified time")
    });
    let most_recent = files.get(0);
    let second_most_recent = files.get(1);

    match (most_recent, second_most_recent) {
        (Some(most_recent), Some(second_most_recent)) => {
            log::debug!("Compacting {:?} and {:?}", most_recent, second_most_recent);

            let most_recent_ss_table = SSTable {
                path: most_recent.path(),
            };
            let second_most_recent_ss_table = SSTable {
                path: second_most_recent.path(),
            };

            let merged = merge_sorted_key_value_iters(
                most_recent_ss_table.iter()?,
                second_most_recent_ss_table.iter()?,
            );

            SSTable::write_from_iter(merged)?;

            // Delete existing tables
            most_recent_ss_table.remove()?;
            second_most_recent_ss_table.remove()?;
        }
        _ => return Ok(()),
    }
    Ok(())
}

pub fn demo_ss_tables() -> Result<()> {
    let stopwatch = Stopwatch::start_new();
    let skip_list = SkipList::new();
    SSTable::write_from_iter(skip_list.iter())?;
    println!("Saving took {}ms", stopwatch.elapsed_ms());

    compact_ss_tables()?;

    Ok(())
}
