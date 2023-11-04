use std::{
    path::PathBuf,
    thread::{sleep, spawn, JoinHandle},
    time::Duration,
};

use crate::storage::{
    skip_list::{coin_flip, SkipList},
    ss_table::{compact_ss_tables, SSTable},
    write_ahead_log::WriteAheadLog,
};
use anyhow::Result;

pub struct Coordinator {
    skip_list: SkipList,
    write_ahead_log: WriteAheadLog,
    maintenance_thread: Option<JoinHandle<()>>,
}

impl Coordinator {
    pub fn create(write_ahead_log_path: PathBuf, write_ahead_index_path: PathBuf) -> Result<Self> {
        Ok(Self {
            skip_list: SkipList::new(),
            write_ahead_log: WriteAheadLog::create_or_open_for_append(
                write_ahead_log_path,
                write_ahead_index_path,
            )?,
            maintenance_thread: None,
        })
    }
    pub fn read(&self, key: &str) -> Option<String> {
        self.skip_list.find(key)
    }
    pub fn write(&mut self, key: String, value: String) -> Result<()> {
        let position = self.write_ahead_log.position() + 1;
        self.write_ahead_log.write_entry(position, &key, &value)?;
        // self.skip_list
        //     .insert(key.to_string(), value.to_string(), coin_flip());
        // println!("Size: {}", self.skip_list.size);
        // if self.skip_list.size > 1_000_000 {
        //     SSTable::write_from_iter(self.skip_list.iter())?;
        //     self.skip_list = SkipList::new();
        // }
        Ok(())
    }
    pub fn start(&mut self) {
        self.maintenance_thread = Some(spawn(|| loop {
            sleep(Duration::from_secs(1));
            compact_ss_tables().expect("Failure compacting SS tables");
        }));
    }
}
