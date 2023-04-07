#![allow(dead_code)]
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::storage::memtable::MemTable;
use crate::storage::sstable::SSTable;
use crate::storage::wal::WriteAheadLog;

use glob::glob;

#[derive(Hash, PartialEq, Eq)]
enum DBConfig {
    MemtableSize,
}
pub struct DB {
    root_path: PathBuf,
    wal: WriteAheadLog,
    mem_table: MemTable<String, String>,
    sstable_path: PathBuf,
    wal_path: PathBuf,
    config: HashMap<DBConfig, usize>,
}

impl DB {
    pub fn new(path: &Path) -> DB {
        let wal = WriteAheadLog::new(&path.join("wal")).unwrap();
        let mem_table = MemTable::new();

        DB {
            root_path: path.to_path_buf(),
            wal,
            mem_table,
            sstable_path: path.join("sstable"),
            wal_path: path.join("wal"),
            config: vec![(DBConfig::MemtableSize, 128_000)]
                .into_iter()
                .collect(),
        }
    }

    fn set_config(&mut self, config: DBConfig, value: usize) {
        self.config.insert(config, value);
    }

    pub fn restore_wal(&mut self) {
        let mut mem_table = MemTable::new();
        let s = self.wal_path.join("*.wal").to_str().unwrap().to_string();
        for wal_entry in glob(&s).unwrap() {
            match wal_entry {
                Ok(wal_path) => {
                    let wal = WriteAheadLog::from_file(&wal_path).unwrap();
                    for wal_entry in wal.into_iter() {
                        mem_table.set(wal_entry.key, wal_entry.value)
                    }
                }
                Err(_) => todo!(),
            }
        }

        // mem_table
        // let mem_table = self.wal.into_memtable();
        self.mem_table = mem_table;
    }
    pub fn get(&self, key: &String) -> Option<String> {
        if let Some(v) = self.mem_table.get(key) {
            return Some(v);
        }

        for ss_table in glob(&self.sstable_path.join("*.ss").to_str().unwrap())
            .unwrap()
            .filter_map(Result::ok)
            .collect::<Vec<PathBuf>>()
            .into_iter()
            .rev()
        {
            if let Ok(Some(v)) = SSTable::get_disk(key, &ss_table) {
                return Some(v);
            }
        }

        None
    }

    pub fn set(&mut self, key: String, value: String) -> Result<(), io::Error> {
        let res = self.wal.set(key.clone(), value.clone());
        match res {
            Ok(_) => {
                self.mem_table.set(key, value);
            }
            Err(e) => return Err(e),
        }

        if self.mem_table.len() == *self.config.get(&DBConfig::MemtableSize).unwrap() {
            let new_sstable = SSTable::from_records(self.mem_table.to_records());

            new_sstable.write(&self.sstable_path).unwrap();

            self.mem_table = MemTable::new();
            self.wal = WriteAheadLog::new(&&self.root_path.join("wal")).unwrap();
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs::remove_dir_all;
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn set() {
        let path = PathBuf::from("./tests/db/output/set");

        let mut db = DB::new(&path);
        db.set("a".to_owned(), "b".to_owned()).unwrap();

        assert_eq!(db.get(&"a".to_owned()), Some("b".to_owned()));
        remove_dir_all(path).unwrap();
    }

    #[test]
    fn set_wal_fail() {
        let path = PathBuf::from("./tests/db/output/set_wal_fail");
        let mut db = DB::new(&path);

        db.set("a".to_owned(), "b".to_owned()).unwrap();
        remove_dir_all(path).unwrap();
    }

    #[test]
    fn restore_wal() {
        let path = PathBuf::from("./tests/db/output/restore_wal");
        let mut db = DB::new(&path);

        db.set("a".to_owned(), "b".to_owned()).unwrap();

        let mut db2 = DB::new(&path);
        db2.restore_wal();
        assert_eq!(db2.get(&"a".to_owned()), Some("b".to_owned()));
        remove_dir_all(path).unwrap();
    }

    #[test]
    fn multiple_ss_tables() {
        let path = PathBuf::from("./tests/db/output/multiple_ss_tables");
        let mut db = DB::new(&path);
        let mem_table_size = 10_000;
        db.set_config(DBConfig::MemtableSize, mem_table_size);

        for i in 0..mem_table_size - 1 {
            db.set(format!("{}{}", "a".to_owned(), i), i.to_string())
                .unwrap();
        }

        assert_eq!(db.mem_table.len(), mem_table_size - 1);
        db.set(
            format!("{}{}", "a".to_owned(), mem_table_size),
            mem_table_size.to_string(),
        )
        .unwrap();
        assert_eq!(db.mem_table.len(), 0);
        for i in mem_table_size + 1..2 * mem_table_size + 11 {
            db.set(format!("{}{}", "a".to_owned(), i), i.to_string())
                .unwrap();
        }

        assert_eq!(db.mem_table.len(), 10);

        assert_eq!(db.get(&"a1".to_owned()), Some("1".to_owned()));
        assert_eq!(db.get(&"a2".to_owned()), Some("2".to_owned()));
        assert_eq!(db.get(&"a3".to_owned()), Some("3".to_owned()));
        remove_dir_all(path).unwrap();
    }
}
