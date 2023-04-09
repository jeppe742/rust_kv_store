#![allow(dead_code)]
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::storage::memtable::MemTable;
use crate::storage::sstable::SSTable;
use crate::storage::wal::WriteAheadLog;

use glob::{glob, GlobResult};
use itertools::Itertools;

#[derive(Hash, PartialEq, Eq)]
enum DBConfig {
    MemtableSize,
}
pub struct DB {
    root_path: PathBuf,
    wal: WriteAheadLog,
    mem_table: MemTable,
    sstable_path: PathBuf,
    sstables: Vec<PathBuf>,
    wal_path: PathBuf,
    config: HashMap<DBConfig, usize>,
}

impl DB {
    pub fn new(path: &Path) -> DB {
        let wal = WriteAheadLog::new(&path.join("wal")).unwrap();
        let mem_table = MemTable::new();

        let sstables: Vec<PathBuf> = glob(path.join("sstable").join("*.ss").to_str().unwrap())
            .unwrap()
            .flat_map(|p| p.ok())
            .sorted()
            .collect();

        DB {
            root_path: path.to_path_buf(),
            wal,
            mem_table,
            sstable_path: path.join("sstable"),
            sstables,
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
        let s = self.wal_path.join("*.wal").to_str().unwrap().to_string();
        let mem_table = match glob(&s).unwrap().next() {
            Some(GlobResult::Ok(wal_path)) => {
                let wal = WriteAheadLog::from_file(&wal_path).unwrap();
                wal.into_memtable()
            }
            _ => unreachable!(),
        };

        self.mem_table = mem_table;
    }
    pub fn get(&self, key: &String) -> Option<String> {
        if let Some(v) = self.mem_table.get(key) {
            return Some(v);
        }

        for ss_table_path in self.sstables.iter().rev() {
            if let Some(value) = SSTable::from_disk(ss_table_path)
                .unwrap_or_default()
                .get(ss_table_path, key)
                .unwrap()
            {
                return Some(value);
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

            let new_sstable_path = new_sstable.write(&self.sstable_path).unwrap();

            self.sstables.push(new_sstable_path);

            self.mem_table = MemTable::new();
            self.wal = WriteAheadLog::new(&self.root_path.join("wal")).unwrap();
        }

        Ok(())
    }

    pub fn delete(&mut self, key: String) -> Result<(), io::Error> {
        let res = self.wal.delete(key.clone());
        match res {
            Ok(_) => {
                self.mem_table.delete(key);
            }
            Err(e) => return Err(e),
        }

        if self.mem_table.len() == *self.config.get(&DBConfig::MemtableSize).unwrap() {
            let new_sstable = SSTable::from_records(self.mem_table.to_records());

            new_sstable.write(&self.sstable_path).unwrap();

            self.mem_table = MemTable::new();
            self.wal = WriteAheadLog::new(&self.root_path.join("wal")).unwrap();
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
    fn delete() {
        let path = PathBuf::from("./tests/db/output/delete");

        let mut db = DB::new(&path);
        db.set("a".to_owned(), "b".to_owned()).unwrap();

        assert_eq!(db.get(&"a".to_owned()), Some("b".to_owned()));

        db.delete("a".to_owned()).unwrap();

        assert_eq!(db.get(&"a".to_owned()), None);

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
