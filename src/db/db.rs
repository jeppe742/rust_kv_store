use std::io;
use std::path::{Path, PathBuf};

use crate::storage::memtable::MemTable;
use crate::storage::sstable::SSTable;
use crate::storage::wal::WriteAheadLog;

use glob::glob;

struct DB {
    root_path: PathBuf,
    wal: WriteAheadLog,
    mem_table: MemTable<String, String>,
    sstable_path: PathBuf,
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
        }
    }

    pub fn restore_wal(&mut self) {
        let mut mem_table = MemTable::new();

        let latest_wal = glob(self.root_path.join("*.wal").to_str().unwrap()).unwrap();

        for wal_entry in self.wal.into_iter() {
            mem_table.set(wal_entry.key, wal_entry.value)
        }

        // mem_table
        // let mem_table = self.wal.into_memtable();
        self.mem_table = mem_table;
    }
    pub fn get(&self, key: &String) -> Option<String> {
        if let Some(v) = self.mem_table.get(key) {
            return Some(v);
        }

        if let Some(v) = SSTable::get_disk(key, &self.sstable_path) {
            return Some(v);
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

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_set() {
        let path = PathBuf::from("./tests/db/output/1");

        let mut db = DB::new(&path);
        db.set("a".to_owned(), "b".to_owned()).unwrap();

        assert_eq!(db.get(&"a".to_owned()), Some("b".to_owned()));
    }

    #[test]
    fn test_set_wal_fail() {
        let path = PathBuf::from("./tests/db/output/2");
        let mut db = DB::new(&path);

        db.set("a".to_owned(), "b".to_owned()).unwrap();
    }

    #[test]
    fn test_restore_wal() {
        let path = PathBuf::from("./tests/db/output/3");
        let mut db = DB::new(&path);

        db.set("a".to_owned(), "b".to_owned()).unwrap();

        let mut db2 = DB::new(&path);
        db2.restore_wal();
        assert_eq!(db2.get(&"a".to_owned()), Some("b".to_owned()));
    }
}
