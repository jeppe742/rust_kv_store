#![allow(dead_code)]
use super::memtable::MemTable;
use super::record::Record;

use std::fs::{create_dir_all, File, OpenOptions};

use std::io::Write;
use std::io::{self, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const BLOCKSIZE: u16 = 32000;

struct WALBlock {
    entries: Vec<Record>,
}

pub struct WriteAheadLog {
    path: PathBuf,
    buf_writer: BufWriter<File>,
}

impl WriteAheadLog {
    pub fn new(path: &Path) -> io::Result<WriteAheadLog> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        create_dir_all(path)?;
        let path = Path::new(path).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let buf_writer = BufWriter::new(file);

        Ok(WriteAheadLog { path, buf_writer })
    }

    pub fn from_file(path: &Path) -> io::Result<WriteAheadLog> {
        let file = OpenOptions::new().append(true).open(&path)?;
        let buf_writer = BufWriter::new(file);

        Ok(WriteAheadLog {
            path: path.to_path_buf(),
            buf_writer,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> io::Result<()> {
        let entry = Record::new(key, value);
        self.buf_writer.write_all(&entry.as_bytes()).unwrap();
        self.buf_writer.flush().unwrap();
        Ok(())
    }

    pub fn into_memtable(self) -> MemTable<String, String> {
        let mut mem_table = MemTable::new();
        for wal_entry in self.into_iter() {
            mem_table.set(wal_entry.key, wal_entry.value)
        }

        mem_table
    }
}

pub struct WriteAheadLogIter {
    buf_reader: BufReader<File>,
}

impl IntoIterator for WriteAheadLog {
    type Item = Record;

    type IntoIter = WriteAheadLogIter;

    fn into_iter(self) -> Self::IntoIter {
        let file = OpenOptions::new().read(true).open(self.path).unwrap();
        let buf_reader = BufReader::new(file);
        WriteAheadLogIter { buf_reader }
    }
}

impl Iterator for WriteAheadLogIter {
    type Item = Record;
    fn next(&mut self) -> Option<Self::Item> {
        Record::from_reader(&mut self.buf_reader)
    }
}

#[cfg(test)]
mod test {
    use std::fs::remove_dir_all;

    use super::*;

    #[test]
    fn set() {
        let path = PathBuf::from("./tests/wal/output/set");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        wal.set("a".to_owned(), "b".to_owned()).unwrap();
    }

    #[test]
    fn iterator() {
        let path = PathBuf::from("./tests/wal/output/iterator");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        wal.set("a".to_owned(), "b".to_owned()).unwrap();

        let mut wal_iter = wal.into_iter();
        let read_entry = wal_iter.next().unwrap();
        assert_eq!(read_entry.key, "a");
        assert_eq!(read_entry.value, "b");

        assert!(wal_iter.next().is_none());
    }

    #[test]
    fn into_memtable() {
        let path = PathBuf::from("./tests/wal/output/into_memtable");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        wal.set("a".to_owned(), "b".to_owned()).unwrap();

        let mem_table = wal.into_memtable();

        assert_eq!(mem_table.get(&"a".to_owned()), Some("b".to_owned()));
        remove_dir_all(path).unwrap();
    }
}
