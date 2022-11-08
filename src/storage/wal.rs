#![allow(dead_code)]
use super::memtable::MemTable;
use crc32fast;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::prelude::*;
use std::io::Write;
use std::io::{self, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const BLOCKSIZE: u16 = 32000;

struct WALEntry {
    crc: u32, // CRC = 32bit hash computed over the payload using CRC
    key_size: usize,
    value_size: usize,
    timestamp: u128,
    key: String,
    value: String,
}

impl WALEntry {
    pub fn new(key: String, value: String) -> WALEntry {
        let crc = crc32fast::hash(value.as_bytes());
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        WALEntry {
            crc,
            key_size: key.len(),
            value_size: value.len(),
            timestamp,
            key,
            value,
        }
    }

    fn as_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(self.crc.to_le_bytes().to_vec());
        bytes.extend(self.key_size.to_le_bytes().to_vec());
        bytes.extend(self.value_size.to_le_bytes().to_vec());
        bytes.extend(self.timestamp.to_le_bytes().to_vec());
        bytes.extend(self.key.as_bytes().to_vec());
        bytes.extend(self.value.as_bytes().to_vec());
        bytes
    }
}

struct WALBlock {
    entries: Vec<WALEntry>,
}

struct WriteAheadLog {
    path: PathBuf,
    buf_writer: BufWriter<File>,
}

impl WriteAheadLog {
    pub fn new(path: &Path) -> io::Result<WriteAheadLog> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        create_dir_all(path);
        let path = Path::new(path).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let buf_writer = BufWriter::new(file);

        Ok(WriteAheadLog { path, buf_writer })
    }

    fn set(&mut self, entry: WALEntry) -> io::Result<()> {
        self.buf_writer.write_all(&entry.as_bytes()).unwrap();
        self.buf_writer.flush()
    }

    pub fn into_memtable(self) -> MemTable<String, String> {
        let mut mem_table = MemTable::new();
        for wal_entry in self.into_iter() {
            mem_table.insert(wal_entry.key, wal_entry.value)
        }

        mem_table
    }
}

struct WriteAheadLogIter {
    buf_reader: BufReader<File>,
}

impl IntoIterator for WriteAheadLog {
    type Item = WALEntry;

    type IntoIter = WriteAheadLogIter;

    fn into_iter(self) -> Self::IntoIter {
        let file = OpenOptions::new().read(true).open(self.path).unwrap();
        let buf_reader = BufReader::new(file);
        WriteAheadLogIter { buf_reader }
    }
}

impl Iterator for WriteAheadLogIter {
    type Item = WALEntry;
    fn next(&mut self) -> Option<Self::Item> {
        let mut crc_buffer = [0; 4]; // u32 (4 bytes)
        if let Err(err) = self.buf_reader.read_exact(&mut crc_buffer) {
            println!("{}", err);
            return None;
        };
        let crc = u32::from_le_bytes(crc_buffer);

        let mut key_size_buffer = [0; 8]; // usize (8 bytes on x64)
        if let Err(err) = self.buf_reader.read_exact(&mut key_size_buffer) {
            println!("{}", err);
            return None;
        }
        let key_size = usize::from_le_bytes(key_size_buffer);

        let mut value_size_buffer = [0; 8]; // usize (8 bytes on x64)
        if let Err(err) = self.buf_reader.read_exact(&mut value_size_buffer) {
            println!("{}", err);
            return None;
        }
        let value_size = usize::from_le_bytes(value_size_buffer);

        let mut timestamp_buffer = [0; 16]; // u128 (16 bytes)
        if let Err(err) = self.buf_reader.read_exact(&mut timestamp_buffer) {
            println!("{}", err);
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffer);

        let mut key_buffer = vec![0; key_size];
        if let Err(err) = self.buf_reader.read_exact(&mut key_buffer) {
            println!("{}", err);
            return None;
        }
        let key = String::from_utf8_lossy(&key_buffer).into_owned();

        let mut value_buffer = vec![0; value_size];
        if let Err(err) = self.buf_reader.read_exact(&mut value_buffer) {
            println!("{}", err);
            return None;
        }
        let value = String::from_utf8_lossy(&value_buffer).into_owned();

        match crc32fast::hash(value.as_bytes()).eq(&crc) {
            true => Some(WALEntry {
                crc,
                key_size,
                value_size,
                timestamp,
                key,
                value,
            }),
            false => {
                panic!("Log has been corrupted!!")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_set() {
        let path = PathBuf::from("./tests/output/1");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        let entry = WALEntry::new("a".to_owned(), "b".to_owned());
        wal.set(entry).unwrap();
    }

    #[test]
    fn test_iterator() {
        let path = PathBuf::from("./tests/output/2");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        let entry = WALEntry::new("a".to_owned(), "b".to_owned());
        wal.set(entry).unwrap();

        let mut wal_iter = wal.into_iter();
        let read_entry = wal_iter.next().unwrap();
        assert_eq!(read_entry.key, "a");
        assert_eq!(read_entry.value, "b");

        assert!(wal_iter.next().is_none());
    }

    #[test]
    fn test_into_memtable() {
        let path = PathBuf::from("./tests/output/3");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        let entry = WALEntry::new("a".to_owned(), "b".to_owned());
        wal.set(entry).unwrap();

        let mem_table = wal.into_memtable();

        assert_eq!(mem_table.get(&"a".to_owned()), Some(&"b".to_owned()))
    }
}
