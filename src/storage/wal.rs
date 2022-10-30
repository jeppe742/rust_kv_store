use crc32fast;
use std::error::Error;
use std::fs::{File, OpenOptions};
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
    buf_reader: BufReader<File>, //todo: this will potentially keep unnessesary buffered data in memory
}

impl WriteAheadLog {
    pub fn new(path: &Path) -> io::Result<WriteAheadLog> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let path = Path::new(path).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let mut buf_writer = BufWriter::new(file);

        let file = OpenOptions::new().read(true).open(&path)?;
        let buf_reader = BufReader::new(file);

        Ok(WriteAheadLog {
            path,
            buf_writer,
            buf_reader,
        })
    }

    pub fn set(&mut self, entry: WALEntry) -> io::Result<()> {
        self.buf_writer.write_all(&entry.as_bytes());
        self.buf_writer.flush()
    }
}

// struct WriteAheadLogIntoIter {

// }

// impl IntoIterator for WriteAheadLog {
//     fn into_iter(self) -> Self::IntoIter {}

//     type Item = WALEntry;

//     type IntoIter = WriteAheadLog;
// }

impl Iterator for WriteAheadLog {
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
        let path = PathBuf::from("./");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        let entry = WALEntry::new("a".to_owned(), "b".to_owned());
        wal.set(entry);
    }

    #[test]
    fn test_iterator() {
        let path = PathBuf::from("./");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        let entry = WALEntry::new("a".to_owned(), "b".to_owned());
        wal.set(entry);

        let read_entry = wal.next().unwrap();
        assert_eq!(read_entry.key, "a");
        assert_eq!(read_entry.value, "b");

        assert!(wal.next().is_none());
        // wal.iter()
    }
}
