use std::{
    fs::File,
    io::{BufReader, Read},
    time::{SystemTime, UNIX_EPOCH},
};

const USIZE_BYTES: usize = (usize::BITS / 8) as usize;
const U128_BYTES: usize = (u128::BITS / 8) as usize;

#[derive(Debug)]
pub struct Record {
    pub timestamp: u128,
    pub key: String,
    pub value: String,
}

impl Record {
    pub fn new(key: String, value: String) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Record {
            timestamp,
            key,
            value,
        }
    }
    fn key_size(&self) -> usize {
        self.key.len()
    }
    fn value_size(&self) -> usize {
        self.value.len()
    }

    pub fn size(&self) -> usize {
        self.key_size() + self.value_size() + U128_BYTES + 2 * USIZE_BYTES
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(self.key_size().to_le_bytes().to_vec());
        bytes.extend(self.value_size().to_le_bytes().to_vec());
        bytes.extend(self.timestamp.to_le_bytes().to_vec());
        bytes.extend(self.key.as_bytes().to_vec());
        bytes.extend(self.value.as_bytes().to_vec());
        bytes
    }

    pub fn from_reader(buf_reader: &mut BufReader<File>) -> Option<Self> {
        let mut key_size_buffer = [0; 8]; // usize (8 bytes on x64)
        if let Err(err) = buf_reader.read_exact(&mut key_size_buffer) {
            println!("{}", err);
            return None;
        }
        let key_size = usize::from_le_bytes(key_size_buffer);

        let mut value_size_buffer = [0; 8]; // usize (8 bytes on x64)
        if let Err(err) = buf_reader.read_exact(&mut value_size_buffer) {
            println!("{}", err);
            return None;
        }
        let value_size = usize::from_le_bytes(value_size_buffer);

        let mut timestamp_buffer = [0; 16]; // u128 (16 bytes)
        if let Err(err) = buf_reader.read_exact(&mut timestamp_buffer) {
            println!("{}", err);
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffer);

        let mut key_buffer = vec![0; key_size];
        if let Err(err) = buf_reader.read_exact(&mut key_buffer) {
            println!("{}", err);
            return None;
        }
        let key = String::from_utf8_lossy(&key_buffer).into_owned();

        let mut value_buffer = vec![0; value_size];
        if let Err(err) = buf_reader.read_exact(&mut value_buffer) {
            println!("{}", err);
            return None;
        }
        let value = String::from_utf8_lossy(&value_buffer).into_owned();

        Some(Record {
            timestamp,
            key,
            value,
        })
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.key == other.key && self.value == other.value
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.timestamp.partial_cmp(&other.timestamp) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.key.partial_cmp(&other.key) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.value.partial_cmp(&other.value)
    }
}
