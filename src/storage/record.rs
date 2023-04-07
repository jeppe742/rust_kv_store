use std::{
    fs::File,
    io::{BufReader, Read},
    time::{SystemTime, UNIX_EPOCH},
};

const USIZE_BYTES: usize = (usize::BITS / 8) as usize;
const U128_BYTES: usize = (u128::BITS / 8) as usize;
const U8_BYTES: usize = (u8::BITS / 8) as usize;

#[derive(Debug, Clone)]
pub enum Record {
    Tombstone {
        timestamp: u128,
        key: String,
    },
    Value {
        timestamp: u128,
        key: String,
        value: String,
    },
}

impl Record {
    pub fn new(key: String, value: String) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Record::Value {
            timestamp,
            key,
            value,
        }
    }

    pub fn new_tombstone(key: String) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Record::Tombstone { timestamp, key }
    }

    // utility function, to avoid deconstructing the enum just to get the key, which we have in both
    pub fn get_key(&self) -> String {
        match self {
            Record::Tombstone { key, .. } => key.to_string(),
            Record::Value { key, .. } => key.to_string(),
        }
    }
    fn key_size(&self) -> usize {
        match self {
            Record::Tombstone { key, .. } => key.len(),
            Record::Value { key, .. } => key.len(),
        }
    }
    fn value_size(&self) -> usize {
        match self {
            Record::Tombstone { .. } => 0,
            Record::Value { value, .. } => value.len(),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Record::Tombstone { .. } => {
                self.key_size() + U8_BYTES + U128_BYTES + USIZE_BYTES // u8 for tombstone flag, u128 for timestamp and usize for key size
            }
            Record::Value { .. } => {
                self.key_size() + self.value_size() + U8_BYTES + U128_BYTES + 2 * USIZE_BYTES
                // u8 for tombstone flag, u128 for timestamp and 2 * usize for key & value size
            }
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Record::Tombstone { timestamp, key } => {
                let mut bytes = vec![1];
                bytes.extend(self.key_size().to_le_bytes().to_vec());
                bytes.extend(timestamp.to_le_bytes().to_vec());
                bytes.extend(key.as_bytes().to_vec());
                bytes
            }
            Record::Value {
                timestamp,
                key,
                value,
            } => {
                let mut bytes = vec![0];
                bytes.extend(self.key_size().to_le_bytes().to_vec());
                bytes.extend(timestamp.to_le_bytes().to_vec());
                bytes.extend(key.as_bytes().to_vec());
                bytes.extend(self.value_size().to_le_bytes().to_vec());
                bytes.extend(value.as_bytes().to_vec());
                bytes
            }
        }
    }

    pub fn from_reader(buf_reader: &mut BufReader<File>) -> Option<Self> {
        let mut tombstone_buffer = [0; 1];
        if let Err(err) = buf_reader.read_exact(&mut tombstone_buffer) {
            println!("{}", err);
            return None;
        }

        let is_tombstone = tombstone_buffer[0];

        let mut key_size_buffer = [0; 8]; // usize (8 bytes on x64)
        if let Err(err) = buf_reader.read_exact(&mut key_size_buffer) {
            println!("{}", err);
            return None;
        }
        let key_size = usize::from_le_bytes(key_size_buffer);

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

        match is_tombstone {
            0 => {
                let mut value_size_buffer = [0; 8]; // usize (8 bytes on x64)
                if let Err(err) = buf_reader.read_exact(&mut value_size_buffer) {
                    println!("{}", err);
                    return None;
                }
                let value_size = usize::from_le_bytes(value_size_buffer);

                let mut value_buffer = vec![0; value_size];
                if let Err(err) = buf_reader.read_exact(&mut value_buffer) {
                    println!("{}", err);
                    return None;
                }
                let value = String::from_utf8_lossy(&value_buffer).into_owned();

                Some(Record::Value {
                    timestamp,
                    key,
                    value,
                })
            }

            1 => Some(Record::Tombstone { timestamp, key }),
            _ => unreachable!(),
        }
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Record::Tombstone { timestamp, key },
                Record::Tombstone {
                    timestamp: other_timestamp,
                    key: other_key,
                },
            ) => timestamp == other_timestamp && key == other_key,
            (
                Record::Value {
                    timestamp,
                    key,
                    value,
                },
                Record::Value {
                    timestamp: other_timestamp,
                    key: other_key,
                    value: other_value,
                },
            ) => timestamp == other_timestamp && key == other_key && value == other_value,
            (_, _) => false,
        }
    }
}
