#![allow(dead_code)]
/*

```
<beginning_of_file>
[data block 1]
[data block 2]
...
[data block N]
[meta block 1: filter block]                  (see section: "filter" Meta Block)
[meta block 2: index block]
[meta block 3: compression dictionary block]  (see section: "compression dictionary" Meta Block)
[meta block 4: range deletion block]          (see section: "range deletion" Meta Block)
[meta block 5: stats block]                   (see section: "properties" Meta Block)
...
[meta block K: future extended block]  (we may add more meta blocks in the future)
[metaindex block]
[Footer]                               (fixed size; starts at file_size - sizeof(Footer))
<end_of_file>
```
 */
use std::mem::size_of_val;
use std::time::{SystemTime, UNIX_EPOCH};
const BLOCKSIZE: usize = 32000;
const USIZE_BYTES: usize = (usize::BITS / 8) as usize;
const U128_BYTES: usize = (u128::BITS / 8) as usize;

#[derive(Debug)]
struct Entry {
    key_size: usize,
    value_size: usize,
    timestamp: u128,
    key: String,
    value: String,
}

impl Entry {
    pub fn new(key: String, value: String) -> Entry {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Entry {
            key_size: key.len(),
            value_size: value.len(),
            timestamp,
            key,
            value,
        }
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.key_size == other.key_size
            && self.value_size == other.value_size
            && self.timestamp == other.timestamp
            && self.key == other.key
            && self.value == other.value
    }
}

struct Block {
    /**/
    entries: Vec<Entry>,
}
impl Block {
    pub fn from_bytes(bytes: &[u8; BLOCKSIZE]) -> Block {
        let mut offset = 0;
        let mut entries = vec![];
        while offset < BLOCKSIZE {
            let key_size =
                usize::from_le_bytes(bytes[offset..offset + USIZE_BYTES].try_into().unwrap());
            offset += USIZE_BYTES;

            // we have reatched the end of data in the block.
            // The rest is padded with /0 and will have key_size 0
            if key_size == 0 {
                break;
            }
            let value_size =
                usize::from_le_bytes(bytes[offset..offset + USIZE_BYTES].try_into().unwrap());
            offset += USIZE_BYTES;

            let timestamp =
                u128::from_le_bytes(bytes[offset..offset + U128_BYTES].try_into().unwrap());
            offset += U128_BYTES;

            let key = String::from_utf8_lossy(&bytes[offset..offset + key_size]).into_owned();
            offset += key_size;

            let value = String::from_utf8_lossy(&bytes[offset..offset + value_size]).into_owned();
            offset += value_size;

            let entry = Entry {
                key_size,
                value_size,
                timestamp,
                key,
                value,
            };
            entries.push(entry);
        }
        Block { entries: entries }
    }

    pub fn to_bytes(&self) -> [u8; BLOCKSIZE] {
        let mut bytes = vec![];

        for entry in &self.entries {
            bytes.extend(entry.key_size.to_le_bytes());
            bytes.extend(entry.value_size.to_le_bytes().to_vec());
            bytes.extend(entry.timestamp.to_le_bytes().to_vec());
            bytes.extend(entry.key.as_bytes().to_vec());
            bytes.extend(entry.value.as_bytes().to_vec());
        }
        bytes.resize(BLOCKSIZE, 0);
        bytes.try_into().unwrap()
    }

    pub fn get_value(&self, key: &String) -> Option<&String> {
        let i = self.entries.binary_search_by_key(&key, |e| &e.key);
        match i {
            Ok(idx) => Some(&self.entries.get(idx).unwrap().value),
            Err(_) => None,
        }
    }
}

struct IndexEntry {
    key: String,
    block_index: usize,
}
struct IndexBlock {
    entries: Vec<IndexEntry>,
}

impl IndexBlock {
    fn get_block_index(&self, key: &String) -> usize {
        // index = |a|b|d|f, key = c  -> 1
        match self.entries.binary_search_by_key(&key, |e| &e.key) {
            Ok(v) => v,
            Err(v) => v - 1,
        }
    }
    fn new() -> IndexBlock {
        IndexBlock { entries: vec![] }
    }
}

struct SSTable {
    data_blocks: Vec<Block>,
    index_block: IndexBlock,
    crc: u32, // CRC = 32bit hash computed over the payload using CRC
}

impl SSTable {
    pub fn get_value(&self, key: &String) -> Option<&String> {
        let block_idx = self.index_block.get_block_index(key);
        self.data_blocks.get(block_idx).unwrap().get_value(key)
    }

    pub fn from_bytes(bytes: &[u8]) -> SSTable {
        let mut offset = 0;
        let input_size = bytes.len();
        let mut data_blocks = vec![];
        let mut index_block = IndexBlock::new();
        for block_idx in 0..(input_size / BLOCKSIZE) {
            let block = Block::from_bytes(bytes[offset..offset + BLOCKSIZE].try_into().unwrap());
            let min_key = block.entries.get(0).unwrap().key.to_string();
            data_blocks.push(block);
            offset += BLOCKSIZE;

            index_block.entries.push(IndexEntry {
                key: min_key,
                block_index: block_idx,
            })
        }

        SSTable {
            data_blocks,
            index_block,
            crc: 123,
        }
    }

    pub fn from_blocks(blocks: Vec<Block>) -> SSTable {
        let mut data_blocks = vec![];
        let mut index_block = IndexBlock::new();
        for (i, block) in blocks.into_iter().enumerate() {
            let bytes = block.to_bytes();
            data_blocks.push(Block::from_bytes(&bytes));
            let min_key = block.entries.get(0).unwrap().key.clone();
            index_block.entries.push(IndexEntry {
                key: min_key,
                block_index: i,
            })
        }

        SSTable {
            data_blocks,
            index_block,
            crc: 123,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn block_from_bytes() {
        let block = Block {
            entries: vec![
                Entry::new("a".to_owned(), "b".to_owned()),
                Entry::new("aa".to_owned(), "bb".to_owned()),
            ],
        };

        let bytes = block.to_bytes();

        let new_block = Block::from_bytes(&bytes);
        assert_eq!(block.entries, new_block.entries)
    }

    #[test]
    fn get_value_by_index() {
        let entry_a = Entry::new("a".to_owned(), "aa".to_owned());
        let entry_b = Entry::new("b".to_owned(), "bb".to_owned());

        let mut bytes = vec![];

        let mut block = Block { entries: vec![] };
        for i in 0..(BLOCKSIZE / size_of_val(&entry_a)) {
            block.entries.push(Entry::new(
                format!("{}{}", "a".to_owned(), i.to_string()),
                format!("{}{}", "aa".to_owned(), i.to_string()),
            ));
        }

        bytes.push(block);

        let mut block = Block { entries: vec![] };
        for i in 0..(BLOCKSIZE / size_of_val(&entry_b)) {
            block.entries.push(Entry::new(
                format!("{}{}", "b".to_owned(), i.to_string()),
                format!("{}{}", "bb".to_owned(), i.to_string()),
            ));
        }

        bytes.push(block);

        let new_sstable = SSTable::from_blocks(bytes);
        assert_eq!(
            Some(&"bb1".to_owned()),
            new_sstable.get_value(&"b1".to_owned())
        )
    }
}
