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

use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
    vec,
};

use super::record::Record;
const BLOCKSIZE: usize = 32000;
const USIZE_BYTES: usize = (usize::BITS / 8) as usize;
const U128_BYTES: usize = (u128::BITS / 8) as usize;

pub struct Block {
    records: Vec<Record>,
}
impl Block {
    pub fn from_bytes(bytes: &[u8; BLOCKSIZE]) -> Block {
        let mut offset = 0;
        let mut entries = vec![];
        while offset < BLOCKSIZE {
            if offset + USIZE_BYTES >= BLOCKSIZE {
                break;
            }

            let is_tombstone = bytes[offset..offset + 1][0];
            offset += 1;

            let key_size =
                usize::from_le_bytes(bytes[offset..offset + USIZE_BYTES].try_into().unwrap());
            offset += USIZE_BYTES;

            // we have reached the end of data in the block.
            // The rest is padded with /0 and will have key_size 0
            if key_size == 0 {
                break;
            }

            let timestamp =
                u128::from_le_bytes(bytes[offset..offset + U128_BYTES].try_into().unwrap());
            offset += U128_BYTES;

            let key = String::from_utf8_lossy(&bytes[offset..offset + key_size]).into_owned();
            offset += key_size;

            let entry = match is_tombstone {
                1 => Record::Tombstone { timestamp, key },
                0 => {
                    let value_size = usize::from_le_bytes(
                        bytes[offset..offset + USIZE_BYTES].try_into().unwrap(),
                    );
                    offset += USIZE_BYTES;

                    let value =
                        String::from_utf8_lossy(&bytes[offset..offset + value_size]).into_owned();
                    offset += value_size;
                    Record::Value {
                        timestamp,
                        key,
                        value,
                    }
                }
                _ => unreachable!(),
            };

            entries.push(entry);
        }
        Block { records: entries }
    }

    pub fn to_bytes(&self) -> [u8; BLOCKSIZE] {
        let mut bytes: Vec<u8> = self.records.iter().flat_map(|r| r.as_bytes()).collect();

        println!("len before = {}", bytes.len());
        // pad bytes with 0 to a fixed size of BLOCKSIZE
        bytes.resize(BLOCKSIZE, 0);
        println!("len = {}", bytes.len());

        bytes.try_into().unwrap()
    }

    pub fn get_value(&self, key: &String) -> Option<String> {
        let i = self.records.binary_search_by_key(key, |e| e.get_key());
        if let Ok(idx) = i {
            match &self.records.get(idx) {
                Some(Record::Value { value, .. }) => Some(value.clone()),
                Some(Record::Tombstone { .. }) => None,

                None => None,
            }
        } else {
            None
        }
    }
}

struct IndexEntry {
    key: String,
    offset: usize,
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
    fn get_block_offset(&self, key: &String) -> usize {
        // index = |a|b|d|f, key = c  -> 1
        match self.entries.binary_search_by_key(&key, |e| &e.key) {
            Ok(v) => self.entries.get(v).unwrap().offset,
            Err(v) if v == 0 => self.entries.get(v).unwrap().offset,
            Err(v) => self.entries.get(v - 1).unwrap().offset,
        }
    }
    fn new() -> IndexBlock {
        IndexBlock { entries: vec![] }
    }
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        for index in self.entries.iter() {
            bytes.extend(index.key.len().to_le_bytes().to_vec());
            bytes.extend(index.key.as_bytes().to_vec());
            bytes.extend(index.offset.to_le_bytes().to_vec());
            bytes.extend(index.block_index.to_le_bytes().to_vec());
        }
        bytes
    }
}

struct Footer {
    index_offset: usize,
    index_size: usize,
}

impl Footer {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(self.index_offset.to_le_bytes().to_vec());
        bytes.extend(self.index_size.to_le_bytes().to_vec());
        bytes
    }
}

pub struct SSTable {
    data_blocks: Vec<Block>,
    index_block: IndexBlock,
    footer: Footer,
}

impl SSTable {
    pub fn get_value(&self, key: &String) -> Option<String> {
        let block_idx = self.index_block.get_block_index(key);
        self.data_blocks.get(block_idx).unwrap().get_value(key)
    }

    pub fn from_records(records: Vec<Record>) -> Self {
        let mut offset = 0;
        let mut data_blocks = vec![];
        let mut current_block = 0;
        let mut block_records: Vec<Record> = vec![];
        let mut index_block = IndexBlock::new();

        for record in records {
            if offset + record.size() > BLOCKSIZE {
                let min_key = block_records.get(0).unwrap().get_key();
                index_block.entries.push(IndexEntry {
                    key: min_key,
                    offset: BLOCKSIZE * current_block,
                    block_index: current_block,
                });

                let block = Block {
                    records: block_records,
                };
                data_blocks.push(block);
                offset = 0;
                current_block += 1;
                block_records = vec![];
            }
            offset += record.size();
            block_records.push(record);
        }

        // dump remaining records into block, with index
        if !block_records.is_empty() {
            let min_key = block_records.get(0).unwrap().get_key();
            index_block.entries.push(IndexEntry {
                key: min_key,
                offset: BLOCKSIZE * current_block,
                block_index: current_block,
            });

            let block = Block {
                records: block_records,
            };
            data_blocks.push(block);
        }

        let footer = Footer {
            index_offset: data_blocks.len() * BLOCKSIZE,
            index_size: index_block
                .entries
                .iter()
                .map(|x| x.key.len() + 3 * USIZE_BYTES)
                .sum::<usize>(),
        };

        SSTable {
            data_blocks,
            index_block,
            footer,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        for block in self.data_blocks.iter() {
            bytes.extend(block.to_bytes());
        }
        bytes.extend(self.index_block.to_bytes());
        bytes.extend(self.footer.to_bytes());

        bytes
    }

    pub fn write(&self, path: &Path) -> Result<PathBuf, std::io::Error> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        create_dir_all(path)?;
        let path = Path::new(path).join(timestamp.to_string() + ".ss");

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut buf_writer = BufWriter::new(file);
        buf_writer.write_all(&self.to_bytes())?;
        Ok(path)
    }

    pub fn get_disk(
        input_key: &String,
        file_path: &Path,
    ) -> Result<Option<String>, std::io::Error> {
        let mut file = File::open(file_path).unwrap();
        file.seek(SeekFrom::End(-(2 * USIZE_BYTES as i64))).unwrap();

        let mut footer_buffer = [0; 2 * USIZE_BYTES];
        if let Err(err) = file.read_exact(&mut footer_buffer) {
            print!("{}", err);
        };
        let footer = Footer {
            index_offset: usize::from_le_bytes(footer_buffer[0..USIZE_BYTES].try_into().unwrap()),
            index_size: usize::from_le_bytes(
                footer_buffer[USIZE_BYTES..2 * USIZE_BYTES]
                    .try_into()
                    .unwrap(),
            ),
        };

        file.seek(SeekFrom::Start(footer.index_offset.try_into().unwrap()))?;

        let mut index_buffer = vec![0; footer.index_size];
        file.read_exact(&mut index_buffer)?;

        let mut index_entries = vec![];
        let mut index_offset = 0;
        while index_offset < footer.index_size {
            let key_size = usize::from_le_bytes(
                index_buffer[index_offset..index_offset + USIZE_BYTES]
                    .try_into()
                    .unwrap(),
            );
            index_offset += USIZE_BYTES;
            let key = String::from_utf8_lossy(
                index_buffer[index_offset..index_offset + key_size]
                    .try_into()
                    .unwrap(),
            )
            .to_string();
            index_offset += key_size;

            let offset = usize::from_le_bytes(
                index_buffer[index_offset..index_offset + USIZE_BYTES]
                    .try_into()
                    .unwrap(),
            );
            index_offset += USIZE_BYTES;

            let block_index = usize::from_le_bytes(
                index_buffer[index_offset..index_offset + USIZE_BYTES]
                    .try_into()
                    .unwrap(),
            );
            index_offset += USIZE_BYTES;

            index_entries.push(IndexEntry {
                key,
                offset,
                block_index,
            })
        }

        let index_block = IndexBlock {
            entries: index_entries,
        };

        let block_offset = index_block.get_block_offset(input_key);

        let mut block_buffer = [0; BLOCKSIZE];
        file.seek(SeekFrom::Start(block_offset.try_into().unwrap()))?;
        file.read_exact(&mut block_buffer)?;

        let block = Block::from_bytes(&block_buffer);
        // TODO: Result<Option<>> is kind of ugly
        Ok(block.get_value(input_key))
    }
}

#[cfg(test)]
mod test {
    use crate::storage::memtable::MemTable;

    use super::*;
    use std::{fs::remove_dir_all, mem::size_of_val};

    #[test]
    fn block_from_bytes() {
        let block = Block {
            records: vec![
                Record::new("a".to_owned(), "b".to_owned()),
                Record::new("aa".to_owned(), "bb".to_owned()),
            ],
        };

        let bytes = block.to_bytes();

        let new_block = Block::from_bytes(&bytes);
        assert_eq!(block.records, new_block.records)
    }

    #[test]
    fn get_value_by_index() {
        let entry_a = Record::new("a".to_owned(), "aa".to_owned());
        let entry_b = Record::new("b".to_owned(), "bb".to_owned());

        let mut records = vec![];

        for i in 0..(BLOCKSIZE / size_of_val(&entry_a)) {
            records.push(Record::new(
                format!("{}{}", "a".to_owned(), i),
                format!("{}{}", "aa".to_owned(), i),
            ));
        }

        for i in 0..(BLOCKSIZE / size_of_val(&entry_b)) {
            records.push(Record::new(
                format!("{}{}", "b".to_owned(), i),
                format!("{}{}", "bb".to_owned(), i),
            ));
        }

        let new_sstable = SSTable::from_records(records);
        assert_eq!(
            Some("bb1".to_owned()),
            new_sstable.get_value(&"b1".to_owned())
        )
    }

    #[test]
    fn from_memtable_bytes() {
        let mut mem_table = MemTable::new();
        for i in 0..(BLOCKSIZE / 4) {
            mem_table.set(
                format!("{}{}", "a".to_owned(), i),
                format!("{}{}", "aa".to_owned(), i),
            );
        }

        let new_sstable = SSTable::from_records(mem_table.to_records());
        assert_eq!(
            Some("aa3000".to_owned()),
            new_sstable.get_value(&"a3000".to_owned())
        );
    }

    #[test]
    fn get_from_disk() {
        let mut mem_table = MemTable::new();
        for i in 0..(BLOCKSIZE / 5) {
            mem_table.set(
                format!("{}{}", "a".to_owned(), i),
                format!("{}{}", "aa".to_owned(), i),
            );
        }

        let new_sstable = SSTable::from_records(mem_table.to_records());

        let path = Path::new("./tests/sstable/output/get_from_disk");
        let ss_path = new_sstable.write(path).unwrap();
        assert_eq!(
            SSTable::get_disk(&"a3000".to_owned(), &ss_path).unwrap(),
            Some("aa3000".to_owned())
        );

        assert_eq!(
            SSTable::get_disk(&"a4001".to_owned(), &ss_path).unwrap(),
            Some("aa4001".to_owned())
        );
        remove_dir_all(path).unwrap();
    }
}
