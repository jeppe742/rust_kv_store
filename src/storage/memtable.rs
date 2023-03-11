use std::time::{SystemTime, UNIX_EPOCH};

use rbtree::RBTree;

pub struct MemTable<K: Ord, V> {
    _storage: RBTree<K, V>,
}

impl<K: Ord, V: Clone> MemTable<K, V> {
    pub fn new() -> MemTable<K, V> {
        MemTable {
            _storage: RBTree::new(),
        }
    }

    pub fn set(&mut self, key: K, value: V) {
        self._storage.insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<V> {
        match self._storage.get(key) {
            None => None,
            Some(value) => Some(value.to_owned()),
        }
    }
}
const BLOCKSIZE: usize = 32000;
impl MemTable<String, String> {
    pub fn to_bytes_padded(&self) -> Vec<u8> {
        let mut bytes = vec![];
        let mut buffer_size = 0;
        for (key, value) in self._storage.iter() {
            if buffer_size + key.len() + value.len() + 2 * 8 + 16 > BLOCKSIZE {
                let padding = vec![0; BLOCKSIZE - buffer_size];
                bytes.extend(padding);
                buffer_size = 0;
            }

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            bytes.extend(key.len().to_le_bytes().to_vec());
            bytes.extend(value.len().to_le_bytes().to_vec());
            bytes.extend(timestamp.to_le_bytes().to_vec());
            bytes.extend(key.as_bytes().to_vec());
            bytes.extend(value.as_bytes().to_vec());

            buffer_size += key.len() + value.len() + 2 * 8 + 16;
        }
        bytes
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_int() {
        let mut mem_table = MemTable::new();
        mem_table.set(1, 1);
        assert_eq!(mem_table.get(&1), Some(1));
    }

    #[test]
    fn test_get_int_none() {
        let mut mem_table = MemTable::new();
        mem_table.set(1, 1);
        assert_eq!(mem_table.get(&2), None);
    }

    #[test]
    fn test_get_str() {
        let mut mem_table = MemTable::new();
        mem_table.set("a", "a");
        assert_eq!(mem_table.get(&"a"), Some("a"));
    }

    #[test]
    fn test_get_str_none() {
        let mut mem_table = MemTable::new();
        mem_table.set("a", "a");
        assert_eq!(mem_table.get(&"b"), None);
    }
}
