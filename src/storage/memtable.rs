#![allow(dead_code)]

use std::collections::BTreeMap;

use super::record::Record;

pub struct MemTable {
    _storage: BTreeMap<String, Record>,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            _storage: BTreeMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self._storage.insert(key.clone(), Record::new(key, value));
    }

    pub fn get(&self, key: &String) -> Option<String> {
        match self._storage.get(key) {
            None => None,
            Some(Record::Value { value, .. }) => Some(value.to_string()),
            Some(Record::Tombstone { .. }) => None,
        }
    }

    pub fn delete(&mut self, key: String) {
        self._storage
            .insert(key.clone(), Record::new_tombstone(key));
    }

    pub fn len(&self) -> usize {
        self._storage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_records(&self) -> Vec<Record> {
        self._storage.values().map(|v| (*v).clone()).collect()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get() {
        let mut mem_table = MemTable::new();
        mem_table.set("a".to_string(), "a".to_string());
        assert_eq!(mem_table.get(&"a".to_string()), Some("a".to_string()));
    }

    #[test]
    fn test_get_none() {
        let mut mem_table = MemTable::new();
        mem_table.set("a".to_string(), "a".to_string());
        assert_eq!(mem_table.get(&"b".to_string()), None);
    }

    #[test]
    fn test_delete() {
        let mut mem_table = MemTable::new();
        mem_table.set("a".to_string(), "a".to_string());
        assert_eq!(mem_table.get(&"a".to_string()), Some("a".to_string()));

        mem_table.delete("a".to_string());
        assert_eq!(mem_table.get(&"a".to_string()), None);
    }
}
