use rbtree::RBTree;

pub struct MemTable<K: Ord, V> {
    _storage: RBTree<K, V>,
}

impl<K: Ord, V> MemTable<K, V> {
    pub fn new() -> MemTable<K, V> {
        MemTable {
            _storage: RBTree::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self._storage.insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        match self._storage.get(key) {
            None => None,
            Some(value) => Some(value),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_int() {
        let mut mem_table = MemTable::new();
        mem_table.insert(1, 1);
        assert_eq!(mem_table.get(&1), Some(&1));
    }

    #[test]
    fn test_get_int_none() {
        let mut mem_table = MemTable::new();
        mem_table.insert(1, 1);
        assert_eq!(mem_table.get(&2), None);
    }

    #[test]
    fn test_get_str() {
        let mut mem_table = MemTable::new();
        mem_table.insert("a", "a");
        assert_eq!(mem_table.get(&"a"), Some(&"a"));
    }

    #[test]
    fn test_get_str_none() {
        let mut mem_table = MemTable::new();
        mem_table.insert("a", "a");
        assert_eq!(mem_table.get(&"b"), None);
    }
}
