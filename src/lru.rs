use std::collections::HashMap;
use std::ptr;

struct LruEntry<K, V> {
    key: K,
    value: V,
    prev: *mut LruEntry<K, V>,
    next: *mut LruEntry<K, V>,
}

pub struct LruCache<K, V> {
    map: HashMap<K, Box<LruEntry<K, V>>>,
    capacity: usize,
    head: *mut LruEntry<K, V>,
    tail: *mut LruEntry<K, V>,
}

impl<K: Eq + std::hash::Hash + Clone, V> LruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            capacity,
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(entry) = self.map.get_mut(key) {
            let entry_ptr: *mut _ = &mut **entry;
            self.detach(entry_ptr);
            self.attach(entry_ptr);
            return Some(unsafe { &(*entry_ptr).value });
        }
        None
    }

    pub fn put(&mut self, key: K, value: V) {
        if let Some(entry) = self.map.get_mut(&key) {
            entry.value = value;
            let entry_ptr: *mut _ = &mut **entry;
            self.detach(entry_ptr);
            self.attach(entry_ptr);
        } else {
            if self.map.len() == self.capacity {
                let old_tail = self.tail;
                self.detach(old_tail);
                let old_key = unsafe { (*old_tail).key.clone() };
                self.map.remove(&old_key);
            }
            let mut entry = Box::new(LruEntry {
                key: key.clone(),
                value,
                prev: ptr::null_mut(),
                next: ptr::null_mut(),
            });
            let entry_ptr: *mut _ = &mut *entry;
            self.attach(entry_ptr);
            self.map.insert(key, entry);
        }
    }

    fn detach(&mut self, entry: *mut LruEntry<K, V>) {
        unsafe {
            if !(*entry).prev.is_null() {
                (*(*entry).prev).next = (*entry).next;
            } else {
                self.head = (*entry).next;
            }

            if !(*entry).next.is_null() {
                (*(*entry).next).prev = (*entry).prev;
            } else {
                self.tail = (*entry).prev;
            }
        }
    }

    fn attach(&mut self, entry: *mut LruEntry<K, V>) {
        unsafe {
            (*entry).next = self.head;
            (*entry).prev = ptr::null_mut();

            if !self.head.is_null() {
                (*self.head).prev = entry;
            }

            self.head = entry;

            if self.tail.is_null() {
                self.tail = entry;
            }
        }
    }
}
