use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

pub(crate) struct UniqueQueue<T> {
    queue: VecDeque<T>,
    set: HashSet<T>,
}

impl<T: Eq + Hash + Clone> UniqueQueue<T> {
    pub fn new() -> Self {
        UniqueQueue {
            queue: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    fn from(arr: Vec<T>) -> Self {
        let mut set = HashSet::new();
        let queue = arr
            .into_iter()
            .filter(|item| set.insert(item.clone()))
            .collect();
        UniqueQueue { queue, set }
    }

    fn drain(&mut self) -> Vec<T> {
        self.queue.drain(..).collect()
    }

    pub(crate) fn drain_batch(&mut self, batch_size: usize) -> Vec<T> {
        let drain_until = batch_size.min(self.queue.len());
        self.queue.drain(..drain_until).collect()
    }

    pub(crate) fn pop_front(&mut self) -> Option<T> {
        if let Some(item) = self.queue.pop_front() {
            self.set.remove(&item);
            return Some(item);
        }
        None
    }

    pub(crate) fn push_back(&mut self, item: T) {
        if self.set.insert(item.clone()) {
            self.queue.push_back(item);
        }
    }

    // Add an item to the queue if it's not already present
    pub(crate) fn enqueue(&mut self, item: T) {
        if self.set.insert(item.clone()) {
            self.queue.push_back(item);
        }
    }

    // Remove and return the first item from the queue, if any
    pub(crate) fn dequeue(&mut self) -> Option<T> {
        if let Some(item) = self.queue.pop_front() {
            self.set.remove(&item);
            return Some(item);
        }
        None
    }

    // Peek at the first item in the queue without removing it, if any
    fn peek(&self) -> Option<&T> {
        self.queue.front()
    }

    // Check if the queue is empty
    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}
