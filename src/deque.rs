use std::sync::atomic::{AtomicUsize, AtomicPtr, Ordering};
use std::ptr::null_mut;
use std::mem::MaybeUninit;

const CAPACITY: usize = 16;

pub struct Deque<T> {
    buffer: Box<[AtomicPtr<MaybeUninit<T>>]>,
    head: AtomicUsize,
    tail: AtomicUsize,
    capacity: AtomicUsize,
}

impl<T> Deque<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        if capacity == 0 {
            panic!("Deque capacity must be greater than 0");
        }

        let mut buffer: Vec<AtomicPtr<MaybeUninit<T>>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(AtomicPtr::default());
        }
        let buffer = buffer.into_boxed_slice();

        Deque {
            buffer,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            capacity: AtomicUsize::new(capacity),
        }
    }

    pub fn new() -> Self {
        let mut buffer: Vec<AtomicPtr<MaybeUninit<T>>> = Vec::with_capacity(CAPACITY);
        for _ in 0..CAPACITY {
            buffer.push(AtomicPtr::default());
        }
        let buffer = buffer.into_boxed_slice();

        Deque {
            buffer,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            capacity: AtomicUsize::new(CAPACITY),
        }
    }

    pub fn push_back(&self, mut item: T) -> Result<(), T> {
        loop {
            let tail = self.tail.load(Ordering::SeqCst);
            let head = self.head.load(Ordering::SeqCst);

            let next_tail = (tail + 1) % self.capacity.load(Ordering::SeqCst);
            if next_tail == head {
                // Buffer is full
                return Err(item);
            }

            let boxed_item = Box::new(MaybeUninit::new(item));
            let ptr = Box::into_raw(boxed_item);

            // CAS operation to ensure atomic operation

            // CAS operation to ensure atomic operation
            let cas_result = self.buffer[tail].compare_exchange(null_mut(), ptr, Ordering::AcqRel, Ordering::Relaxed);
            if cas_result.is_ok() {
                self.tail.store(next_tail, Ordering::Release);
                return Ok(());
            } else {
                // CAS failed, clean up and retry
                item = unsafe { (*Box::from_raw(ptr)).assume_init() }; // Safely recover item for retry
            }
        }
    }

    pub fn pop_back(&self) -> Option<T> {
        loop {
            let tail = self.tail.load(Ordering::SeqCst);
            let head = self.head.load(Ordering::SeqCst);

            if tail == head {
                // Buffer is empty
                return None;
            }

            let capacity = self.capacity.load(Ordering::SeqCst);
            let prev_tail = (tail + capacity - 1) % capacity;

            let ptr = self.buffer[prev_tail].swap(null_mut(), Ordering::AcqRel);
            if ptr.is_null() {
                // Swap failed, no item found
                continue;
            }

            self.tail.store(prev_tail, Ordering::Release);

            // Convert raw pointer back to Box<MaybeUninit<T>> and retrieve value
            unsafe {
                let boxed = Box::from_raw(ptr);
                return Some(*boxed.assume_init());
            }
        }
    }
}

// Destructor to clean up any remaining elements
impl<T> Drop for Deque<T> {
    fn drop(&mut self) {
        while let Some(_) = self.pop_back() {}
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::*;

    #[test]
    fn test_deque() {
        let deque = Deque::new();
        assert_eq!(deque.pop_back(), None);

        deque.push_back(1).unwrap();
        deque.push_back(2).unwrap();
        deque.push_back(3).unwrap();
        assert_eq!(deque.pop_back(), Some(3));
        assert_eq!(deque.pop_back(), Some(2));
        assert_eq!(deque.pop_back(), Some(1));
        assert_eq!(deque.pop_back(), None);
    }

    #[test]
    fn test_deque_threaded() {
        let capacity: usize = 5001;
        let deque = Arc::new(Deque::with_capacity(capacity));
        let mut handles = vec![];

        for i in 0..5 {
            let deque = deque.clone();
            handles.push(std::thread::spawn(move || {
                for y in 0..1000 {
                    assert_eq!(deque.push_back(y), Ok(()));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut handles = vec![];
        for _ in 0..5 {
            let deque = deque.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    assert!(deque.pop_back().is_some());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(deque.pop_back().is_none());
    }

    #[test]
    fn test_deque_capacity() {
        let deque = Deque::with_capacity(3);
        assert_eq!(deque.pop_back(), None);

        deque.push_back(1).unwrap();
        deque.push_back(2).unwrap();
        assert_eq!(deque.push_back(3), Err(3));
        assert_eq!(deque.pop_back(), Some(2));
        assert_eq!(deque.pop_back(), Some(1));
        assert_eq!(deque.pop_back(), None);

        drop(deque); // Ensure destructor cleans up remaining elements

        let deque = Deque::with_capacity(1001);
        for i in 0..1000 {
            assert_eq!(deque.push_back(i), Ok(()));
        }
        assert_eq!(deque.push_back(1000), Err(1000));
        for i in (0..1000).rev() {
            assert_eq!(deque.pop_back(), Some(i));
        }
        assert_eq!(deque.pop_back(), None);
    }
}
