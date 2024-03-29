use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    marker::PhantomData,
    sync::{
        atomic::{AtomicPtr, AtomicUsize, Ordering},
        Arc,
    },
};

pub struct Iter<T: Copy + 'static> {
    _guard: Arc<DropGuard>,
    inner: core::slice::Iter<'static, T>,
    next: *const Segment,
}

unsafe impl<T: Send + Copy + 'static> Send for Iter<T> {}
unsafe impl<T: Sync + Copy + 'static> Sync for Iter<T> {}

impl<T: Copy + 'static> Iterator for Iter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(v) = self.inner.next() {
            return Some(*v);
        }

        if self.next.is_null() {
            return None;
        }

        unsafe {
            let r = &*self.next;
            let s = core::slice::from_raw_parts(r.values as *const T, r.len.load(Ordering::SeqCst));
            self.inner = s.iter();
            let r = &*self.next;
            self.next = r.next.load(Ordering::SeqCst);
        }

        self.inner.next().cloned()
    }
}

#[derive(Clone)]
pub struct Reader<T: Copy> {
    _guard: Arc<DropGuard>,
    first_segment: *const Segment,
    segment_length: usize,
    _phantom: PhantomData<T>,
}

unsafe impl<T: Sync + Copy + 'static> Sync for Reader<T> {}
unsafe impl<T: Send + Copy + 'static> Send for Reader<T> {}

impl<T: Copy> Reader<T> {
    pub fn last(&self) -> Option<T> {
        let mut segment = self.first_segment;

        loop {
            unsafe {
                let r = &*segment;
                let next = r.next.load(Ordering::SeqCst);

                if next.is_null() {
                    let inner = core::slice::from_raw_parts(
                        r.values as *const T,
                        r.len.load(Ordering::SeqCst),
                    );
                    return inner.last().copied();
                }

                segment = next;
            }
        }
    }

    pub fn iter_from(&self, index: usize) -> Iter<T> {
        let mut segment = self.first_segment;
        let mut offset = index;
        for _ in 0..index / self.segment_length {
            unsafe {
                let r = &*segment;
                segment = r.next.load(Ordering::SeqCst);
            }

            if segment.is_null() {
                return Iter {
                    _guard: self._guard.clone(),
                    inner: [].iter(),
                    next: core::ptr::null(),
                };
            }

            offset -= self.segment_length;
        }

        unsafe {
            let segment = &*segment;
            let len = segment.len.load(Ordering::SeqCst);
            let offset = core::cmp::min(offset, len);
            let inner =
                core::slice::from_raw_parts((segment.values as *const T).add(offset), len - offset);
            let inner = inner.iter();

            Iter {
                _guard: self._guard.clone(),
                inner,
                next: segment.next.load(Ordering::SeqCst),
            }
        }
    }
}

impl<T: Copy + PartialOrd + Ord> Reader<T> {
    /// Returns the position the first key that is greater than the given key
    pub fn next_position(&self, key: T) -> Option<usize> {
        unsafe {
            let mut segment = self.first_segment;
            let mut offset = 0;
            while !segment.is_null() {
                let r = &*segment;
                let values = core::slice::from_raw_parts::<T>(
                    r.values as *const T,
                    r.len.load(Ordering::SeqCst),
                );

                let last = *values.last()?;
                if last <= key {
                    segment = r.next.load(Ordering::SeqCst);
                    offset += self.segment_length;
                } else {
                    // TODO: can implement this more efficiently by
                    // implementing a binary search that does this.
                    // Stdlib implementation of binary search doesn't do this
                    for (pos, &val) in values.iter().enumerate() {
                        if val > key {
                            return Some(pos + offset);
                        }
                    }

                    return None;
                }
            }

            None
        }
    }

    pub fn position(&self, key: T) -> Option<usize> {
        unsafe {
            let mut segment = self.first_segment;
            let mut offset = 0;
            while !segment.is_null() {
                let r = &*segment;
                let values = core::slice::from_raw_parts::<T>(
                    r.values as *const T,
                    r.len.load(Ordering::SeqCst),
                );

                let last = *values.last()?;
                if last < key {
                    segment = r.next.load(Ordering::SeqCst);
                    offset += self.segment_length;
                } else {
                    let first = *values.get_unchecked(0);
                    if key < first {
                        return None;
                    }
                    return values.binary_search(&key).ok().map(|p| p + offset);
                }
            }

            None
        }
    }
}

pub struct Writer<T: Copy> {
    _guard: Arc<DropGuard>,
    segment_length: usize,
    tip_segment: *mut Segment,
    segment_layout: Layout,
    _phantom: PhantomData<T>,
}

unsafe impl<T: Sync + Copy + 'static> Sync for Writer<T> {}
unsafe impl<T: Send + Copy + 'static> Send for Writer<T> {}

impl<T: Copy> Writer<T> {
    pub fn append(&mut self, values: &[T]) {
        unsafe {
            let mut src = values.as_ptr();
            let mut segment = &mut *self.tip_segment;
            let mut src_len = values.len();
            while src_len > 0 {
                let segment_len = segment.len.load(Ordering::SeqCst);
                if segment_len < self.segment_length {
                    let to_copy = core::cmp::min(src_len, self.segment_length - segment_len);
                    let dst = segment.values as *mut T;
                    core::ptr::copy_nonoverlapping(src, dst.add(segment_len), to_copy);
                    src = src.add(to_copy);
                    src_len -= to_copy;
                    segment.len.fetch_add(to_copy, Ordering::SeqCst);
                } else {
                    let next = segment.next.load(Ordering::SeqCst);
                    if !next.is_null() {
                        segment = &mut *next;
                    } else {
                        let new_segment = Segment::alloc(self.segment_layout);
                        segment.next.store(new_segment, Ordering::SeqCst);
                        segment = &mut *new_segment;
                    }
                }
            }
        }
    }
}

struct Segment {
    values: *mut u8,
    len: AtomicUsize,
    next: AtomicPtr<Segment>,
}

unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}

impl Segment {
    unsafe fn alloc(layout: Layout) -> *mut Self {
        let values = alloc_zeroed(layout);

        let segment = Self {
            values,
            len: AtomicUsize::new(0),
            next: AtomicPtr::new(core::ptr::null::<Segment>() as *mut _),
        };

        Box::leak(Box::new(segment))
    }

    unsafe fn dealloc(&self, layout: Layout) {
        dealloc(self.values, layout);
    }
}

pub fn new<T: Copy>(segment_length: usize) -> (Writer<T>, Reader<T>) {
    assert!(segment_length > 0);
    assert!(core::mem::align_of::<T>() < ALIGNMENT);
    assert!(ALIGNMENT % core::mem::align_of::<T>() == 0);

    let segment_layout =
        Layout::from_size_align(segment_length * core::mem::size_of::<T>(), ALIGNMENT)
            .unwrap()
            .pad_to_align();

    unsafe {
        let first_segment = Segment::alloc(segment_layout);

        let guard = Arc::new(DropGuard {
            first_segment,
            segment_layout,
        });

        let writer = Writer {
            _guard: guard.clone(),
            segment_length,
            tip_segment: first_segment,
            segment_layout,
            _phantom: PhantomData,
        };

        let reader = Reader {
            _guard: guard,
            first_segment,
            segment_length,
            _phantom: PhantomData,
        };

        (writer, reader)
    }
}

struct DropGuard {
    first_segment: *mut Segment,
    segment_layout: Layout,
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        unsafe {
            let mut segment = self.first_segment;
            while !segment.is_null() {
                let r = &mut *segment;
                r.dealloc(self.segment_layout);
                segment = r.next.load(Ordering::SeqCst);
                core::mem::drop(Box::from_raw(r));
            }
        }
    }
}

unsafe impl Send for DropGuard {}
unsafe impl Sync for DropGuard {}

const ALIGNMENT: usize = 64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_position() {
        let (mut writer, reader) = new::<usize>(3);

        let values = (0..40).step_by(13).collect::<Vec<usize>>();
        writer.append(&values);
        // 0, 13, 26, 39

        assert_eq!(reader.next_position(0).unwrap(), 1);
        assert_eq!(reader.next_position(8).unwrap(), 1);
        assert_eq!(reader.next_position(12).unwrap(), 1);
        assert_eq!(reader.next_position(13).unwrap(), 2);
        assert_eq!(reader.next_position(22).unwrap(), 2);
        assert_eq!(reader.next_position(23).unwrap(), 2);
        assert_eq!(reader.next_position(26).unwrap(), 3);
        assert_eq!(reader.next_position(30).unwrap(), 3);
        assert!(reader.next_position(39).is_none());
        assert!(reader.next_position(40).is_none());
    }

    #[test]
    fn smoke() {
        let (mut writer, reader) = new::<usize>(3);

        let values = (0..1131).collect::<Vec<usize>>();
        writer.append(&values);

        for val in values {
            assert_eq!(reader.position(val), Some(val), "{} failed", val);
        }

        for (i, val) in reader.iter_from(4).enumerate() {
            assert_eq!(val, i + 4, "failed at {}", i);
        }
    }

    #[test]
    fn smoke_one_by_one() {
        let (mut writer, reader) = new::<usize>(33);

        assert_eq!(reader.last(), None);

        let values = (0..1131).collect::<Vec<usize>>();

        for &val in values.iter() {
            writer.append(&[val]);
            assert_eq!(reader.position(val), Some(val), "{} failed", val);
            assert_eq!(reader.last(), Some(val));
        }

        for (i, val) in reader.iter_from(33).enumerate() {
            assert_eq!(val, i + 33);
        }
    }

    #[test]
    fn smoke_by_chunks() {
        let (mut writer, reader) = new::<usize>(13);

        let values = (0..1131).collect::<Vec<usize>>();

        for chunk in values.chunks(13) {
            writer.append(chunk);
        }

        for val in values {
            assert_eq!(reader.position(val), Some(val), "{} failed", val);
        }

        for (i, val) in reader.iter_from(31).enumerate() {
            assert_eq!(val, i + 31);
        }
    }
}
