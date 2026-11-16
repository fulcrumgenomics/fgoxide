use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::mpsc::{Receiver, sync_channel};
use std::thread::{self, JoinHandle};
use std::vec::IntoIter;

// type aliased to get clippy to not think this is too complex
type PanicUnwindErr = Box<dyn Any + Send>;

/// Iterator extension that spawns an additional thread to read-ahead in the iterator. Sends
/// results back to this object via a channel and returns them in the same manner as a normal
/// iterator. This is useful in the context in which the reading of an iterator (or iterators) is
/// time consuming or computationally expensive and the consuming thread is bottlenecked
/// by the speed of the underlying iterator.
///
/// To use on an Iterator that is ``Send`` + ``'static`` , it is as simple as:
/// ```
/// use fgoxide::iter::IntoChunkedReadAheadIterator;
///
/// let v = vec![0,1,2,3,4,5,6,7];
/// let chunk_size = 5;
/// let chunk_queue_size = 5;
///
/// let mut chunked_iter = v.into_iter().read_ahead(chunk_size, chunk_queue_size);
/// assert_eq!(chunked_iter.next(), Some(0));
/// assert_eq!(chunked_iter.next(), Some(1));
/// assert_eq!(chunked_iter.next(), Some(2));
/// assert_eq!(chunked_iter.next(), Some(3));
/// assert_eq!(chunked_iter.next(), Some(4));
/// assert_eq!(chunked_iter.next(), Some(5));
/// assert_eq!(chunked_iter.next(), Some(6));
/// assert_eq!(chunked_iter.next(), Some(7));
/// assert_eq!(chunked_iter.next(), None);
/// ```
/// Where ``chunk_size`` is the number of elements in the iter to include per send / recieve over
/// the underlying channel, and ``chunk_queue_size`` is the maximum number of chunks to keep on the
/// channel at any given time (will block the thread until the space is freed up).
///
/// The chunked iterator can panic in the following circumstances:
///     - panics if the underlying iterator panics after the same number of ``next()`` calls.
///
/// Dropping the iterator before it is exhausted disconnects the channel and joins the read-ahead
/// thread, so the thread has stopped by the time ``drop`` returns. The thread only notices the
/// disconnect at its next send, so ``drop`` blocks while the underlying iterator produces up to one
/// more chunk; if the underlying iterator's ``next()`` blocks indefinitely (e.g. reading an
/// interactive stdin that never reaches EOF), ``drop`` blocks too. Panics raised by the read-ahead
/// thread after the iterator is dropped are discarded.
pub struct ChunkedReadAheadIterator<T: Send + 'static> {
    /// The recieving object that recieves chunks of ``T``. Only ``None`` once ``drop`` has taken
    /// it to disconnect the channel.
    receiver: Option<Receiver<Result<Vec<T>, PanicUnwindErr>>>,
    /// The handle to the thread that was spawned to read ahead on the iterator.
    join_handle: Option<JoinHandle<()>>,
    /// The most recent chunk recieved as an iterator. Used to produce owned ``T`` objects from
    /// the chunk
    current_chunk: IntoIter<T>,
}

impl<T> ChunkedReadAheadIterator<T>
where
    T: Send + 'static,
{
    /// Creates a new ``Self`` from an existing iterator. Takes two parameters that
    /// control the number of items stored in the read-ahead buffer.  `chunk_size`
    /// refers to how many items are transferred at a time from the read-ahead thread and
    /// `chunk_count` controls how many chunks are read ahead.
    ///
    /// # Panics
    /// - panics if the spawned thread fails to spawn
    pub fn new<I>(mut inner: I, chunk_size: usize, num_chunk_buffer_size: usize) -> Self
    where
        I: Iterator<Item = T> + Send + 'static,
    {
        assert_ne!(chunk_size, 0, "Chunk size cannot be zero!");
        assert_ne!(num_chunk_buffer_size, 0, "Number of buffered chunks cannot be zero!");

        // Create a channel over which we can send our chunks of ``T``
        let (sender, receiver) = sync_channel(num_chunk_buffer_size);

        // Create our spawned thread, holding on to the resulting handle for downstream error
        // management.
        let join_handle = thread::Builder::new()
            .name("chunked_read_ahead_thread".to_owned())
            .spawn(move || {
                'chunk_loop: loop {
                    let mut chunk = Vec::with_capacity(chunk_size);
                    let mut exhausted = false;
                    for _ in 0..chunk_size {
                        match catch_unwind(AssertUnwindSafe(|| inner.by_ref().next())) {
                            Ok(Some(val)) => chunk.push(val),
                            Ok(None) => {
                                exhausted = true;
                                break;
                            }
                            Err(e) => {
                                let _x = sender.send(Ok(chunk));
                                let _x = sender.send(Err(e));
                                break 'chunk_loop;
                            }
                        }
                    }
                    // Stop once the inner iterator is exhausted, without polling it again (its
                    // `next()` may panic after returning `None`), or once a send fails because
                    // the receiver has been dropped.
                    if (!chunk.is_empty() && sender.send(Ok(chunk)).is_err()) || exhausted {
                        break;
                    }
                }
            })
            .expect("failed to spawn chunked read ahead thread");

        // Store the necessary objects on ``Self``
        Self {
            receiver: Some(receiver),
            join_handle: Some(join_handle),
            current_chunk: Vec::new().into_iter(),
        }
    }
}

impl<T> Iterator for ChunkedReadAheadIterator<T>
where
    T: Send + 'static,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        loop {
            // Return the next item from the current chunk, if it has any left.
            if let Some(item) = self.current_chunk.next() {
                return Some(item);
            }
            // Otherwise grab a new chunk (note that ``recv`` is blocking, so this will only return
            // an error if the sender has been dropped and there are no more elements in the
            // channel.)  A new chunk may be empty, e.g. when the inner iterator panics on the first
            // item of a chunk, so keep receiving rather than treating it as the end of the stream.
            // If the new chunk is an Err, raise it to the main thread.
            let receiver = self.receiver.as_ref()?;
            if let Ok(chunk_or_panic) = receiver.recv() {
                match chunk_or_panic {
                    Ok(next_chunk) => self.current_chunk = next_chunk.into_iter(),
                    Err(e) => resume_unwind(e),
                }
            } else {
                // join handle is not ``Copy`` or ``Clone`` and we need ownership of it to be able
                // to join on it, hence the optional field and taking it off the iterator struct.
                if let Some(join_handle) = self.join_handle.take() {
                    if let Err(e) = join_handle.join() {
                        resume_unwind(e)
                    }
                }
                return None;
            }
        }
    }
}

impl<T> Drop for ChunkedReadAheadIterator<T>
where
    T: Send + 'static,
{
    fn drop(&mut self) {
        // Disconnect before joining: a thread blocked sending into a full channel only wakes once
        // the receiver is gone, so joining first would deadlock.
        drop(self.receiver.take());
        if let Some(join_handle) = self.join_handle.take() {
            // Re-raising a panic from ``drop`` aborts the process if already unwinding, so the
            // join result is discarded.
            let _ = join_handle.join();
        }
    }
}

/// Trait that implements ``read_ahead`` a method for converting ``self`` to a
/// ``ChunkedReadAheadIterator``.
#[allow(clippy::module_name_repetitions)]
pub trait IntoChunkedReadAheadIterator<T>
where
    T: Send + 'static,
{
    /// Converts a struct into a ``ChunkedReadAheadIterator``, with chunks of size `chunk_size`
    /// and a read ahead buffer of `num_chunk_buffer_size` chunks.
    fn read_ahead(
        self,
        chunk_size: usize,
        num_chunk_buffer_size: usize,
    ) -> ChunkedReadAheadIterator<T>
    where
        Self: Send + 'static;
}

impl<I, T> IntoChunkedReadAheadIterator<T> for I
where
    T: Send + 'static,
    I: Iterator<Item = T>,
{
    fn read_ahead(
        self,
        chunk_size: usize,
        num_chunk_buffer_size: usize,
    ) -> ChunkedReadAheadIterator<T>
    where
        Self: Send + 'static,
    {
        ChunkedReadAheadIterator::new(self, chunk_size, num_chunk_buffer_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::mem::drop;
    use std::panic;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    // use std::panic;
    use std::thread::sleep;
    use std::time::Duration;

    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_wrapping_empty_iter(#[case] chunk_size: usize) {
        let test_vec: Vec<usize> = Vec::new();

        let mut chunked_iter = test_vec.into_iter().read_ahead(chunk_size, 1);
        assert_eq!(chunked_iter.next(), None);
    }

    #[rstest]
    #[case(1, 1)] // smallest possible
    #[case(2, 1)]
    #[case(4, 1)]
    #[case(8, 1)]
    #[case(16, 1)]
    #[case(1, 2)]
    #[case(2, 2)]
    #[case(4, 2)]
    #[case(8, 2)]
    #[case(16, 2)]
    #[case(1, 16)]
    #[case(2, 16)]
    #[case(4, 16)]
    #[case(8, 16)]
    #[case(16, 16)]
    #[case(1, 100)]
    #[case(2, 100)]
    #[case(4, 100)]
    #[case(8, 100)]
    #[case(16, 100)]
    fn test_handle_large_iterator_and_low_chunk_size(
        #[case] chunk_size: usize,
        #[case] buffer_size: usize,
    ) {
        let test_vec: Vec<usize> = (0..1_000_000).collect();
        let test_vec2 = test_vec.clone();

        let mut regular_iter = test_vec.into_iter();
        let mut chunked_iter = test_vec2.into_iter().read_ahead(chunk_size, buffer_size);

        loop {
            let i = regular_iter.next();
            let j = chunked_iter.next();
            assert_eq!(i, j);
            if i.is_none() {
                assert!(j.is_none());
                break;
            }
        }
    }

    #[test]
    fn test_low_bound_on_channel_for_blocking() {
        let chunked_iter = (0..100_000).read_ahead(8, 1);
        for i in chunked_iter {
            // Do some work so iter will get consumed
            let _ = i % 2;
        }
    }

    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_dropping_before_doesnt_explode(#[case] chunk_size: usize) {
        let test_vec = vec![0usize, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let chunked_iter = test_vec.into_iter().read_ahead(chunk_size, 1);
        sleep(Duration::from_millis(10));
        drop(chunked_iter);
    }

    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_dropping_half_used_iterator_doesnt_explode(#[case] chunk_size: usize) {
        let test_vec = vec![0usize, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let mut chunked_iter = test_vec.into_iter().read_ahead(chunk_size, 1);
        for _ in 0..4 {
            chunked_iter.next();
        }
        drop(chunked_iter);
    }

    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_dropping_fully_used_iterator_doesnt_explode(#[case] chunk_size: usize) {
        let test_vec = vec![0usize, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let mut chunked_iter = test_vec.clone().into_iter().read_ahead(chunk_size, 1);
        // need to do it this way so we don't lose ownership
        for _ in 0..test_vec.len() {
            chunked_iter.next();
        }
        drop(chunked_iter);
    }

    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_read_ahead_results_in_same_results_as_regular_iter(#[case] chunk_size: usize) {
        let test_vec = vec![0usize, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let mut regular_iter = test_vec.clone().into_iter();
        let mut chunked_iter = test_vec.into_iter().read_ahead(chunk_size, 1);

        loop {
            let i = regular_iter.next();
            let j = chunked_iter.next();
            assert_eq!(i, j);
            if i.is_none() {
                assert!(j.is_none());
                break;
            }
        }
    }

    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_read_past_end(#[case] chunk_size: usize) {
        let mut test_iter =
            vec![0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter().read_ahead(chunk_size, 1);
        for i in 0..20 {
            let v = test_iter.next();
            if i < 10 {
                assert_eq!(v, Some(i));
            } else {
                assert_eq!(v, None);
            }
        }
    }

    /// Number of iterations into iteration at which the ``FailingIter`` should fail.
    const FAIL_POINT: usize = 6;

    /// Iterator struct that fails after ``FAIL_POINT`` + 1 iterations with a panic.
    /// Text on the panic is "expected error message"
    struct FailingIter {
        counter: usize,
    }

    impl FailingIter {
        fn new() -> Self {
            Self { counter: 0 }
        }
    }

    impl Iterator for FailingIter {
        type Item = usize;

        fn next(&mut self) -> Option<Self::Item> {
            assert!(self.counter < FAIL_POINT, "expected error message");
            let current = self.counter;
            self.counter += 1;

            Some(current)
        }
    }

    #[test]
    #[should_panic(expected = "expected error message")]
    fn test_panic_occurring_mid_chunk_returns_results_until_panic() {
        let mut test_iter = FailingIter::new().read_ahead(8, 1);

        for _ in 0..FAIL_POINT {
            panic::catch_unwind(AssertUnwindSafe(|| {
                test_iter.next();
            }))
            .expect("different error message");
        }
        test_iter.next();
    }

    /// A panic on the first item of a chunk is raised after the preceding items, rather than
    /// ending the iteration early.
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(6)]
    fn test_panic_occurring_at_chunk_start_raises(#[case] chunk_size: usize) {
        assert_eq!(FAIL_POINT % chunk_size, 0);
        let mut test_iter = FailingIter::new().read_ahead(chunk_size, 1);
        for expected in 0..FAIL_POINT {
            assert_eq!(test_iter.next(), Some(expected));
        }
        let result = panic::catch_unwind(AssertUnwindSafe(|| test_iter.next()));
        let message = *result.unwrap_err().downcast::<&str>().unwrap();
        assert_eq!(message, "expected error message");
    }

    /// Iterator that yields `0..len`, returns `None` once, and panics if polled again.
    struct PanicAfterNoneIter {
        next: usize,
        len: usize,
        exhausted: bool,
    }

    impl Iterator for PanicAfterNoneIter {
        type Item = usize;

        fn next(&mut self) -> Option<Self::Item> {
            assert!(!self.exhausted, "polled after returning None");
            if self.next == self.len {
                self.exhausted = true;
                return None;
            }
            self.next += 1;
            Some(self.next - 1)
        }
    }

    /// The inner iterator is not polled again after it returns `None`, whether or not its length
    /// is a multiple of the chunk size.
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(8)]
    fn test_inner_iterator_is_not_polled_after_none(#[case] chunk_size: usize) {
        let inner = PanicAfterNoneIter { next: 0, len: 5, exhausted: false };
        let items: Vec<usize> = inner.read_ahead(chunk_size, 1).collect();
        assert_eq!(items, vec![0, 1, 2, 3, 4]);
    }

    /// Iterator struct that fails with a panic upon being dropped.
    struct ExitFailingIter {
        counter: usize,
    }

    impl ExitFailingIter {
        fn new() -> Self {
            Self { counter: 0 }
        }
    }

    impl Iterator for ExitFailingIter {
        type Item = usize;

        fn next(&mut self) -> Option<Self::Item> {
            if self.counter < FAIL_POINT {
                let current = self.counter;
                self.counter += 1;
                Some(current)
            } else {
                None
            }
        }
    }
    impl Drop for ExitFailingIter {
        fn drop(&mut self) {
            panic!("expected error message")
        }
    }

    #[test]
    #[should_panic(expected = "expected error message")]
    fn test_panic_occurring_after_iteration_raises() {
        {
            let mut test_iter = ExitFailingIter::new().read_ahead(8, 1);

            for _ in 0..FAIL_POINT {
                panic::catch_unwind(AssertUnwindSafe(|| {
                    test_iter.next();
                }))
                .expect("different error message");
            }
            assert_eq!(test_iter.next(), None);
        }
    }

    /// Infinite iterator that counts the items it has produced and sets a flag when dropped. The
    /// read-ahead thread owns the inner iterator, so the flag being set means the thread has
    /// finished with it.
    struct DropFlaggingIter {
        produced: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    impl DropFlaggingIter {
        fn new() -> Self {
            Self {
                produced: Arc::new(AtomicUsize::new(0)),
                dropped: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl Iterator for DropFlaggingIter {
        type Item = usize;

        fn next(&mut self) -> Option<Self::Item> {
            Some(self.produced.fetch_add(1, Ordering::SeqCst))
        }
    }

    impl Drop for DropFlaggingIter {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_dropping_mid_stream_stops_reader_thread_before_drop_returns() {
        let inner = DropFlaggingIter::new();
        let dropped = Arc::clone(&inner.dropped);
        let mut chunked_iter = inner.read_ahead(4, 2);
        for i in 0..10 {
            assert_eq!(chunked_iter.next(), Some(i));
        }

        drop(chunked_iter);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn test_dropping_with_full_channel_stops_reader_thread_before_drop_returns() {
        let inner = DropFlaggingIter::new();
        let produced = Arc::clone(&inner.produced);
        let dropped = Arc::clone(&inner.dropped);
        let chunked_iter = inner.read_ahead(1, 1);

        // With one single-item chunk buffered, producing a second item means the reader thread
        // is about to block sending into the full channel.
        while produced.load(Ordering::SeqCst) < 2 {
            thread::yield_now();
        }

        drop(chunked_iter);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn test_dropping_unconsumed_iterator_discards_reader_thread_panic() {
        let chunked_iter = ExitFailingIter::new().read_ahead(8, 1);
        drop(chunked_iter);
    }
}
