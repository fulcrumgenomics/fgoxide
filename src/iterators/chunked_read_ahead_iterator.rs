use std::any::Any;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread;
use std::vec::IntoIter;

// type aliased to get clippy to not think this is too complex
type PanicUnwindErr = Box<dyn Any + Send>;

/// Iterator extension that spawns an additional thread to read-ahead in the iterator. Sends
/// results back to this object via a channel and returns them in the same manner as a normal
/// iterator. This is useful in the context in which the reading of an iterator (or iterators) is
/// time consuming (e.g. reading from a compressed FASTQ file) and the main thread is bottlenecked
/// by the speed of the underlying iterator.
///
/// To use on a struct that implements ``IntoIter``, it is as simple as:
/// ```
/// use fgoxide::iterators::chunked_read_ahead_iterator::IntoChunkedReadAheadIterator;
///
/// let v = vec![0,1,2,3,4,5,6,7];
/// let chunk_size = 5;
/// let buffer_size = 5;
///
/// let mut chunked_iter = v.into_iter().read_ahead(chunk_size, buffer_size);
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
/// the underlying channel, and ``buffer_size`` is the maximum number of chunks to keep on the
/// channel at any given time (will block the thread until the space is freed up).
///
/// If your struct does not implement ``IntoIter``, you can either `impl`
/// ``IntoChunkedReadAheadIterator`` manually or `impl` ``IntoIter`` manually and use the auto
/// implementation from this module by importing ``IntoChunkedReadAheadIterator``.
///
/// The chunked iterator can panic in the following circumstances:
///     - panics if the underlying iterator panics after the same number of ``next()`` calls.
pub struct ChunkedReadAheadIterator<T: Send + 'static> {
    /// The recieving object that recieves chunks of ``T``.
    receiver: Receiver<Result<Vec<T>, PanicUnwindErr>>,
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
        thread::Builder::new()
            .name("chunked_read_ahead_thread".to_owned())
            .spawn(move || {
                'chunk_loop: loop {
                    let mut chunk = Vec::with_capacity(chunk_size);
                    for _ in 0..chunk_size {
                        match catch_unwind(AssertUnwindSafe(|| inner.by_ref().next())) {
                            Ok(Some(val)) => chunk.push(val),
                            Ok(None) => break,
                            Err(e) => {
                                let _x = sender.send(Ok(chunk));
                                let _x = sender.send(Err(e));
                                break 'chunk_loop;
                            }
                        }
                    }
                    // If there is nothing in the chunk because the innner iterator is
                    // exhausted, or we get a send error (implying the receiver has been
                    // dropped), then exit the thread's main loop.
                    if chunk.is_empty() || sender.send(Ok(chunk)).is_err() {
                        break;
                    }
                }
            })
            .expect("failed to spawn chunked read ahead thread");

        // Store the necessary objects on ``Self``
        Self { receiver, current_chunk: Vec::new().into_iter() }
    }
}

impl<T> Iterator for ChunkedReadAheadIterator<T>
where
    T: Send + 'static,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        // Check if our current chunk has anything left in it
        // If so, just return that result
        // If not, see documentation on else block
        let next_option = self.current_chunk.next();
        if next_option.is_some() {
            next_option
        } else {
            // Current chunk didn't have anything left in it, so
            // Try to grab a new chunk, and panic if there are no chunks left (note that
            // ``recv`` is blocking, so this will only return an error if the sender has been
            // dropped and there are no more elements in the channel.)
            // Clippy incorrectly marks this as something that can be done with a question mark
            // so ignore that lint

            #[allow(clippy::question_mark)]
            let res_r = if let Ok(result) = self.receiver.recv() {
                result
            } else {
                return None;
            };
            // If the new chunk is present and Ok, convert it to an iterator, store it on ``self``,
            // and return its next value ( shutting down our reciever if the next value is None).
            // if the new chunk is an Err, raise it to the main thread.
            match res_r {
                Ok(next_chunk) => {
                    self.current_chunk = next_chunk.into_iter();
                    self.current_chunk.next()
                }
                Err(e) => {
                    resume_unwind(e);
                }
            }
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
        let test_vec: Vec<usize> = (0..1_000_000).into_iter().collect();
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
        let chunked_iter = (0..100_000).into_iter().read_ahead(8, 1);
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
        let mut test_iter = FailingIter::new().into_iter().read_ahead(8, 1);

        for _ in 0..FAIL_POINT {
            // Need to pass ownership back and forth to avoid the borrow checker complaining
            panic::catch_unwind(AssertUnwindSafe(|| {
                test_iter.next();
            }))
            .expect("different error message");
        }
        test_iter.next();
    }
}
