use std::any::Any;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread;
use std::vec::IntoIter;

// type aliased to get clippy to not think this is too complex
type PanicUnwindErr = Box<dyn Any + Send>;
/// Iterator extension that spawns an additional thread to read-ahead in the iterator. Sends
/// results back to this object via a channel and returns them in the same manner as a normal
/// iterator
pub struct ChunkedReadAheadIterator<T: Send + 'static> {
    /// The recieving object that recieves chunks of ``T``. TODO - make this a Vec<T> when adding
    /// chunking.
    receiver: Option<Receiver<Result<Vec<T>, PanicUnwindErr>>>,
    /// The most recent chunk recieved as an iterator. Used to produce owned ``T`` objects from
    /// the chunk
    current_chunk: IntoIter<T>,
}

impl<T> ChunkedReadAheadIterator<T>
where
    T: Send + 'static,
{
    /// Creates a new ``Self`` from an existing iterator, and parameters concerning the size of
    /// underlying buffer elements.
    ///
    /// # Panics
    ///
    /// - panics if the underlying iterator panics
    /// - panics if sending final chunk after an internal panic fails
    /// - panics if sending the None kill signal to the recieiver after an internal panic fails
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
                                sender
                                    .send(Ok(chunk))
                                    .expect("Error sending final chunk before internal panic");
                                sender.send(Err(e)).expect("Error sending internal panic result");
                                break 'chunk_loop;
                            }
                        }
                    }
                    if chunk.is_empty() {
                        break;
                    }
                    let send_result = sender.send(Ok(chunk));
                    if send_result.is_err() {
                        // TODO - Logging?
                        break;
                    }
                }
            })
            .expect("failed to spawn chunked read ahead thread");

        // Store the necessary objects on ``Self``
        Self { current_chunk: Vec::new().into_iter(), receiver: Some(receiver) }
    }
}

impl<T> Drop for ChunkedReadAheadIterator<T>
where
    T: Send + 'static,
{
    fn drop(&mut self) {
        // Make sure our reciever is dropped so our spawned thread shuts down.
        self.receiver = None;
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
        if let Some(result) = self.current_chunk.next() {
            Some(result)
        } else {
            // Current chunk didn't have anything left in it, so
            // Try to grab a new chunk, and panic if there are no chunks left (note that
            // ``recv`` is blocking, so this will only return an error if the sender has been
            // dropped and there are no more elements in the channel.)
            let res_r = if let Some(r) = self.receiver.as_ref() {
                // Clippy incorrectly marks this as something that can be done with a question mark
                // so ignore that lint
                #[allow(clippy::question_mark)]
                if let Ok(result) = r.recv() {
                    result
                } else {
                    return None;
                }
            } else {
                let box_message: Box<dyn Any + Send> =
                    Box::new("Reciever is None yet iteration is ongoing.");
                Err(box_message)
            };
            // If the new chunk is present and Ok, convert it to an iterator, store it on ``self``,
            // and return its next value ( shutting down our reciever if the next value is None).
            // if the new chunk is an Err, raise it to the main thread.
            match res_r {
                Ok(next_chunk) => {
                    self.current_chunk = next_chunk.into_iter();
                    if let Some(result) = self.current_chunk.next() {
                        Some(result)
                    } else {
                        self.receiver = None;
                        None
                    }
                }
                Err(e) => {
                    self.receiver = None;
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

        let mut chunked_iter = test_vec.into_iter().read_ahead(chunk_size, 1); // TODO add into chunked readahead
        assert_eq!(chunked_iter.next(), None);
    }

    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_handle_large_iterator_and_low_chunk_size(#[case] chunk_size: usize) {
        let test_vec: Vec<usize> = (0..1_000_000).into_iter().collect();
        let test_vec2 = test_vec.clone();

        let mut regular_iter = test_vec.into_iter();
        let mut chunked_iter = test_vec2.into_iter().read_ahead(chunk_size, 1);

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
        let mut chunked_iter = (0..100_000).into_iter().read_ahead(8, 1);
        for _ in 0..4 {
            chunked_iter.next();
        }
        drop(chunked_iter);
        let mut test_iter = vec![0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter();
        for i in 0..20 {
            let v = test_iter.next();
            if i < 10 {
                assert_eq!(v, Some(i));
            } else {
                assert_eq!(v, None);
            }
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

    #[test]
    fn test_read_past_end() {
        let mut test_iter = vec![0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter();
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
