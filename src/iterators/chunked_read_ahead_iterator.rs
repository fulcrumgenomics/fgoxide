use std::panic::panic_any;
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread::{self, JoinHandle};
use std::vec::IntoIter;


/// Iterator extension that spawns an additional thread to read-ahead in the iterator. Sends
/// results back to this object via a channel and returns them in the same manner as a normal
/// iterator
/// 
// A few main classes of error handling that need handling here:
// 1. The interior thread panics at some point, halfway through a chunk
// 2. The interior thread panics at some point, on the border of a chunk
// 3. The interior thread panics at some point, after done iterating
// 4. The exterior thread panics at some point, and needs to shut down the interior thread

// 1-3. I have run into a bit of a blocker on. We either need to not use the .take method on iterator
// as described in the spec or accept that we may end up with a panic cutting off the chunk early.
// I'd vote for the former. (The reason I believe this is because if take panics it will not return
// the values leading up to the panic, so we'll need to consume the iterator manually to chunk
// stuff)
// 4. is handled by the shutting down of the reciever upon termination of the ChunkedReadaheadIterator
// resulting in a resulting Send error.

pub struct ChunkedReadaheadIterator<T: Send + 'static> {
    /// The recieving object that recieves chunks of ``T``. TODO - make this a Vec<T> when adding
    /// chunking.
    receiver: Option<Receiver<Option<Vec<T>>>>,
    /// The handle to the thread that was spawned to read ahead on the iterator.
    join_handle: Option<JoinHandle<()>>,
    current_chunk: IntoIter<T>,
}

impl<T> ChunkedReadaheadIterator<T>
where
    T: Send + 'static,
{
    /// Creates a new ``Self`` from an existing iterator, and parameters concerning the size of
    /// underlying buffer elements.
    ///
    /// # Panics
    ///
    /// - panics if the sender object inside the spawned thread fails to send.
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
        let join_handle: JoinHandle<()> = thread::Builder::new()
            .name("chunked_read_ahead_thread".to_owned())
            .spawn(move || {
                loop {
                    let chunk = inner.by_ref().take(chunk_size).into_iter().collect::<Vec<_>>();
                    if chunk.is_empty() {
                        break;
                    }
                    sender.send(Some(chunk)).expect("Error sending chunk");
                }
                sender.send(None).expect("Error sending final None");
            })
            .expect("failed to spawn chunked read ahead thread");
        // Store the necessary objects on ``Self``

        Self { current_chunk: Vec::new().into_iter(), receiver: Some(receiver), join_handle: Some(join_handle) }
    }
}

impl<T> Drop for ChunkedReadaheadIterator<T>
where
    T: Send + 'static,
{
    // prepares for dropping chunkedReadaheadIterator. Does this by setting the reciever to null
    // (thereby killing the spawned thread the next time it tries to send to it).
    fn drop(&mut self) {
        // Get the error value out of our join handle. To do this we need to take ownership of
        // the handle from ``self``, as otherwise it will not let us call ``join``.
        self.receiver = None;

        // Get the error value out of our join handle. To do this we need to take ownership of
        // the handle from ``self``, as otherwise it will not let us call ``join``.
        if let Some(join_handle) = Option::take(&mut self.join_handle) {
            // Call join, and if there was a resulting panic in the spawned thread raise that
            // panic to the main thread.
            if let Err(e) = join_handle.join() {
                panic_any(e);
            }
        }
    }
}

impl<T> Iterator for ChunkedReadaheadIterator<T>
where
    T: Send + 'static,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if let Some(result) = self.current_chunk.next() {
            Some(result)
        } else {
            let opt_r =
                self.receiver.as_ref().and_then(|r| r.recv().expect("recv of iterator value failed"));
            if let Some(next_chunk) = opt_r {
                self.current_chunk = next_chunk.into_iter();
                if let Some(result) = self.current_chunk.next() {
                    Some(result)
                } else {
                    self.receiver = None;
                    None
                }
            }
            else{
                self.receiver = None;
                None
            }
        }
    }
}

pub trait IntoChunkedReadaheadIterator<T>
where
    T: Send + 'static,
{
    /// Apply a readahead adaptor to an iterator.
    ///
    /// `buffer_size` is the maximum number of buffered items.
    fn readahead(
        self,
        chunk_size: usize,
        num_chunk_buffer_size: usize,
    ) -> ChunkedReadaheadIterator<T>
    where
        Self: Send + 'static;
}

impl<I, T> IntoChunkedReadaheadIterator<T> for I
where
    T: Send + 'static,
    I: Iterator<Item = T>,
{
    fn readahead(
        self,
        chunk_size: usize,
        num_chunk_buffer_size: usize,
    ) -> ChunkedReadaheadIterator<T>
    where
        Self: Send + 'static,
    {
        ChunkedReadaheadIterator::new(self, chunk_size, num_chunk_buffer_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::mem::drop;
    // use std::panic;
    use std::thread::sleep;
    use std::time::Duration;

    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_wrapping_empty_iter(#[case] _chunk_size: usize) {
        let test_vec: Vec<usize> = Vec::new();

        let mut chunked_iter = test_vec.iter(); // TODO add into chunked readahead
        assert_eq!(chunked_iter.next(), None);
    }

    #[allow(clippy::no_effect_underscore_binding)]
    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_handle_large_iterator_and_low_chunk_size(#[case] _chunk_size: usize) {
        let test_vec: Vec<usize> = (0..1_000_000).into_iter().collect();
        let test_vec2 = test_vec.clone();

        let mut regular_iter = test_vec.iter();
        let mut chunked_iter = test_vec2.iter(); // TODO add into chunked readahead

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

    /// Continuing to read after the enclosed function ends gets more Nones.
    #[test]
    fn test_low_bound_on_channel_for_blocking() {
        let test_vec: Vec<usize> = (0..100_000).into_iter().collect();

        let mut chunked_iter = test_vec.iter(); // TODO add into chunked readahead
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

    #[allow(clippy::no_effect_underscore_binding)]
    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_dropping_before_doesnt_explode(#[case] _chunk_size: usize) {
        let test_vec = vec![0usize, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let chunked_iter = test_vec.iter(); // TODO add into chunked readahead
        sleep(Duration::from_millis(10));
        drop(chunked_iter);
    }

    #[allow(clippy::no_effect_underscore_binding)]
    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_dropping_half_used_iterator_doesnt_explode(#[case] chunk_size: usize) {
        let test_vec = vec![0usize, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let mut chunked_iter = test_vec.into_iter().readahead(chunk_size, 1); // TODO add into chunked readahead
        for _ in 0..4 {
            chunked_iter.next();
        }
        drop(chunked_iter);
    }

    #[allow(clippy::no_effect_underscore_binding)]
    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_dropping_fully_used_iterator_doesnt_explode(#[case] _chunk_size: usize) {
        let test_vec = vec![0usize, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let mut chunked_iter = test_vec.iter(); // TODO add into chunked readahead
                                                // need to do it this way so we don't lose ownership
        for _ in 0..test_vec.len() {
            chunked_iter.next();
        }
        drop(chunked_iter);
    }

    #[allow(clippy::no_effect_underscore_binding)]
    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    fn test_read_ahead_results_in_same_results_as_regular_iter(#[case] chunk_size: usize) {
        let test_vec = vec![0usize, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let mut regular_iter = test_vec.clone().into_iter();
        let mut chunked_iter = test_vec.clone().into_iter().readahead(chunk_size, 1);

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

    /// Number of iterations into iteration at which the FailingIter should fail.
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

    // Weird ownership issues here so commenting this out for now until I can figure it out
    // while working on error handling.

    // #[test]
    // #[should_panic(expected = "expected error message")]
    // fn test_panic_occurring_mid_chunk_returns_results_until_panic() {
    //     // TODO - add into_chunked and chunk size specification to this > ``FAIL_POINT``
    //     let mut test_iter = FailingIter::new().into_iter().readahead(8, 1);

    //     for _ in 0..FAIL_POINT {
    //         // Need to pass ownership back and forth to avoid the borrow checker complaining
    //         panic::catch_unwind(|| {
    //             test_iter.next();
    //         })
    //         .expect("different error message");
    //     }
    //     test_iter.next();
    // }
}
