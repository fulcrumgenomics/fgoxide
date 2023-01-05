#[cfg(test)]
mod tests {
    use rstest::*;
    use std::mem::drop;
    use std::panic;
    use std::thread::sleep;
    use std::time::Duration;

    #[allow(clippy::no_effect_underscore_binding)]
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
    fn test_dropping_half_used_iterator_doesnt_explode(#[case] _chunk_size: usize) {
        let test_vec = vec![0usize, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let mut chunked_iter = test_vec.iter(); // TODO add into chunked readahead
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
    fn test_read_ahead_results_in_same_results_as_regular_iter(#[case] _chunk_size: usize) {
        let test_vec = vec![0usize, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
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

    #[test]
    #[should_panic(expected = "expected error message")]
    fn test_panic_occurring_mid_chunk_returns_results_until_panic() {
        // TODO - add into_chunked and chunk size specification to this > ``FAIL_POINT``
        let mut test_iter = FailingIter::new();

        for _ in 0..FAIL_POINT {
            // Need to pass ownership back and forth to avoid the borrow checker complaining
            test_iter = panic::catch_unwind(|| {
                test_iter.next();
                test_iter
            })
            .expect("different error message");
        }
        test_iter.next();
    }
}
