#[cfg(test)]
mod tests {
    use rstest::*;
    use std::mem::drop;
    use std::panic;

    #[allow(clippy::no_effect_underscore_binding)]
    #[rstest]
    #[case(1)] // smallest possible
    #[case(2)]
    #[case(4)]
    #[case(8)]
    #[case(16)] // larger than the inner iterator
    #[test]
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
                break;
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
