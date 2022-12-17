use std::iter::Iterator;

/// An iterator that chunks the underlying ``T`` objects in an inner iterator to ``Vec``s  of
/// length ``chunk_size``
pub struct ChunkingIterator<I: Iterator> {
    /// The size of the chunk to return in the chunking iterator
    pub chunk_size: usize,
    /// The owned iterator object being iterated over.
    inner_iterator: I,
}

impl<I: Iterator> ChunkingIterator<I> {
    #[must_use]
    /// Create a new chunking iterator from an owned ``Iterator`` and a size of chunk to produce.
    pub fn new(iterator: I, chunk_size: usize) -> Self {
        Self { chunk_size, inner_iterator: iterator }
    }
}

impl<I: Iterator> Iterator for ChunkingIterator<I> {
    type Item = Vec<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let result =
            self.inner_iterator.by_ref().take(self.chunk_size).into_iter().collect::<Vec<_>>();
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}

/// Trait that defines a method for converting a regular itearator into a chunking iterator
trait IntoChunkingItererator {
    /// The item being iterated over in the underlying iterator.
    type Item;
    /// The type of of the underlying iterator.
    type IntoChunkingIter: Iterator<Item = Self::Item>;

    /// Converts ``self`` into a ``ChunkingIterator``.
    fn into_chunked(self, chunk_size: usize) -> ChunkingIterator<Self::IntoChunkingIter>;
}

impl<T, I: Iterator<Item = T>> IntoChunkingItererator for I {
    type Item = T;
    type IntoChunkingIter = I;

    fn into_chunked(self, chunk_size: usize) -> ChunkingIterator<Self::IntoChunkingIter> {
        ChunkingIterator::new(self, chunk_size)
    }
}

#[cfg(test)]
mod tests {
    use crate::iterators::IntoChunkingItererator;

    #[test]
    fn test_chunking_iterator_trait_works() {
        let boxed_iter: Box<dyn Iterator<Item = usize>> =
            Box::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10].into_iter());

        let mut chunked_iter = boxed_iter.into_chunked(5);

        assert_eq!(chunked_iter.next().unwrap(), vec![1, 2, 3, 4, 5]);
        assert_eq!(chunked_iter.next().unwrap(), vec![6, 7, 8, 9, 10]);
        assert!(chunked_iter.next().is_none());
    }

    #[test]
    fn test_unbalanced_underlying_iter_works() {
        let boxed_iter: Box<dyn Iterator<Item = usize>> =
            Box::new(vec![1, 2, 3, 4, 5, 6, 7, 8].into_iter());

        let mut chunked_iter = boxed_iter.into_chunked(5);

        assert_eq!(chunked_iter.next().unwrap(), vec![1, 2, 3, 4, 5]);
        assert_eq!(chunked_iter.next().unwrap(), vec![6, 7, 8]);
        assert!(chunked_iter.next().is_none());
    }

    #[test]
    fn test_empty_underlying_iter_works() {
        let boxed_iter: Box<dyn Iterator<Item = usize>> = Box::new(Vec::new().into_iter());

        let mut chunked_iter = boxed_iter.into_chunked(5);

        assert!(chunked_iter.next().is_none());
    }
}
