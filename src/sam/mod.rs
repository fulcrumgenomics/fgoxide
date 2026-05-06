//! Helpers for working with SAM/BAM alignment records.
//!
//! This module provides the [`Template`] struct and the [`TemplateIterator`] adapter,
//! which group alignment records by query name in the same way as the equivalent
//! abstractions in [`fgbio`] and [`fgpyo`].
//!
//! A [`Template`] holds the primary R1 and R2 alignments for a single template along
//! with its secondary and supplementary alignments. A [`TemplateIterator`] consumes a
//! queryname-grouped stream of records and yields one [`Template`] per group.
//!
//! Records are owned by the resulting [`Template`]; the iterator therefore takes ownership
//! of records as it consumes them. Records are typed as [`noodles_sam::alignment::RecordBuf`]
//! so that this module is independent of the source format (SAM, BAM, or otherwise) provided
//! the input has been deserialized into the buffered alignment record type.
//!
//! [`fgbio`]: https://github.com/fulcrumgenomics/fgbio
//! [`fgpyo`]: https://github.com/fulcrumgenomics/fgpyo

pub mod template;

pub use self::template::{Template, TemplateIterator};
