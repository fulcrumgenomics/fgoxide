//! Types and utilities for working with BAM/SAM alignment data.
//!
//! This module provides the [`Template`] struct for grouping alignment records
//! by query name, following the pattern established by fgbio (Scala) and fgpyo (Python).

use rust_htslib::bam::Record;
use thiserror::Error;

/// Errors that can occur when building a [`Template`].
#[derive(Error, Debug)]
pub enum TemplateError {
    /// Records have mismatched query names.
    #[error("Records have mismatched query names: expected '{expected}', found '{found}'")]
    MismatchedQueryNames {
        /// The expected query name (from the first record).
        expected: String,
        /// The query name that didn't match.
        found: String,
    },

    /// Multiple primary R1 records found.
    #[error("Multiple primary R1 records found for template '{name}'")]
    MultiplePrimaryR1 {
        /// The query name of the template.
        name: String,
    },

    /// Multiple primary R2 records found.
    #[error("Multiple primary R2 records found for template '{name}'")]
    MultiplePrimaryR2 {
        /// The query name of the template.
        name: String,
    },
}

/// A collection of alignment records for a single template (query name).
///
/// A `Template` groups all BAM records sharing the same query name, organizing them
/// into primary, secondary, and supplementary alignments for both R1 and R2 reads.
///
/// # Record Classification
///
/// Records are classified based on their FLAG bits:
/// - **Primary**: Neither secondary (0x100) nor supplementary (0x800)
/// - **Secondary**: Has secondary flag (0x100) set - checked first
/// - **Supplementary**: Has supplementary flag (0x800) set but not secondary
///
/// Records are assigned to R1 or R2 based on:
/// - **R1**: Unpaired reads, or paired reads with "first in pair" flag (0x40)
/// - **R2**: Paired reads with "last in pair" flag (0x80)
///
/// # Note on Secondary+Supplementary Records
///
/// Records marked as both secondary AND supplementary are placed in the secondary
/// list, following the fgbio convention.
#[derive(Debug, Default)]
pub struct Template {
    /// The primary R1 alignment, if present.
    pub r1: Option<Record>,
    /// The primary R2 alignment, if present.
    pub r2: Option<Record>,
    /// Secondary alignments for R1.
    pub r1_secondaries: Vec<Record>,
    /// Secondary alignments for R2.
    pub r2_secondaries: Vec<Record>,
    /// Supplementary alignments for R1.
    pub r1_supplementaries: Vec<Record>,
    /// Supplementary alignments for R2.
    pub r2_supplementaries: Vec<Record>,
}
