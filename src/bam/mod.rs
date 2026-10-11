//! Types and utilities for working with BAM/SAM alignment data.
//!
//! This module provides the [`Template`] struct for grouping alignment records
//! by query name, following the pattern established by `fgbio` (Scala) and
//! `fgpyo` (Python).
//!
//! # Source material being ported
//!
//! Pinned to upstream commits at the time this module was written:
//!
//! - fgbio `Template` (case class + companion object): `Bams.scala`
//!   lines 44-174 at `fulcrumgenomics/fgbio@768d38c0`.
//! - fgpyo `Template`: `fgpyo/sam/__init__.py` lines 1346-1561 at
//!   `fulcrumgenomics/fgpyo@416f0f64`.

use rust_htslib::bam::Record;
use thiserror::Error;

/// Errors that can occur when building a [`Template`].
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum TemplateError {
    /// No records were provided.
    #[error("Cannot construct a Template from zero records")]
    EmptyTemplate,

    /// Records have mismatched query names.
    #[error(
        "Records have mismatched query names: expected '{}', found '{}'",
        String::from_utf8_lossy(.expected),
        String::from_utf8_lossy(.found)
    )]
    MismatchedQueryNames {
        /// The expected query name (from the first record).
        expected: Vec<u8>,
        /// The query name that didn't match.
        found: Vec<u8>,
    },

    /// Multiple primary R1 records found.
    #[error(
        "Multiple primary R1 records found for template '{}'",
        String::from_utf8_lossy(.name)
    )]
    MultiplePrimaryR1 {
        /// The query name of the template.
        name: Vec<u8>,
    },

    /// Multiple primary R2 records found.
    #[error(
        "Multiple primary R2 records found for template '{}'",
        String::from_utf8_lossy(.name)
    )]
    MultiplePrimaryR2 {
        /// The query name of the template.
        name: Vec<u8>,
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
///
/// # Direct Field Mutation
///
/// Fields are public to mirror fgbio (Scala `var`) and fgpyo. Mutating them directly
/// bypasses the validation that [`Template::new`] enforces; a `Template` constructed
/// or modified that way may not satisfy the documented invariants.
#[derive(Debug, Default, Clone)]
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

enum Bucket {
    Primary,
    Secondary,
    Supplementary,
}

// Secondary checked before supplementary so records flagged as both land in the
// secondaries list, matching fgbio's convention.
fn classify(rec: &Record) -> Bucket {
    if rec.is_secondary() {
        Bucket::Secondary
    } else if rec.is_supplementary() {
        Bucket::Supplementary
    } else {
        Bucket::Primary
    }
}

fn is_r1(rec: &Record) -> bool {
    !rec.is_paired() || rec.is_first_in_template()
}

impl Template {
    /// Creates a `Template` from a collection of records sharing the same query name.
    ///
    /// Records are classified based on their FLAG bits:
    /// - Primary records (neither secondary nor supplementary) are stored in `r1` or `r2`
    /// - Secondary records (0x100 flag) are stored in `r1_secondaries` or `r2_secondaries`
    /// - Supplementary records (0x800 flag, but not secondary) are stored in
    ///   `r1_supplementaries` or `r2_supplementaries`
    ///
    /// Records are assigned to R1 if they are unpaired or have the "first in pair" flag (0x40).
    /// Records are assigned to R2 if they are paired and have the "last in pair" flag (0x80).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No records are provided ([`TemplateError::EmptyTemplate`])
    /// - Records have mismatched query names ([`TemplateError::MismatchedQueryNames`])
    /// - Multiple primary R1 records are found ([`TemplateError::MultiplePrimaryR1`])
    /// - Multiple primary R2 records are found ([`TemplateError::MultiplePrimaryR2`])
    ///
    /// # Examples
    ///
    /// ```
    /// use fgoxide::bam::Template;
    /// use rust_htslib::bam::Record;
    ///
    /// let mut r1 = Record::new();
    /// r1.set(b"read1", None, b"ACGT", &[30, 30, 30, 30]);
    /// r1.set_flags(0x41); // PAIRED | FIRST_IN_PAIR
    ///
    /// let mut r2 = Record::new();
    /// r2.set(b"read1", None, b"ACGT", &[30, 30, 30, 30]);
    /// r2.set_flags(0x81); // PAIRED | LAST_IN_PAIR
    ///
    /// let template = Template::new(vec![r1, r2]).unwrap();
    /// assert!(template.r1.is_some());
    /// assert!(template.r2.is_some());
    /// ```
    pub fn new(recs: impl IntoIterator<Item = Record>) -> Result<Self, TemplateError> {
        let mut iter = recs.into_iter();
        let first = iter.next().ok_or(TemplateError::EmptyTemplate)?;
        let expected_name = first.qname().to_vec();

        let mut template = Template::default();
        template.push_validated(first, &expected_name)?;
        for rec in iter {
            template.push_validated(rec, &expected_name)?;
        }
        Ok(template)
    }

    /// Creates a `Template` from records without validation.
    ///
    /// This is the unchecked variant of [`Template::new`]. It does not verify that all
    /// records share the same query name, and silently overwrites the primary R1 or R2
    /// when multiple primaries are passed in (the last one wins; earlier primaries are
    /// dropped).
    ///
    /// Use this when input invariants are guaranteed by the producer (e.g., records
    /// emerging from a query-name-grouped iterator) and the validation overhead is
    /// undesirable.
    ///
    /// An empty input produces a default `Template` (no error, unlike [`Template::new`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use fgoxide::bam::Template;
    /// use rust_htslib::bam::Record;
    ///
    /// let mut rec = Record::new();
    /// rec.set(b"read1", None, b"ACGT", &[30, 30, 30, 30]);
    /// rec.set_flags(0x41);
    ///
    /// let template = Template::new_unchecked(vec![rec]);
    /// assert!(template.r1.is_some());
    /// ```
    pub fn new_unchecked(recs: impl IntoIterator<Item = Record>) -> Self {
        let mut template = Template::default();
        for rec in recs {
            template.push(rec);
        }
        template
    }

    fn push(&mut self, rec: Record) {
        match (classify(&rec), is_r1(&rec)) {
            (Bucket::Primary, true) => self.r1 = Some(rec),
            (Bucket::Primary, false) => self.r2 = Some(rec),
            (Bucket::Secondary, true) => self.r1_secondaries.push(rec),
            (Bucket::Secondary, false) => self.r2_secondaries.push(rec),
            (Bucket::Supplementary, true) => self.r1_supplementaries.push(rec),
            (Bucket::Supplementary, false) => self.r2_supplementaries.push(rec),
        }
    }

    fn push_validated(&mut self, rec: Record, expected_name: &[u8]) -> Result<(), TemplateError> {
        if rec.qname() != expected_name {
            return Err(TemplateError::MismatchedQueryNames {
                expected: expected_name.to_vec(),
                found: rec.qname().to_vec(),
            });
        }
        if matches!(classify(&rec), Bucket::Primary) {
            if is_r1(&rec) {
                if self.r1.is_some() {
                    return Err(TemplateError::MultiplePrimaryR1 { name: expected_name.to_vec() });
                }
            } else if self.r2.is_some() {
                return Err(TemplateError::MultiplePrimaryR2 { name: expected_name.to_vec() });
            }
        }
        self.push(rec);
        Ok(())
    }

    /// Returns the query name of this template, or `None` if the template is empty.
    ///
    /// The name is read from the first record yielded by [`Template::all_recs`].
    ///
    /// # Examples
    ///
    /// ```
    /// use fgoxide::bam::Template;
    /// use rust_htslib::bam::Record;
    ///
    /// let mut rec = Record::new();
    /// rec.set(b"read1", None, b"ACGT", &[30, 30, 30, 30]);
    /// rec.set_flags(0x41);
    ///
    /// let template = Template::new(vec![rec]).unwrap();
    /// assert_eq!(template.name(), Some(b"read1".as_slice()));
    /// ```
    pub fn name(&self) -> Option<&[u8]> {
        self.r1
            .as_ref()
            .or(self.r2.as_ref())
            .or_else(|| self.r1_supplementaries.first())
            .or_else(|| self.r2_supplementaries.first())
            .or_else(|| self.r1_secondaries.first())
            .or_else(|| self.r2_secondaries.first())
            .map(|r| r.qname())
    }

    /// Returns an iterator over all records in the template.
    ///
    /// Order matches fgbio's Scala `Template.allRecs`:
    /// `r1`, `r2`, `r1_supplementaries`, `r2_supplementaries`,
    /// `r1_secondaries`, `r2_secondaries`.
    ///
    /// Note: fgpyo's `Template.all_recs` interleaves the R1/R2 supplementaries and
    /// secondaries (`r1, r2, r1_supplementals, r1_secondaries, r2_supplementals,
    /// r2_secondaries`); this implementation deliberately follows the Scala ordering.
    ///
    /// # Examples
    ///
    /// ```
    /// use fgoxide::bam::Template;
    /// use rust_htslib::bam::Record;
    ///
    /// let mut rec = Record::new();
    /// rec.set(b"read1", None, b"ACGT", &[30, 30, 30, 30]);
    /// rec.set_flags(0x41);
    ///
    /// let template = Template::new(vec![rec]).unwrap();
    /// assert_eq!(template.all_recs().count(), 1);
    /// ```
    pub fn all_recs(&self) -> impl Iterator<Item = &Record> {
        self.r1
            .iter()
            .chain(self.r2.iter())
            .chain(self.r1_supplementaries.iter())
            .chain(self.r2_supplementaries.iter())
            .chain(self.r1_secondaries.iter())
            .chain(self.r2_secondaries.iter())
    }

    /// Returns an iterator over only the primary records: `r1` then `r2`.
    pub fn primary_recs(&self) -> impl Iterator<Item = &Record> {
        self.r1.iter().chain(self.r2.iter())
    }

    /// Returns an iterator over all R1 records: primary, then supplementaries, then secondaries.
    pub fn all_r1s(&self) -> impl Iterator<Item = &Record> {
        self.r1.iter().chain(self.r1_supplementaries.iter()).chain(self.r1_secondaries.iter())
    }

    /// Returns an iterator over all R2 records: primary, then supplementaries, then secondaries.
    pub fn all_r2s(&self) -> impl Iterator<Item = &Record> {
        self.r2.iter().chain(self.r2_supplementaries.iter()).chain(self.r2_secondaries.iter())
    }

    /// Returns the total number of records across all six buckets.
    pub fn len(&self) -> usize {
        usize::from(self.r1.is_some())
            + usize::from(self.r2.is_some())
            + self.r1_secondaries.len()
            + self.r2_secondaries.len()
            + self.r1_supplementaries.len()
            + self.r2_supplementaries.len()
    }

    /// Returns `true` if this template contains no records.
    pub fn is_empty(&self) -> bool {
        self.r1.is_none()
            && self.r2.is_none()
            && self.r1_secondaries.is_empty()
            && self.r2_secondaries.is_empty()
            && self.r1_supplementaries.is_empty()
            && self.r2_supplementaries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a BAM record with specific flags and query name.
    fn make_record(qname: &[u8], flags: u16) -> Record {
        let mut rec = Record::new();
        rec.set(qname, None, b"ACGT", &[30, 30, 30, 30]);
        rec.set_flags(flags);
        rec
    }

    const PAIRED: u16 = 0x1;
    const FIRST_IN_PAIR: u16 = 0x40;
    const LAST_IN_PAIR: u16 = 0x80;
    const SECONDARY: u16 = 0x100;
    const SUPPLEMENTARY: u16 = 0x800;

    mod paired_end_tests {
        use super::*;

        #[test]
        fn test_new_paired_end_primaries() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"read1", PAIRED | LAST_IN_PAIR);

            let template = Template::new(vec![r1, r2]).unwrap();

            assert!(template.r1.is_some());
            assert!(template.r2.is_some());
            assert_eq!(template.r1.as_ref().unwrap().qname(), b"read1");
            assert_eq!(template.r2.as_ref().unwrap().qname(), b"read1");
            assert!(template.r1_secondaries.is_empty());
            assert!(template.r2_secondaries.is_empty());
            assert!(template.r1_supplementaries.is_empty());
            assert!(template.r2_supplementaries.is_empty());
        }

        #[test]
        fn test_name_returns_query_name() {
            let r1 = make_record(b"my_read", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"my_read", PAIRED | LAST_IN_PAIR);

            let template = Template::new(vec![r1, r2]).unwrap();

            assert_eq!(template.name(), Some(b"my_read".as_slice()));
        }

        #[test]
        fn test_all_recs_iterator() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"read1", PAIRED | LAST_IN_PAIR);

            let template = Template::new(vec![r1, r2]).unwrap();

            assert_eq!(template.all_recs().count(), 2);
        }

        #[test]
        fn test_primary_recs_iterator() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"read1", PAIRED | LAST_IN_PAIR);

            let template = Template::new(vec![r1, r2]).unwrap();

            assert_eq!(template.primary_recs().count(), 2);
        }

        #[test]
        fn test_r1_only() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);

            let template = Template::new(vec![r1]).unwrap();

            assert!(template.r1.is_some());
            assert!(template.r2.is_none());
            assert_eq!(template.name(), Some(b"read1".as_slice()));
        }

        #[test]
        fn test_r2_only() {
            let r2 = make_record(b"read1", PAIRED | LAST_IN_PAIR);

            let template = Template::new(vec![r2]).unwrap();

            assert!(template.r1.is_none());
            assert!(template.r2.is_some());
            assert_eq!(template.name(), Some(b"read1".as_slice()));
        }
    }

    mod single_end_tests {
        use super::*;

        #[test]
        fn test_unpaired_read_goes_to_r1() {
            // Unpaired reads (no PAIRED flag) should go to r1
            let rec = make_record(b"fragment", 0);

            let template = Template::new(vec![rec]).unwrap();

            assert!(template.r1.is_some());
            assert!(template.r2.is_none());
            assert_eq!(template.name(), Some(b"fragment".as_slice()));
        }

        #[test]
        fn test_unpaired_read_iterators() {
            let rec = make_record(b"fragment", 0);

            let template = Template::new(vec![rec]).unwrap();

            assert_eq!(template.all_recs().count(), 1);
            assert_eq!(template.primary_recs().count(), 1);
        }

        #[test]
        fn test_last_in_pair_without_paired_goes_to_r1() {
            // Per the SAM spec, FIRST/LAST_IN_PAIR are meaningful only when PAIRED is set.
            // Without PAIRED, the record is treated as unpaired and routed to r1.
            let rec = make_record(b"fragment", LAST_IN_PAIR);

            let template = Template::new(vec![rec]).unwrap();

            assert!(template.r1.is_some());
            assert!(template.r2.is_none());
        }
    }

    mod secondary_supplementary_tests {
        use super::*;

        #[test]
        fn test_secondary_r1_alignment() {
            let primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let secondary = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);

            let template = Template::new(vec![primary, secondary]).unwrap();

            assert!(template.r1.is_some());
            assert_eq!(template.r1_secondaries.len(), 1);
            assert!(template.r1_supplementaries.is_empty());
        }

        #[test]
        fn test_secondary_r2_alignment() {
            let primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let secondary = make_record(b"read1", PAIRED | LAST_IN_PAIR | SECONDARY);

            let template = Template::new(vec![primary, secondary]).unwrap();

            assert!(template.r2.is_some());
            assert_eq!(template.r2_secondaries.len(), 1);
            assert!(template.r2_supplementaries.is_empty());
        }

        #[test]
        fn test_supplementary_r1_alignment() {
            let primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let supplementary = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SUPPLEMENTARY);

            let template = Template::new(vec![primary, supplementary]).unwrap();

            assert!(template.r1.is_some());
            assert!(template.r1_secondaries.is_empty());
            assert_eq!(template.r1_supplementaries.len(), 1);
        }

        #[test]
        fn test_supplementary_r2_alignment() {
            let primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let supplementary = make_record(b"read1", PAIRED | LAST_IN_PAIR | SUPPLEMENTARY);

            let template = Template::new(vec![primary, supplementary]).unwrap();

            assert!(template.r2.is_some());
            assert!(template.r2_secondaries.is_empty());
            assert_eq!(template.r2_supplementaries.len(), 1);
        }

        #[test]
        fn test_secondary_and_supplementary_goes_to_secondaries() {
            // Per fgbio convention: records with both secondary AND supplementary flags
            // go to the secondaries list (secondary is checked first)
            let primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let both = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY | SUPPLEMENTARY);

            let template = Template::new(vec![primary, both]).unwrap();

            assert!(template.r1.is_some());
            assert_eq!(template.r1_secondaries.len(), 1);
            assert!(template.r1_supplementaries.is_empty());
        }

        #[test]
        fn test_multiple_secondaries_and_supplementaries() {
            let r1_primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2_primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let r1_sec1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);
            let r1_sec2 = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);
            let r2_sec1 = make_record(b"read1", PAIRED | LAST_IN_PAIR | SECONDARY);
            let r1_supp1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SUPPLEMENTARY);
            let r2_supp1 = make_record(b"read1", PAIRED | LAST_IN_PAIR | SUPPLEMENTARY);
            let r2_supp2 = make_record(b"read1", PAIRED | LAST_IN_PAIR | SUPPLEMENTARY);

            let template = Template::new(vec![
                r1_primary, r2_primary, r1_sec1, r1_sec2, r2_sec1, r1_supp1, r2_supp1, r2_supp2,
            ])
            .unwrap();

            assert!(template.r1.is_some());
            assert!(template.r2.is_some());
            assert_eq!(template.r1_secondaries.len(), 2);
            assert_eq!(template.r2_secondaries.len(), 1);
            assert_eq!(template.r1_supplementaries.len(), 1);
            assert_eq!(template.r2_supplementaries.len(), 2);
        }

        #[test]
        fn test_r1_and_r2_supplementaries_with_both_primaries() {
            // Verify R1-supp and R2-supp land in their respective buckets when both
            // primaries are present in the same template.
            let r1_primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2_primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let r1_supp = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SUPPLEMENTARY);
            let r2_supp = make_record(b"read1", PAIRED | LAST_IN_PAIR | SUPPLEMENTARY);

            let template = Template::new(vec![r1_primary, r2_primary, r1_supp, r2_supp]).unwrap();

            assert!(template.r1.is_some());
            assert!(template.r2.is_some());
            assert_eq!(template.r1_supplementaries.len(), 1);
            assert_eq!(template.r2_supplementaries.len(), 1);
            assert!(template.r1_supplementaries[0].is_first_in_template());
            assert!(template.r2_supplementaries[0].is_last_in_template());
        }

        #[test]
        fn test_all_recs_includes_all_alignments() {
            let r1_primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2_primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let r1_sec = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);
            let r2_supp = make_record(b"read1", PAIRED | LAST_IN_PAIR | SUPPLEMENTARY);

            let template = Template::new(vec![r1_primary, r2_primary, r1_sec, r2_supp]).unwrap();

            assert_eq!(template.all_recs().count(), 4);
            assert_eq!(template.primary_recs().count(), 2);
        }
    }

    mod validation_error_tests {
        use super::*;

        #[test]
        fn test_empty_input_is_error() {
            let result = Template::new(Vec::<Record>::new());
            assert_eq!(result.unwrap_err(), TemplateError::EmptyTemplate);
        }

        #[test]
        fn test_mismatched_query_names_error() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"read2", PAIRED | LAST_IN_PAIR);

            let result = Template::new(vec![r1, r2]);

            assert_eq!(
                result.unwrap_err(),
                TemplateError::MismatchedQueryNames {
                    expected: b"read1".to_vec(),
                    found: b"read2".to_vec(),
                }
            );
        }

        #[test]
        fn test_multiple_primary_r1_error() {
            let r1a = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r1b = make_record(b"read1", PAIRED | FIRST_IN_PAIR);

            let result = Template::new(vec![r1a, r1b]);

            assert_eq!(
                result.unwrap_err(),
                TemplateError::MultiplePrimaryR1 { name: b"read1".to_vec() }
            );
        }

        #[test]
        fn test_multiple_primary_r2_error() {
            let r2a = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let r2b = make_record(b"read1", PAIRED | LAST_IN_PAIR);

            let result = Template::new(vec![r2a, r2b]);

            assert_eq!(
                result.unwrap_err(),
                TemplateError::MultiplePrimaryR2 { name: b"read1".to_vec() }
            );
        }

        #[test]
        fn test_multiple_unpaired_primaries_error() {
            // Two unpaired reads should fail since both go to r1
            let r1 = make_record(b"read1", 0);
            let r2 = make_record(b"read1", 0);

            let result = Template::new(vec![r1, r2]);

            assert_eq!(
                result.unwrap_err(),
                TemplateError::MultiplePrimaryR1 { name: b"read1".to_vec() }
            );
        }

        #[test]
        fn test_mismatched_query_names_error_preserves_non_utf8_bytes() {
            // qnames are bytes; ensure non-UTF8 bytes round-trip in the error.
            let r1 = make_record(&[0xff, 0xfe], PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(&[0xfd, 0xfc], PAIRED | LAST_IN_PAIR);

            let err = Template::new(vec![r1, r2]).unwrap_err();
            assert_eq!(
                err,
                TemplateError::MismatchedQueryNames {
                    expected: vec![0xff, 0xfe],
                    found: vec![0xfd, 0xfc],
                }
            );
        }
    }

    mod edge_case_tests {
        use super::*;

        #[test]
        fn test_default_template_is_empty() {
            let template = Template::default();

            assert!(template.r1.is_none());
            assert!(template.r2.is_none());
            assert!(template.r1_secondaries.is_empty());
            assert!(template.r2_secondaries.is_empty());
            assert!(template.r1_supplementaries.is_empty());
            assert!(template.r2_supplementaries.is_empty());
            assert!(template.name().is_none());
            assert_eq!(template.all_recs().count(), 0);
            assert_eq!(template.primary_recs().count(), 0);
            assert_eq!(template.len(), 0);
            assert!(template.is_empty());
        }

        #[test]
        fn test_name_from_secondary_only() {
            let sec = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);

            let template = Template::new(vec![sec]).unwrap();

            assert!(template.r1.is_none());
            assert_eq!(template.name(), Some(b"read1".as_slice()));
        }

        #[test]
        fn test_name_from_supplementary_only() {
            let supp = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SUPPLEMENTARY);

            let template = Template::new(vec![supp]).unwrap();

            assert!(template.r1.is_none());
            assert_eq!(template.name(), Some(b"read1".as_slice()));
        }

        #[test]
        fn test_all_recs_ordering_matches_fgbio() {
            // fgbio Scala order: r1, r2, r1_supplementaries, r2_supplementaries,
            // r1_secondaries, r2_secondaries.
            let r1_primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2_primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let r1_sec = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);
            let r2_sec = make_record(b"read1", PAIRED | LAST_IN_PAIR | SECONDARY);
            let r1_supp = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SUPPLEMENTARY);
            let r2_supp = make_record(b"read1", PAIRED | LAST_IN_PAIR | SUPPLEMENTARY);

            let template =
                Template::new(vec![r1_primary, r2_primary, r1_sec, r2_sec, r1_supp, r2_supp])
                    .unwrap();

            let recs: Vec<_> = template.all_recs().collect();
            assert_eq!(recs.len(), 6);

            // [0] r1 primary
            assert!(recs[0].is_first_in_template());
            assert!(!recs[0].is_secondary() && !recs[0].is_supplementary());
            // [1] r2 primary
            assert!(recs[1].is_last_in_template());
            assert!(!recs[1].is_secondary() && !recs[1].is_supplementary());
            // [2] r1 supplementary
            assert!(recs[2].is_first_in_template());
            assert!(recs[2].is_supplementary());
            // [3] r2 supplementary
            assert!(recs[3].is_last_in_template());
            assert!(recs[3].is_supplementary());
            // [4] r1 secondary
            assert!(recs[4].is_first_in_template());
            assert!(recs[4].is_secondary());
            // [5] r2 secondary
            assert!(recs[5].is_last_in_template());
            assert!(recs[5].is_secondary());
        }
    }

    mod accessor_tests {
        use super::*;

        fn full_template() -> Template {
            let r1_primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2_primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let r1_sec = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);
            let r2_sec = make_record(b"read1", PAIRED | LAST_IN_PAIR | SECONDARY);
            let r1_supp = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SUPPLEMENTARY);
            let r2_supp = make_record(b"read1", PAIRED | LAST_IN_PAIR | SUPPLEMENTARY);

            Template::new(vec![r1_primary, r2_primary, r1_sec, r2_sec, r1_supp, r2_supp]).unwrap()
        }

        #[test]
        fn test_all_r1s_yields_only_r1_records() {
            let template = full_template();

            let r1s: Vec<_> = template.all_r1s().collect();
            assert_eq!(r1s.len(), 3);
            for rec in &r1s {
                assert!(rec.is_first_in_template());
            }
            // primary first, then supplementary, then secondary
            assert!(!r1s[0].is_secondary() && !r1s[0].is_supplementary());
            assert!(r1s[1].is_supplementary());
            assert!(r1s[2].is_secondary());
        }

        #[test]
        fn test_all_r2s_yields_only_r2_records() {
            let template = full_template();

            let r2s: Vec<_> = template.all_r2s().collect();
            assert_eq!(r2s.len(), 3);
            for rec in &r2s {
                assert!(rec.is_last_in_template());
            }
            assert!(!r2s[0].is_secondary() && !r2s[0].is_supplementary());
            assert!(r2s[1].is_supplementary());
            assert!(r2s[2].is_secondary());
        }

        #[test]
        fn test_len_counts_all_records() {
            let template = full_template();
            assert_eq!(template.len(), 6);
            assert!(!template.is_empty());
        }

        #[test]
        fn test_len_with_only_primaries() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let template = Template::new(vec![r1, r2]).unwrap();

            assert_eq!(template.len(), 2);
            assert!(!template.is_empty());
        }

        #[test]
        fn test_template_is_clone() {
            let template = full_template();
            let cloned = template.clone();
            assert_eq!(cloned.len(), template.len());
            assert_eq!(cloned.name(), template.name());
        }
    }

    mod new_unchecked_tests {
        use super::*;

        #[test]
        fn test_new_unchecked_paired_end() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"read1", PAIRED | LAST_IN_PAIR);

            let template = Template::new_unchecked(vec![r1, r2]);

            assert!(template.r1.is_some());
            assert!(template.r2.is_some());
            assert_eq!(template.name(), Some(b"read1".as_slice()));
        }

        #[test]
        fn test_new_unchecked_empty_yields_default_template() {
            let template = Template::new_unchecked(Vec::<Record>::new());

            assert!(template.is_empty());
            assert!(template.name().is_none());
        }

        #[test]
        fn test_new_unchecked_skips_qname_validation() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"read2", PAIRED | LAST_IN_PAIR);

            let template = Template::new_unchecked(vec![r1, r2]);

            assert_eq!(template.r1.as_ref().unwrap().qname(), b"read1");
            assert_eq!(template.r2.as_ref().unwrap().qname(), b"read2");
        }

        #[test]
        fn test_new_unchecked_overwrites_duplicate_primary() {
            // Per the documented behavior: last primary wins; earlier is dropped.
            let r1a = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let mut r1b = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            r1b.set(b"read1", None, b"GGGG", &[20, 20, 20, 20]);

            let template = Template::new_unchecked(vec![r1a, r1b]);

            // Only one r1; it's the second one (verified by sequence).
            assert!(template.r1.is_some());
            assert_eq!(template.r1.as_ref().unwrap().seq().as_bytes(), b"GGGG");
            assert!(template.r2.is_none());
        }

        #[test]
        fn test_new_unchecked_classifies_secondaries_and_supplementaries() {
            let r1_primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2_primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let r1_sec = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);
            let r2_supp = make_record(b"read1", PAIRED | LAST_IN_PAIR | SUPPLEMENTARY);

            let template = Template::new_unchecked(vec![r1_primary, r2_primary, r1_sec, r2_supp]);

            assert!(template.r1.is_some());
            assert!(template.r2.is_some());
            assert_eq!(template.r1_secondaries.len(), 1);
            assert_eq!(template.r2_supplementaries.len(), 1);
        }
    }
}
