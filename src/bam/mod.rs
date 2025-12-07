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

impl Template {
    /// Builds a `Template` from a collection of records sharing the same query name.
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
    /// - Records have mismatched query names
    /// - Multiple primary R1 records are found
    /// - Multiple primary R2 records are found
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use fgoxide::bam::Template;
    /// use rust_htslib::bam::Record;
    ///
    /// let records: Vec<Record> = vec![/* records with same qname */];
    /// let template = Template::build(records)?;
    /// ```
    pub fn build(recs: impl IntoIterator<Item = Record>) -> Result<Self, TemplateError> {
        let mut template = Template::default();
        let mut expected_name: Option<Vec<u8>> = None;

        for rec in recs {
            // Validate query name matches
            let qname = rec.qname().to_vec();
            match &expected_name {
                None => expected_name = Some(qname.clone()),
                Some(expected) if *expected != qname => {
                    return Err(TemplateError::MismatchedQueryNames {
                        expected: String::from_utf8_lossy(expected).into_owned(),
                        found: String::from_utf8_lossy(&qname).into_owned(),
                    });
                }
                Some(_) => {}
            }

            // Determine if R1 or R2: unpaired or first-in-pair -> R1, otherwise R2
            let is_r1 = !rec.is_paired() || rec.is_first_in_template();

            // Classify by secondary/supplementary status (check secondary first per fgbio)
            if rec.is_secondary() {
                if is_r1 {
                    template.r1_secondaries.push(rec);
                } else {
                    template.r2_secondaries.push(rec);
                }
            } else if rec.is_supplementary() {
                if is_r1 {
                    template.r1_supplementaries.push(rec);
                } else {
                    template.r2_supplementaries.push(rec);
                }
            } else {
                // Primary record
                let name = String::from_utf8_lossy(&qname).into_owned();
                if is_r1 {
                    if template.r1.is_some() {
                        return Err(TemplateError::MultiplePrimaryR1 { name });
                    }
                    template.r1 = Some(rec);
                } else {
                    if template.r2.is_some() {
                        return Err(TemplateError::MultiplePrimaryR2 { name });
                    }
                    template.r2 = Some(rec);
                }
            }
        }

        Ok(template)
    }

    /// Returns the query name of this template, if any records are present.
    ///
    /// The query name is taken from the first available record in this order:
    /// r1, r2, r1_secondaries, r2_secondaries, r1_supplementaries, r2_supplementaries.
    ///
    /// Returns `None` if the template contains no records.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use fgoxide::bam::Template;
    ///
    /// let template = Template::build(records)?;
    /// if let Some(name) = template.name() {
    ///     println!("Template name: {}", String::from_utf8_lossy(name));
    /// }
    /// ```
    pub fn name(&self) -> Option<&[u8]> {
        self.all_recs().next().map(|r| r.qname())
    }

    /// Returns an iterator over all records in the template.
    ///
    /// Records are yielded in the following order:
    /// 1. Primary R1 (if present)
    /// 2. Primary R2 (if present)
    /// 3. R1 secondaries
    /// 4. R2 secondaries
    /// 5. R1 supplementaries
    /// 6. R2 supplementaries
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use fgoxide::bam::Template;
    ///
    /// let template = Template::build(records)?;
    /// for record in template.all_recs() {
    ///     println!("Record: {:?}", record.qname());
    /// }
    /// ```
    pub fn all_recs(&self) -> impl Iterator<Item = &Record> {
        self.r1
            .iter()
            .chain(self.r2.iter())
            .chain(self.r1_secondaries.iter())
            .chain(self.r2_secondaries.iter())
            .chain(self.r1_supplementaries.iter())
            .chain(self.r2_supplementaries.iter())
    }

    /// Returns an iterator over only the primary records (r1 and r2).
    ///
    /// This is a convenience method that iterates over just the primary alignments,
    /// yielding R1 first (if present), then R2 (if present).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use fgoxide::bam::Template;
    ///
    /// let template = Template::build(records)?;
    /// for primary in template.primary_recs() {
    ///     println!("Primary record: {:?}", primary.qname());
    /// }
    /// ```
    pub fn primary_recs(&self) -> impl Iterator<Item = &Record> {
        self.r1.iter().chain(self.r2.iter())
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

    // FLAG constants for readability
    const PAIRED: u16 = 0x1;
    const FIRST_IN_PAIR: u16 = 0x40;
    const LAST_IN_PAIR: u16 = 0x80;
    const SECONDARY: u16 = 0x100;
    const SUPPLEMENTARY: u16 = 0x800;

    mod paired_end_tests {
        use super::*;

        #[test]
        fn test_build_paired_end_primaries() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"read1", PAIRED | LAST_IN_PAIR);

            let template = Template::build(vec![r1, r2]).unwrap();

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

            let template = Template::build(vec![r1, r2]).unwrap();

            assert_eq!(template.name(), Some(b"my_read".as_slice()));
        }

        #[test]
        fn test_all_recs_iterator() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"read1", PAIRED | LAST_IN_PAIR);

            let template = Template::build(vec![r1, r2]).unwrap();

            let all: Vec<_> = template.all_recs().collect();
            assert_eq!(all.len(), 2);
        }

        #[test]
        fn test_primary_recs_iterator() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"read1", PAIRED | LAST_IN_PAIR);

            let template = Template::build(vec![r1, r2]).unwrap();

            let primaries: Vec<_> = template.primary_recs().collect();
            assert_eq!(primaries.len(), 2);
        }

        #[test]
        fn test_r1_only() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);

            let template = Template::build(vec![r1]).unwrap();

            assert!(template.r1.is_some());
            assert!(template.r2.is_none());
            assert_eq!(template.name(), Some(b"read1".as_slice()));
        }

        #[test]
        fn test_r2_only() {
            let r2 = make_record(b"read1", PAIRED | LAST_IN_PAIR);

            let template = Template::build(vec![r2]).unwrap();

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

            let template = Template::build(vec![rec]).unwrap();

            assert!(template.r1.is_some());
            assert!(template.r2.is_none());
            assert_eq!(template.name(), Some(b"fragment".as_slice()));
        }

        #[test]
        fn test_unpaired_read_iterators() {
            let rec = make_record(b"fragment", 0);

            let template = Template::build(vec![rec]).unwrap();

            assert_eq!(template.all_recs().count(), 1);
            assert_eq!(template.primary_recs().count(), 1);
        }
    }

    mod secondary_supplementary_tests {
        use super::*;

        #[test]
        fn test_secondary_r1_alignment() {
            let primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let secondary = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);

            let template = Template::build(vec![primary, secondary]).unwrap();

            assert!(template.r1.is_some());
            assert_eq!(template.r1_secondaries.len(), 1);
            assert!(template.r1_supplementaries.is_empty());
        }

        #[test]
        fn test_secondary_r2_alignment() {
            let primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let secondary = make_record(b"read1", PAIRED | LAST_IN_PAIR | SECONDARY);

            let template = Template::build(vec![primary, secondary]).unwrap();

            assert!(template.r2.is_some());
            assert_eq!(template.r2_secondaries.len(), 1);
            assert!(template.r2_supplementaries.is_empty());
        }

        #[test]
        fn test_supplementary_r1_alignment() {
            let primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let supplementary = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SUPPLEMENTARY);

            let template = Template::build(vec![primary, supplementary]).unwrap();

            assert!(template.r1.is_some());
            assert!(template.r1_secondaries.is_empty());
            assert_eq!(template.r1_supplementaries.len(), 1);
        }

        #[test]
        fn test_supplementary_r2_alignment() {
            let primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let supplementary = make_record(b"read1", PAIRED | LAST_IN_PAIR | SUPPLEMENTARY);

            let template = Template::build(vec![primary, supplementary]).unwrap();

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

            let template = Template::build(vec![primary, both]).unwrap();

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

            let template = Template::build(vec![
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
        fn test_all_recs_includes_all_alignments() {
            let r1_primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2_primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let r1_sec = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);
            let r2_supp = make_record(b"read1", PAIRED | LAST_IN_PAIR | SUPPLEMENTARY);

            let template = Template::build(vec![r1_primary, r2_primary, r1_sec, r2_supp]).unwrap();

            assert_eq!(template.all_recs().count(), 4);
            assert_eq!(template.primary_recs().count(), 2);
        }
    }

    mod validation_error_tests {
        use super::*;

        #[test]
        fn test_mismatched_query_names_error() {
            let r1 = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2 = make_record(b"read2", PAIRED | LAST_IN_PAIR);

            let result = Template::build(vec![r1, r2]);

            assert!(result.is_err());
            match result.unwrap_err() {
                TemplateError::MismatchedQueryNames { expected, found } => {
                    assert_eq!(expected, "read1");
                    assert_eq!(found, "read2");
                }
                _ => panic!("Expected MismatchedQueryNames error"),
            }
        }

        #[test]
        fn test_multiple_primary_r1_error() {
            let r1a = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r1b = make_record(b"read1", PAIRED | FIRST_IN_PAIR);

            let result = Template::build(vec![r1a, r1b]);

            assert!(result.is_err());
            match result.unwrap_err() {
                TemplateError::MultiplePrimaryR1 { name } => {
                    assert_eq!(name, "read1");
                }
                _ => panic!("Expected MultiplePrimaryR1 error"),
            }
        }

        #[test]
        fn test_multiple_primary_r2_error() {
            let r2a = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let r2b = make_record(b"read1", PAIRED | LAST_IN_PAIR);

            let result = Template::build(vec![r2a, r2b]);

            assert!(result.is_err());
            match result.unwrap_err() {
                TemplateError::MultiplePrimaryR2 { name } => {
                    assert_eq!(name, "read1");
                }
                _ => panic!("Expected MultiplePrimaryR2 error"),
            }
        }

        #[test]
        fn test_multiple_unpaired_primaries_error() {
            // Two unpaired reads should fail since both go to r1
            let r1 = make_record(b"read1", 0);
            let r2 = make_record(b"read1", 0);

            let result = Template::build(vec![r1, r2]);

            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), TemplateError::MultiplePrimaryR1 { .. }));
        }
    }

    mod edge_case_tests {
        use super::*;

        #[test]
        fn test_empty_template() {
            let template = Template::build(vec![]).unwrap();

            assert!(template.r1.is_none());
            assert!(template.r2.is_none());
            assert!(template.r1_secondaries.is_empty());
            assert!(template.r2_secondaries.is_empty());
            assert!(template.r1_supplementaries.is_empty());
            assert!(template.r2_supplementaries.is_empty());
            assert!(template.name().is_none());
            assert_eq!(template.all_recs().count(), 0);
            assert_eq!(template.primary_recs().count(), 0);
        }

        #[test]
        fn test_name_from_secondary_only() {
            // Template with only secondary alignments (no primaries)
            let sec = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);

            let template = Template::build(vec![sec]).unwrap();

            assert!(template.r1.is_none());
            assert_eq!(template.name(), Some(b"read1".as_slice()));
        }

        #[test]
        fn test_name_from_supplementary_only() {
            // Template with only supplementary alignments (no primaries)
            let supp = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SUPPLEMENTARY);

            let template = Template::build(vec![supp]).unwrap();

            assert!(template.r1.is_none());
            assert_eq!(template.name(), Some(b"read1".as_slice()));
        }

        #[test]
        fn test_default_template() {
            let template = Template::default();

            assert!(template.r1.is_none());
            assert!(template.r2.is_none());
            assert!(template.r1_secondaries.is_empty());
            assert!(template.r2_secondaries.is_empty());
            assert!(template.r1_supplementaries.is_empty());
            assert!(template.r2_supplementaries.is_empty());
        }

        #[test]
        fn test_all_recs_ordering() {
            // Verify the documented ordering of all_recs()
            let r1_primary = make_record(b"read1", PAIRED | FIRST_IN_PAIR);
            let r2_primary = make_record(b"read1", PAIRED | LAST_IN_PAIR);
            let r1_sec = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SECONDARY);
            let r2_sec = make_record(b"read1", PAIRED | LAST_IN_PAIR | SECONDARY);
            let r1_supp = make_record(b"read1", PAIRED | FIRST_IN_PAIR | SUPPLEMENTARY);
            let r2_supp = make_record(b"read1", PAIRED | LAST_IN_PAIR | SUPPLEMENTARY);

            let template =
                Template::build(vec![r1_primary, r2_primary, r1_sec, r2_sec, r1_supp, r2_supp])
                    .unwrap();

            let recs: Vec<_> = template.all_recs().collect();
            assert_eq!(recs.len(), 6);

            // Verify ordering: r1, r2, r1_secondaries, r2_secondaries,
            // r1_supplementaries, r2_supplementaries
            assert!(recs[0].is_first_in_template());
            assert!(!recs[0].is_secondary() && !recs[0].is_supplementary());

            assert!(recs[1].is_last_in_template());
            assert!(!recs[1].is_secondary() && !recs[1].is_supplementary());

            assert!(recs[2].is_first_in_template());
            assert!(recs[2].is_secondary());

            assert!(recs[3].is_last_in_template());
            assert!(recs[3].is_secondary());

            assert!(recs[4].is_first_in_template());
            assert!(recs[4].is_supplementary());

            assert!(recs[5].is_last_in_template());
            assert!(recs[5].is_supplementary());
        }
    }
}
