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
    pub fn build(recs: Vec<Record>) -> Result<Self, TemplateError> {
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
        self.r1
            .as_ref()
            .or(self.r2.as_ref())
            .map(|r| r.qname())
            .or_else(|| self.r1_secondaries.first().map(|r| r.qname()))
            .or_else(|| self.r2_secondaries.first().map(|r| r.qname()))
            .or_else(|| self.r1_supplementaries.first().map(|r| r.qname()))
            .or_else(|| self.r2_supplementaries.first().map(|r| r.qname()))
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
