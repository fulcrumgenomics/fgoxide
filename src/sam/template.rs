//! [`Template`] and [`TemplateIterator`] for grouping alignment records by query name.
//!
//! Ports the equivalent abstractions from `fgbio` (Scala) and `fgpyo` (Python). The
//! source material being ported, pinned to the upstream commits at the time this module
//! was written:
//!
//! - fgbio `Template` (case class + companion object): `Bams.scala` lines 47-178 at
//!   `fulcrumgenomics/fgbio@768d38c0`.
//! - fgbio `templateIterator`: `ZipperBams.scala` line 109 at the same commit.
//! - fgpyo `Template`: `fgpyo/sam/__init__.py` lines 1346-1563 at
//!   `fulcrumgenomics/fgpyo@416f0f64`.
//! - fgpyo `TemplateIterator`: `fgpyo/sam/__init__.py` lines 1564-1581 at the same commit.

use std::io;

use noodles_sam::alignment::RecordBuf;
use noodles_sam::alignment::record::Flags;

use crate::{FgError, Result};

/// All alignment records that share a query name (i.e. belong to the same template).
///
/// A template typically contains the primary R1 and R2 alignments and any number of
/// secondary and supplementary alignments for each end. Unpaired records (those without
/// the [`Flags::SEGMENTED`] flag set) are classified as R1.
///
/// Records that are flagged as both secondary and supplementary are placed in the
/// secondary list, matching `fgbio`'s behavior.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Template {
    /// The query name shared by all records in this template.
    pub name: Vec<u8>,
    /// The primary R1 alignment, if present.
    pub r1: Option<RecordBuf>,
    /// The primary R2 alignment, if present.
    pub r2: Option<RecordBuf>,
    /// Supplementary R1 alignments.
    pub r1_supplementary: Vec<RecordBuf>,
    /// Supplementary R2 alignments.
    pub r2_supplementary: Vec<RecordBuf>,
    /// Secondary R1 alignments.
    pub r1_secondary: Vec<RecordBuf>,
    /// Secondary R2 alignments.
    pub r2_secondary: Vec<RecordBuf>,
}

impl Template {
    /// Builds a [`Template`] from an iterable of records that all share the same query name.
    ///
    /// Returns an error if the input is empty, if records disagree on query name, if a
    /// record has no query name, or if multiple records would occupy the primary R1 or R2
    /// slot.
    pub fn from_records<I>(records: I) -> Result<Self>
    where
        I: IntoIterator<Item = RecordBuf>,
    {
        let mut t = Template::default();
        let mut have_name = false;

        for rec in records {
            let rec_name = rec.name().ok_or(FgError::MissingQueryName)?.to_vec();
            if !have_name {
                t.name = rec_name;
                have_name = true;
            } else if t.name != rec_name {
                return Err(FgError::InconsistentTemplateNames {
                    first: String::from_utf8_lossy(&t.name).into_owned(),
                    second: String::from_utf8_lossy(&rec_name).into_owned(),
                });
            }

            t.add_record(rec)?;
        }

        if !have_name { Err(FgError::EmptyTemplate) } else { Ok(t) }
    }

    /// Returns the total number of records held by this template.
    pub fn len(&self) -> usize {
        self.r1.is_some() as usize
            + self.r2.is_some() as usize
            + self.r1_supplementary.len()
            + self.r2_supplementary.len()
            + self.r1_secondary.len()
            + self.r2_secondary.len()
    }

    /// Returns true if this template holds no records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an iterator over the primary R1 and R2 records.
    pub fn primary_reads(&self) -> impl Iterator<Item = &RecordBuf> {
        self.r1.iter().chain(self.r2.iter())
    }

    /// Returns an iterator over every record held by this template, in the order
    /// primary, supplementary, secondary, R1 before R2 within each category.
    pub fn all_reads(&self) -> impl Iterator<Item = &RecordBuf> {
        self.r1
            .iter()
            .chain(self.r2.iter())
            .chain(self.r1_supplementary.iter())
            .chain(self.r2_supplementary.iter())
            .chain(self.r1_secondary.iter())
            .chain(self.r2_secondary.iter())
    }

    /// Inserts a single record into the appropriate slot. Assumes the query name has
    /// already been validated against the rest of the template.
    fn add_record(&mut self, rec: RecordBuf) -> Result<()> {
        let flags = rec.flags();
        // A record is treated as R1 if it is unpaired or explicitly marked as the first
        // segment. Records without SEGMENTED follow fgbio in landing in the R1 slot.
        let is_r1 = !flags.contains(Flags::SEGMENTED) || flags.contains(Flags::FIRST_SEGMENT);

        // Order matters: a record marked as both secondary and supplementary is treated
        // as secondary, matching fgbio.
        if flags.contains(Flags::SECONDARY) {
            if is_r1 {
                self.r1_secondary.push(rec);
            } else {
                self.r2_secondary.push(rec);
            }
        } else if flags.contains(Flags::SUPPLEMENTARY) {
            if is_r1 {
                self.r1_supplementary.push(rec);
            } else {
                self.r2_supplementary.push(rec);
            }
        } else if is_r1 {
            if self.r1.is_some() {
                return Err(FgError::MultiplePrimaryAlignments {
                    name: String::from_utf8_lossy(&self.name).into_owned(),
                    read: "R1",
                });
            }
            self.r1 = Some(rec);
        } else {
            if self.r2.is_some() {
                return Err(FgError::MultiplePrimaryAlignments {
                    name: String::from_utf8_lossy(&self.name).into_owned(),
                    read: "R2",
                });
            }
            self.r2 = Some(rec);
        }
        Ok(())
    }
}

/// Adapter that groups a queryname-grouped stream of [`RecordBuf`] values into [`Template`]s.
///
/// The underlying iterator must already be queryname-sorted or queryname-grouped: the adapter
/// terminates a template as soon as it sees a record whose query name differs from the previous
/// record. It does not detect the case where the same query name reappears later in the stream
/// after an intervening different name; mis-grouped input will silently produce multiple
/// [`Template`]s for the same name.
///
/// The adapter wraps any iterator yielding [`io::Result<RecordBuf>`], matching what `noodles`
/// readers produce.
pub struct TemplateIterator<I>
where
    I: Iterator<Item = io::Result<RecordBuf>>,
{
    inner: std::iter::Peekable<I>,
    /// An error discovered while peeking the next group's first record. Held back so the
    /// in-progress template is yielded first; surfaced on the following call to `next`.
    pending_err: Option<FgError>,
}

impl<I> TemplateIterator<I>
where
    I: Iterator<Item = io::Result<RecordBuf>>,
{
    /// Wraps an iterator of records, assumed to be queryname-grouped.
    pub fn new(inner: I) -> Self {
        Self { inner: inner.peekable(), pending_err: None }
    }

    /// Peeks the next record's query name. Returns `Some(Ok(name))` if the peek yielded a
    /// named record, `Some(Err(_))` if the peeked record was an error or had no name (in
    /// which case the offending record is also consumed so it is not seen twice), or `None`
    /// if the underlying iterator is exhausted.
    fn peek_name(&mut self) -> Option<Result<Vec<u8>>> {
        match self.inner.peek()? {
            Ok(rec) => match rec.name() {
                Some(n) => Some(Ok(n.to_vec())),
                None => {
                    // Drop the bad record so a later call doesn't see it again.
                    let _ = self.inner.next();
                    Some(Err(FgError::MissingQueryName))
                }
            },
            // Cannot move the error out of the peeked Result; consume it now.
            Err(_) => match self.inner.next() {
                Some(Err(e)) => Some(Err(e.into())),
                _ => unreachable!("peek returned Some(Err); next must yield the same Err"),
            },
        }
    }
}

impl<I> Iterator for TemplateIterator<I>
where
    I: Iterator<Item = io::Result<RecordBuf>>,
{
    type Item = Result<Template>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(e) = self.pending_err.take() {
            return Some(Err(e));
        }

        // Pull the first record of the next template, surfacing any underlying error.
        let first = match self.inner.next()? {
            Ok(rec) => rec,
            Err(e) => return Some(Err(e.into())),
        };

        let name = match first.name() {
            Some(n) => n.to_vec(),
            None => return Some(Err(FgError::MissingQueryName)),
        };

        let mut template = Template { name: name.clone(), ..Template::default() };
        if let Err(e) = template.add_record(first) {
            return Some(Err(e));
        }

        // Peek subsequent records; consume those that share the same query name. Errors
        // discovered during peek are buffered so the completed template is yielded first.
        loop {
            match self.peek_name() {
                Some(Ok(next_name)) if next_name == name => {
                    // Safe to unwrap: peek returned Some(Ok), so next() yields Some(Ok).
                    let rec = match self.inner.next().unwrap() {
                        Ok(rec) => rec,
                        Err(e) => return Some(Err(e.into())),
                    };
                    if let Err(e) = template.add_record(rec) {
                        return Some(Err(e));
                    }
                }
                Some(Ok(_)) => break,
                Some(Err(e)) => {
                    self.pending_err = Some(e);
                    break;
                }
                None => break,
            }
        }

        Some(Ok(template))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noodles_sam::alignment::RecordBuf;
    use noodles_sam::alignment::record::Flags;

    /// Builds a record with the given query name and flags. Other fields are left default.
    fn rec(name: &str, flags: Flags) -> RecordBuf {
        RecordBuf::builder().set_name(name.as_bytes()).set_flags(flags).build()
    }

    /// A read 1, primary alignment.
    fn r1_primary(name: &str) -> RecordBuf {
        rec(name, Flags::SEGMENTED | Flags::FIRST_SEGMENT)
    }

    /// A read 2, primary alignment.
    fn r2_primary(name: &str) -> RecordBuf {
        rec(name, Flags::SEGMENTED | Flags::LAST_SEGMENT)
    }

    fn r1_supplementary(name: &str) -> RecordBuf {
        rec(name, Flags::SEGMENTED | Flags::FIRST_SEGMENT | Flags::SUPPLEMENTARY)
    }

    fn r2_supplementary(name: &str) -> RecordBuf {
        rec(name, Flags::SEGMENTED | Flags::LAST_SEGMENT | Flags::SUPPLEMENTARY)
    }

    fn r1_secondary(name: &str) -> RecordBuf {
        rec(name, Flags::SEGMENTED | Flags::FIRST_SEGMENT | Flags::SECONDARY)
    }

    fn r2_secondary(name: &str) -> RecordBuf {
        rec(name, Flags::SEGMENTED | Flags::LAST_SEGMENT | Flags::SECONDARY)
    }

    fn ok_iter(records: Vec<RecordBuf>) -> impl Iterator<Item = std::io::Result<RecordBuf>> {
        records.into_iter().map(Ok)
    }

    #[test]
    fn paired_primary_only() {
        let t = Template::from_records(vec![r1_primary("q1"), r2_primary("q1")]).unwrap();
        assert_eq!(t.name, b"q1");
        assert!(t.r1.is_some());
        assert!(t.r2.is_some());
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn paired_with_supplementary_and_secondary() {
        let records = vec![
            r1_primary("q1"),
            r2_primary("q1"),
            r1_supplementary("q1"),
            r2_supplementary("q1"),
            r1_secondary("q1"),
            r2_secondary("q1"),
        ];
        let t = Template::from_records(records).unwrap();
        assert_eq!(t.r1_supplementary.len(), 1);
        assert_eq!(t.r2_supplementary.len(), 1);
        assert_eq!(t.r1_secondary.len(), 1);
        assert_eq!(t.r2_secondary.len(), 1);
        assert_eq!(t.len(), 6);
    }

    #[test]
    fn unpaired_record_classifies_as_r1() {
        let t = Template::from_records(vec![rec("q1", Flags::empty())]).unwrap();
        assert!(t.r1.is_some());
        assert!(t.r2.is_none());
    }

    #[test]
    fn secondary_supplementary_goes_to_secondary() {
        // Per fgbio, a record marked as both secondary and supplementary is classified as
        // secondary (the secondary check happens first).
        let flags =
            Flags::SEGMENTED | Flags::FIRST_SEGMENT | Flags::SECONDARY | Flags::SUPPLEMENTARY;
        let t = Template::from_records(vec![r1_primary("q1"), rec("q1", flags)]).unwrap();
        assert_eq!(t.r1_secondary.len(), 1);
        assert!(t.r1_supplementary.is_empty());
    }

    #[test]
    fn empty_input_errors() {
        let err = Template::from_records(Vec::<RecordBuf>::new()).unwrap_err();
        assert!(matches!(err, FgError::EmptyTemplate));
    }

    #[test]
    fn inconsistent_names_error() {
        let err = Template::from_records(vec![r1_primary("q1"), r2_primary("q2")]).unwrap_err();
        assert!(matches!(err, FgError::InconsistentTemplateNames { .. }));
    }

    #[test]
    fn multiple_primaries_error() {
        let err = Template::from_records(vec![r1_primary("q1"), r1_primary("q1")]).unwrap_err();
        assert!(matches!(err, FgError::MultiplePrimaryAlignments { read: "R1", .. }));
    }

    #[test]
    fn missing_name_errors() {
        let no_name =
            RecordBuf::builder().set_flags(Flags::SEGMENTED | Flags::FIRST_SEGMENT).build();
        let err = Template::from_records(vec![no_name]).unwrap_err();
        assert!(matches!(err, FgError::MissingQueryName));
    }

    #[test]
    fn iterator_yields_one_template_per_group() {
        let records = vec![
            r1_primary("q1"),
            r2_primary("q1"),
            r1_primary("q2"),
            r2_primary("q2"),
            r2_supplementary("q2"),
            r1_primary("q3"),
        ];
        let templates: Vec<Template> =
            TemplateIterator::new(ok_iter(records)).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(templates.len(), 3);
        assert_eq!(templates[0].name, b"q1");
        assert_eq!(templates[0].len(), 2);
        assert_eq!(templates[1].name, b"q2");
        assert_eq!(templates[1].len(), 3);
        assert_eq!(templates[2].name, b"q3");
        assert_eq!(templates[2].len(), 1);
    }

    #[test]
    fn iterator_on_empty_input_yields_nothing() {
        let templates: Vec<Template> =
            TemplateIterator::new(ok_iter(vec![])).collect::<Result<Vec<_>>>().unwrap();
        assert!(templates.is_empty());
    }

    #[test]
    fn iterator_propagates_underlying_errors() {
        let items: Vec<std::io::Result<RecordBuf>> =
            vec![Err(std::io::Error::other("boom")), Ok(r1_primary("q1"))];
        let mut it = TemplateIterator::new(items.into_iter());
        let err = it.next().unwrap().unwrap_err();
        assert!(matches!(err, FgError::IoError(_)));
        // Subsequent records still produce a template.
        let t = it.next().unwrap().unwrap();
        assert_eq!(t.name, b"q1");
    }

    #[test]
    fn iterator_propagates_error_mid_template() {
        // An error encountered partway through a queryname group surfaces *after* the
        // in-progress template is yielded; iteration then resumes with the next group.
        let items: Vec<std::io::Result<RecordBuf>> =
            vec![Ok(r1_primary("q1")), Err(std::io::Error::other("boom")), Ok(r2_primary("q2"))];
        let mut it = TemplateIterator::new(items.into_iter());
        let t = it.next().unwrap().unwrap();
        assert_eq!(t.name, b"q1");
        let err = it.next().unwrap().unwrap_err();
        assert!(matches!(err, FgError::IoError(_)));
        let t = it.next().unwrap().unwrap();
        assert_eq!(t.name, b"q2");
    }

    // Regression: when an underlying error follows a fully accumulated template, the
    // completed template must be yielded before the error surfaces.  Currently fails:
    // peek_name returns None for a peeked Err, the loop's None arm consumes the error
    // immediately, and the in-progress template is dropped on the floor.
    #[test]
    fn iterator_yields_completed_template_before_error() {
        let items: Vec<std::io::Result<RecordBuf>> = vec![
            Ok(r1_primary("q1")),
            Ok(r2_primary("q1")),
            Err(std::io::Error::other("boom")),
        ];
        let mut it = TemplateIterator::new(items.into_iter());
        let t = it.next().unwrap().expect("template q1 should be yielded before the error");
        assert_eq!(t.name, b"q1");
        assert_eq!(t.len(), 2);
        let err = it.next().unwrap().unwrap_err();
        assert!(matches!(err, FgError::IoError(_)));
    }

    // Regression: a nameless record encountered after a fully accumulated template should
    // surface as MissingQueryName *after* the template is yielded, not in place of it.
    // Currently fails for the same reason as above.
    #[test]
    fn iterator_yields_completed_template_before_missing_name_error() {
        let no_name =
            RecordBuf::builder().set_flags(Flags::SEGMENTED | Flags::FIRST_SEGMENT).build();
        let items: Vec<std::io::Result<RecordBuf>> =
            vec![Ok(r1_primary("q1")), Ok(r2_primary("q1")), Ok(no_name)];
        let mut it = TemplateIterator::new(items.into_iter());
        let t = it.next().unwrap().expect("template q1 should be yielded before the error");
        assert_eq!(t.name, b"q1");
        let err = it.next().unwrap().unwrap_err();
        assert!(matches!(err, FgError::MissingQueryName));
    }

    #[test]
    fn iterator_does_not_collapse_repeated_names_across_groups() {
        // If the input is mis-grouped, each contiguous run with the same name becomes a
        // separate Template. This documents (rather than enforces) the assumption.
        let records = vec![r1_primary("q1"), r1_primary("q2"), r1_primary("q1")];
        let templates: Vec<Template> =
            TemplateIterator::new(ok_iter(records)).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(templates.len(), 3);
    }
}
