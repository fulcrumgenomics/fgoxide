//! Helpers and type definitions for extended I/O functionality
//!
//! The `io` module contains a number of types and functions to assist with common
//! I/O activities, such a slurping a file by lines, or writing a collection of `Serializable`
//! objects to a path.
//!
//! The two core parts of this module are teh [`Io`] and [`DelimFile`] structs. These structs provide
//! methods for reading and writing to files that transparently handle compression based on the
//! file extension of the path given to the methods.
//!
//! ## Example
//!
//! ```rust
//! use std::{
//!     default::Default,
//!     error::Error
//! };
//! use fgoxide::io::{Io, DelimFile};
//! use serde::{Deserialize, Serialize};
//! use tempfile::TempDir;
//!
//! #[derive(Debug, Deserialize)]
//! struct SampleInfo {
//!     sample_name: String,
//!     count: usize,
//!     gene: String
//! }
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let tempdir = TempDir::new()?;
//!     // Picks up gzip compression from the extension when the `gz` feature is enabled;
//!     // the test below uses a plain extension so the example runs regardless of features.
//!     let path = tempdir.path().join("test_file.csv");
//!
//!     let io = Io::default();
//!     let lines = ["sample_name,count,gene", "sample1,100,SEPT14", "sample2,5,MIC"];
//!     io.write_lines(&path, lines.iter())?;
//!
//!     let delim = DelimFile::default();
//!     let samples: Vec<SampleInfo> = delim.read(&path, b',', false)?;
//!     assert_eq!(samples.len(), 2);
//!     assert_eq!(&samples[1].sample_name, "sample2");
//!     Ok(())
//! }
//! ```
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::marker::PhantomData;
use std::path::Path;

use crate::{FgError, Result};
use csv::{
    DeserializeRecordsIntoIter, QuoteStyle, ReaderBuilder, StringRecord, Writer, WriterBuilder,
};
use niffler::Level;
use niffler::send::compression::Format as NifflerFormat;
use serde::{Serialize, de::DeserializeOwned};

const GZIP_EXTENSIONS: [&str; 2] = ["gz", "bgz"];

/// The compression codec to apply when writing a file. Selected from the path extension by
/// [`compression_for_path`]; passed to [`Io::new_writer`] (indirectly) to choose the writer
/// backend. `Gzip` and `Bgzf` both denote a BGZF-encoded gzip stream that any plain gzip
/// reader can still inflate. Distinguishing them is a hook for future code that might want to
/// vary block parameters per extension; the current writer treats them identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// No compression (plain bytes).
    None,
    /// `.gz`: BGZF-encoded gzip (via `gzp::BgzfSyncWriter`).
    Gzip,
    /// `.bgz`: BGZF-encoded gzip (via `gzp::BgzfSyncWriter`).
    Bgzf,
    /// `.bz2`: bzip2.
    Bzip2,
    /// `.xz`: xz / lzma.
    Xz,
    /// `.zst`: zstandard.
    Zstd,
}

impl Compression {
    /// Static identifier used in error messages when a codec is requested but its feature
    /// flag is disabled at compile time. Only referenced from the disabled-feature arms in
    /// [`Io::build_writer`], so it picks up `dead_code` warnings when every codec is on.
    #[allow(dead_code)]
    fn codec_name(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gz",
            Compression::Bgzf => "bgz",
            Compression::Bzip2 => "bz2",
            Compression::Xz => "xz",
            Compression::Zstd => "zst",
        }
    }
}

/// Returns the compression codec implied by `path`'s file extension. Anything unrecognised
/// (including no extension) maps to [`Compression::None`].
#[must_use]
pub fn compression_for_path<P: AsRef<Path>>(p: P) -> Compression {
    match p.as_ref().extension().and_then(|e| e.to_str()) {
        Some("gz") => Compression::Gzip,
        Some("bgz") => Compression::Bgzf,
        Some("bz2") => Compression::Bzip2,
        Some("xz") => Compression::Xz,
        Some("zst") => Compression::Zstd,
        _ => Compression::None,
    }
}

/// The default buffer size when creating buffered readers/writers
const BUFFER_SIZE: usize = 64 * 1024;

/// Clamp a raw compression level into a [`niffler::Level`] variant. `niffler::Level` is
/// `Zero..=TwentyOne`, matching zstd's positive-level range. Anything below zero clamps to
/// `Zero`; anything above twenty-one clamps to `TwentyOne`. Codecs whose native range is
/// narrower (gzip 0..=9, bzip2 1..=9) are clamped further inside the dispatch arms below.
///
/// Only referenced from codec arms that go through niffler (bz2, xz). Marked allow-dead
/// so a build with only the gz / zstd features (which both bypass niffler's writer) does
/// not warn.
#[allow(dead_code)]
fn niffler_level_from_i32(level: i32) -> Level {
    match level {
        i32::MIN..=0 => Level::Zero,
        1 => Level::One,
        2 => Level::Two,
        3 => Level::Three,
        4 => Level::Four,
        5 => Level::Five,
        6 => Level::Six,
        7 => Level::Seven,
        8 => Level::Eight,
        9 => Level::Nine,
        10 => Level::Ten,
        11 => Level::Eleven,
        12 => Level::Twelve,
        13 => Level::Thirteen,
        14 => Level::Fourteen,
        15 => Level::Fifteen,
        16 => Level::Sixteen,
        17 => Level::Seventeen,
        18 => Level::Eighteen,
        19 => Level::Nineteen,
        20 => Level::Twenty,
        _ => Level::TwentyOne,
    }
}

fn map_niffler_err(e: niffler::Error) -> FgError {
    match e {
        niffler::Error::IOError(io) => FgError::IoError(io),
        niffler::Error::FileTooShort => FgError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "input is shorter than the 5-byte compression magic-byte window",
        )),
        niffler::Error::FeatureDisabled => {
            FgError::UnsupportedCodec { codec: "compression codec disabled in niffler" }
        }
    }
}

/// Unit-struct that contains associated functions for reading and writing Structs to/from
/// unstructured files.
pub struct Io {
    /// Raw user-supplied compression level. Stored unclamped; each codec's writer clamps to
    /// its own native range inside [`Io::new_writer`]. Negative values are meaningful for
    /// zstd's "fast mode" (-7..=-1) and become 0 / minimum for every other codec.
    ///
    /// `allow(dead_code)`: when every codec feature is disabled there's no writer arm that
    /// reads this field, but it still needs to exist so the public `Io::with_level` API
    /// stays uniform across feature configurations.
    #[allow(dead_code)]
    compression: i32,
    buffer_size: usize,
}

/// Returns a Default implementation that will compress to level 5 (gzip-equivalent middle).
impl Default for Io {
    fn default() -> Self {
        Io::with_level(5, BUFFER_SIZE)
    }
}

impl Io {
    /// Creates a new `Io` instance with the given raw compression level and read/write
    /// buffer size. The level is stored uninterpreted; the per-codec writer clamps it to its
    /// own native range when a writer is constructed. For example, `with_level(15, _)` will
    /// produce level 15 for `.zst` writes and level 9 for `.gz` writes. Negative values
    /// (-7..=-1) reach zstd's fast-mode levels and have no effect on other codecs.
    pub fn with_level(level: i32, buffer_size: usize) -> Io {
        Io { compression: level, buffer_size }
    }

    /// Returns true if the path ends with a recognized GZIP file extension.
    ///
    /// Retained for backwards compatibility; for new code prefer [`compression_for_path`],
    /// which covers every codec the writer understands rather than just gzip.
    #[must_use]
    #[deprecated(
        since = "0.8.0",
        note = "use `compression_for_path` instead; \
        this predicate predates magic-byte detection and lumps `.bgz` in with `.gz`"
    )]
    pub fn is_gzip_path<P: AsRef<Path>>(p: P) -> bool {
        if let Some(ext) = p.as_ref().extension() {
            match ext.to_str() {
                Some(x) => GZIP_EXTENSIONS.contains(&x),
                None => false,
            }
        } else {
            false
        }
    }

    /// Opens a file for reading. Compression (gzip, bzip2, xz, zstd) is detected from the
    /// file's magic bytes; the path extension is ignored. `.bgz` files are read as gzip.
    ///
    /// For files shorter than the 5-byte sniff window the entire content is read into memory
    /// once and replayed; this avoids the FIFO byte-loss footgun of reopening the path.
    pub fn new_reader<P>(&self, p: P) -> Result<Box<dyn BufRead + Send>>
    where
        P: AsRef<Path>,
    {
        let path = p.as_ref();
        let file = File::open(path).map_err(FgError::IoError)?;
        let buffered = BufReader::with_capacity(self.buffer_size, file);
        match niffler::send::get_reader(Box::new(buffered)) {
            Ok((reader, format)) => {
                self.warn_on_extension_mismatch(path, format);
                Ok(Box::new(BufReader::with_capacity(self.buffer_size, reader)))
            }
            // File shorter than the 5-byte sniff window: read the remaining bytes into a
            // Vec and serve from memory. Reopens the path, so this is only honest for
            // regular files; FIFOs whose writer side already closed below the sniff
            // threshold see an empty replay, but that case is exotic enough not to design
            // around here.
            Err(niffler::Error::FileTooShort) => {
                use std::io::Read;
                let mut file = File::open(path).map_err(FgError::IoError)?;
                let mut bytes = Vec::with_capacity(5);
                file.read_to_end(&mut bytes).map_err(FgError::IoError)?;
                Ok(Box::new(std::io::Cursor::new(bytes)))
            }
            Err(other) => Err(map_niffler_err(other)),
        }
    }

    /// Emit a `log::warn!` when the magic-byte-detected codec disagrees with the path
    /// extension (e.g. a `.gz` file whose magic bytes say zstd). Magic bytes win for the
    /// actual decode; this is purely advisory.
    fn warn_on_extension_mismatch(&self, path: &Path, detected: NifflerFormat) {
        let from_ext = match compression_for_path(path) {
            Compression::None => NifflerFormat::No,
            Compression::Gzip | Compression::Bgzf => NifflerFormat::Gzip,
            Compression::Bzip2 => NifflerFormat::Bzip,
            Compression::Xz => NifflerFormat::Lzma,
            Compression::Zstd => NifflerFormat::Zstd,
        };
        if from_ext != detected {
            log::warn!(
                "compression mismatch reading {}: extension implies {:?}, magic bytes say {:?}",
                path.display(),
                from_ext,
                detected,
            );
        }
    }

    /// Opens a file for writing. The output codec is chosen from the path extension:
    /// `.gz`/`.bgz` produce BGZF-encoded gzip (via `gzp::BgzfSyncWriter`, so output stays
    /// readable by every tabix/htslib-flavoured tool), `.bz2` produces bzip2, `.xz`
    /// produces xz, `.zst` produces zstd (with native support for zstd's negative
    /// "fast mode" levels), anything else uncompressed.
    ///
    /// Codecs whose feature is disabled at compile time return
    /// [`FgError::UnsupportedCodec`].
    pub fn new_writer<P>(&self, p: P) -> Result<BufWriter<Box<dyn Write + Send>>>
    where
        P: AsRef<Path>,
    {
        let codec = compression_for_path(&p);
        let file = File::create(p.as_ref()).map_err(FgError::IoError)?;
        let inner = self.build_writer(file, codec)?;
        Ok(BufWriter::with_capacity(self.buffer_size, inner))
    }

    /// Wrap `file` in the writer pipeline for `codec`, clamping the stored level to the
    /// codec's native range. Split out from [`Io::new_writer`] so the dispatch table is
    /// easy to read and so feature-gated arms can be `cfg`'d cleanly.
    fn build_writer(&self, file: File, codec: Compression) -> Result<Box<dyn Write + Send>> {
        match codec {
            Compression::None => Ok(Box::new(file)),

            #[cfg(feature = "gz")]
            Compression::Gzip | Compression::Bgzf => {
                // gzip/BGZF range is 0..=9; clamp raw level into that window before handing
                // it to gzp. Negative input clamps to zero (= store, no deflate work).
                let level = self.compression.clamp(0, 9) as u32;
                let bgzf = gzp::BgzfSyncWriter::new(file, gzp::Compression::new(level));
                Ok(Box::new(bgzf))
            }
            #[cfg(not(feature = "gz"))]
            Compression::Gzip | Compression::Bgzf => {
                Err(FgError::UnsupportedCodec { codec: codec.codec_name() })
            }

            #[cfg(feature = "bz2")]
            Compression::Bzip2 => {
                // bzip2 valid levels are 1..=9; niffler enforces this internally too but
                // we clamp here so a `with_level(0, _)` user gets level 1 rather than
                // niffler's internal default.
                let level = niffler_level_from_i32(self.compression.clamp(1, 9));
                let raw: Box<dyn Write + Send> = Box::new(file);
                niffler::send::get_writer(raw, NifflerFormat::Bzip, level).map_err(map_niffler_err)
            }
            #[cfg(not(feature = "bz2"))]
            Compression::Bzip2 => Err(FgError::UnsupportedCodec { codec: codec.codec_name() }),

            #[cfg(feature = "xz")]
            Compression::Xz => {
                // xz/lzma preset levels are 0..=9.
                let level = niffler_level_from_i32(self.compression.clamp(0, 9));
                let raw: Box<dyn Write + Send> = Box::new(file);
                niffler::send::get_writer(raw, NifflerFormat::Lzma, level).map_err(map_niffler_err)
            }
            #[cfg(not(feature = "xz"))]
            Compression::Xz => Err(FgError::UnsupportedCodec { codec: codec.codec_name() }),

            #[cfg(feature = "zstd")]
            Compression::Zstd => {
                // zstd supports negative "fast mode" levels (-7..=-1) and positive levels
                // up to 22. Bypass niffler entirely so the raw i32 reaches the codec; this
                // is the only way to express the negative range, since niffler's `Level`
                // enum starts at `Zero`. Clamp into zstd's documented range so a user
                // who passes -8 or 23 gets a defined boundary rather than a codec error.
                let level = self.compression.clamp(-7, 22);
                let encoder =
                    zstd::stream::write::Encoder::new(file, level).map_err(FgError::IoError)?;
                Ok(Box::new(encoder.auto_finish()))
            }
            #[cfg(not(feature = "zstd"))]
            Compression::Zstd => Err(FgError::UnsupportedCodec { codec: codec.codec_name() }),
        }
    }

    /// Reads lines from a file into a Vec
    pub fn read_lines<P>(&self, p: P) -> Result<Vec<String>>
    where
        P: AsRef<Path>,
    {
        let r = self.new_reader(p)?;
        let mut v = Vec::new();
        for result in r.lines() {
            v.push(result.map_err(FgError::IoError)?);
        }

        Ok(v)
    }

    /// Writes all the lines from an iterable of string-like values to a file, separated by new lines.
    pub fn write_lines<P, S>(&self, p: P, lines: impl IntoIterator<Item = S>) -> Result<()>
    where
        P: AsRef<Path>,
        S: AsRef<str>,
    {
        let mut out = self.new_writer(p)?;
        for line in lines {
            out.write_all(line.as_ref().as_bytes()).map_err(FgError::IoError)?;
            out.write_all(b"\n").map_err(FgError::IoError)?;
        }

        out.flush().map_err(FgError::IoError)
    }
}

/// A struct that wraps a csv `Reader` and provides methods for reading one record at a time.
/// It also implements `Iterator`.
pub struct DelimFileReader<D: DeserializeOwned> {
    record_iter: DeserializeRecordsIntoIter<Box<dyn BufRead + Send>, D>,
    header: StringRecord,
}

impl<D: DeserializeOwned> DelimFileReader<D> {
    /// Returns a new `DelimFileReader` that will read records from the given reader with the given
    /// delimiter and quoting. Assumes the input file has a header row.
    pub fn new(reader: Box<dyn BufRead + Send>, delimiter: u8, quote: bool) -> Result<Self> {
        let mut csv_reader = ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(true)
            .quoting(quote)
            .from_reader(reader);

        // NB: csv_reader.has_header() does not actually check for existence of a header, but only
        // checks that the reader is configured to read a header.

        // Empty files are valid but will have empty headers.
        // So validate the header only if one is found for a non-empty file.
        let header = csv_reader.headers().map_err(FgError::ConversionError)?.to_owned();
        if !header.is_empty() {
            Self::validate_header(&header, delimiter)?
        }

        let record_iter = csv_reader.into_deserialize();
        Ok(Self { record_iter, header })
    }

    /// Returns the contents of the header row.
    pub fn header(&self) -> &StringRecord {
        &self.header
    }

    /// Returns the next record from the underlying reader.
    pub fn read(&mut self) -> Option<Result<D>> {
        self.record_iter.next().map(|result| result.map_err(FgError::ConversionError))
    }

    fn validate_header(header: &StringRecord, delimiter: u8) -> Result<()> {
        let delim = String::from_utf8(vec![delimiter]).unwrap();
        let found_header_parts: HashSet<&str> = header.iter().collect();
        let expected_header_parts = serde_aux::prelude::serde_introspect::<D>();

        // Expected header fields must be a _subset_ of found header fields
        let ok = expected_header_parts.iter().all(|field| found_header_parts.contains(field));
        if !ok {
            let found_header_parts: Vec<&str> = header.iter().collect();
            return Err(FgError::DelimFileHeaderError {
                expected: expected_header_parts.join(&delim),
                found: found_header_parts.join(&delim),
            });
        }

        Ok(())
    }
}

impl<D: DeserializeOwned> Iterator for DelimFileReader<D> {
    type Item = Result<D>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read()
    }
}

/// A struct that wraps a csv `Writer` and provides methods for writing single records as well as
/// multiple records from an iterator.
pub struct DelimFileWriter<S: Serialize> {
    csv_writer: Writer<BufWriter<Box<dyn Write + Send>>>,
    _data: PhantomData<S>,
}

impl<S: Serialize> DelimFileWriter<S> {
    /// Returns a new `DelimFileWriter` that writes to the given `writer` with the given delimiter
    /// and quoting. The output file will have a header row.
    pub fn new(writer: BufWriter<Box<dyn Write + Send>>, delimiter: u8, quote: bool) -> Self {
        let csv_writer = WriterBuilder::new()
            .delimiter(delimiter)
            .has_headers(true)
            .quote_style(if quote { QuoteStyle::Necessary } else { QuoteStyle::Never })
            .from_writer(writer);
        Self { csv_writer, _data: PhantomData }
    }

    /// Writes a single record to the underlying writer.
    pub fn write(&mut self, rec: &S) -> Result<()> {
        self.csv_writer.serialize(rec).map_err(FgError::ConversionError)
    }

    /// Writes all records from `iter` to the underlying writer, in order.
    pub fn write_all(&mut self, iter: impl IntoIterator<Item = S>) -> Result<()> {
        for rec in iter {
            self.write(&rec)?;
        }
        self.flush()?;
        Ok(())
    }

    /// Flushes the underlying writer.
    /// Note: this is not strictly necessary as the underlying writer is flushed automatically
    /// on `Drop`.
    pub fn flush(&mut self) -> Result<()> {
        self.csv_writer.flush().map_err(FgError::IoError)
    }
}

/// Unit-struct that contains associated functions for reading and writing Structs to/from
/// delimited files.  Structs should use serde's Serialize/Deserialize derive macros in
/// order to be used with these functions.
pub struct DelimFile {
    io: Io,
}

/// Generates a default implementation that uses the default Io instance
impl Default for DelimFile {
    fn default() -> Self {
        DelimFile { io: Io::default() }
    }
}

impl DelimFile {
    /// Returns a new `DelimFileReader` instance that reads from the given path, opened with this
    /// `DelimFile`'s `Io` instance.
    pub fn new_reader<D: DeserializeOwned, P: AsRef<Path>>(
        &self,
        path: P,
        delimiter: u8,
        quote: bool,
    ) -> Result<DelimFileReader<D>> {
        let file = self.io.new_reader(path)?;
        DelimFileReader::new(file, delimiter, quote)
    }

    /// Returns a new `DelimFileWriter` instance that writes to the given path, opened with this
    /// `DelimFile`'s `Io` instance.
    pub fn new_writer<S: Serialize, P: AsRef<Path>>(
        &self,
        path: P,
        delimiter: u8,
        quote: bool,
    ) -> Result<DelimFileWriter<S>> {
        let file = self.io.new_writer(path)?;
        Ok(DelimFileWriter::new(file, delimiter, quote))
    }

    /// Writes a series of one or more structs to a delimited file.  If `quote` is true then fields
    /// will be quoted as necessary, otherwise they will never be quoted.
    pub fn write<S, P>(
        &self,
        path: P,
        recs: impl IntoIterator<Item = S>,
        delimiter: u8,
        quote: bool,
    ) -> Result<()>
    where
        S: Serialize,
        P: AsRef<Path>,
    {
        self.new_writer(path, delimiter, quote)?.write_all(recs)
    }

    /// Writes structs implementing `[Serialize]` to a file with tab separators between fields.
    pub fn write_tsv<S, P>(&self, path: P, recs: impl IntoIterator<Item = S>) -> Result<()>
    where
        S: Serialize,
        P: AsRef<Path>,
    {
        self.write(path, recs, b'\t', true)
    }

    /// Writes structs implementing `[Serialize]` to a file with comma separators between fields.
    pub fn write_csv<S, P>(&self, path: P, recs: impl IntoIterator<Item = S>) -> Result<()>
    where
        S: Serialize,
        P: AsRef<Path>,
    {
        self.write(path, recs, b',', true)
    }

    /// Reads structs implementing `[Deserialize]` from a file with the given separators between fields.
    /// If `quote` is true then fields surrounded by quotes are parsed, otherwise quotes are not
    /// considered.
    pub fn read<D, P>(&self, path: P, delimiter: u8, quote: bool) -> Result<Vec<D>>
    where
        D: DeserializeOwned,
        P: AsRef<Path>,
    {
        self.new_reader(path, delimiter, quote)?.collect()
    }

    /// Reads structs implementing `[Deserialize]` from a file with tab separators between fields.
    pub fn read_tsv<D, P>(&self, path: P) -> Result<Vec<D>>
    where
        D: DeserializeOwned,
        P: AsRef<Path>,
    {
        self.read(path, b'\t', true)
    }

    /// Reads structs implementing `[Deserialize]` from a file with tab separators between fields.
    pub fn read_csv<D, P>(&self, path: P) -> Result<Vec<D>>
    where
        D: DeserializeOwned,
        P: AsRef<Path>,
    {
        self.read(path, b',', true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{DelimFile, Io};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    /// Record type used in testing DelimFile
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Rec {
        s: String,
        i: usize,
        b: bool,
        o: Option<f64>,
    }

    // Trickier record types in which fields are skipped in de/serialization
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct RecWithSkipDe {
        s: String,
        i: usize,
        b: bool,
        #[serde(skip_deserializing)]
        o: Option<f64>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct RecWithSkipSe {
        s: String,
        i: usize,
        b: bool,
        #[serde(skip_serializing)]
        o: Option<f64>,
    }

    #[test]
    fn test_reading_and_writing_lines_to_file() {
        let lines = vec!["foo", "bar,splat,whee", "baz\twhoopsie"];
        let tempdir = TempDir::new().unwrap();
        let f1 = tempdir.path().join("strs.txt");
        let f2 = tempdir.path().join("Strings.txt");

        let io = Io::default();
        io.write_lines(&f1, &lines).unwrap();
        let strings: Vec<String> = lines.iter().map(|l| l.to_string()).collect();
        io.write_lines(&f2, strings).unwrap();

        let r1 = io.read_lines(&f1).unwrap();
        let r2 = io.read_lines(&f2).unwrap();

        assert_eq!(r1, lines);
        assert_eq!(r2, lines);
    }

    #[cfg(feature = "gz")]
    #[test]
    fn test_reading_and_writing_gzip_files() {
        let lines = vec!["foo", "bar", "baz"];
        let tempdir = TempDir::new().unwrap();
        let text = tempdir.path().join("text.txt");
        let gzipped = tempdir.path().join("gzipped.txt.gz");

        let io = Io::default();
        io.write_lines(&text, lines.iter()).unwrap();
        io.write_lines(&gzipped, lines.iter()).unwrap();

        let r1 = io.read_lines(&text).unwrap();
        let r2 = io.read_lines(&gzipped).unwrap();

        assert_eq!(r1, lines);
        assert_eq!(r2, lines);

        // Also check that we actually wrote gzipped data to the gzip file!
        assert_ne!(text.metadata().unwrap().len(), gzipped.metadata().unwrap().len());
    }

    #[rstest::rstest]
    #[cfg_attr(feature = "gz", case::gz("gzipped.txt.gz", true))]
    #[cfg_attr(feature = "gz", case::bgz("bgzipped.txt.bgz", true))]
    #[cfg_attr(feature = "bz2", case::bz2("compressed.txt.bz2", true))]
    #[cfg_attr(feature = "xz", case::xz("compressed.txt.xz", true))]
    #[cfg_attr(feature = "zstd", case::zst("compressed.txt.zst", true))]
    #[case::plain("plain.txt", false)]
    fn test_round_trip_all_compression_formats(
        #[case] file_name: &str,
        #[case] expect_compressed_smaller: bool,
    ) {
        let lines: Vec<String> = (0..200).map(|i| format!("line-{i}-aaaaaaaaaaaaaaaa")).collect();
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join(file_name);
        let plain = tempdir.path().join("plain-reference.txt");

        let io = Io::default();
        io.write_lines(&path, &lines).unwrap();
        io.write_lines(&plain, &lines).unwrap();

        let read_back = io.read_lines(&path).unwrap();
        assert_eq!(read_back, lines, "round-trip failed for {file_name}");

        if expect_compressed_smaller {
            assert!(
                path.metadata().unwrap().len() < plain.metadata().unwrap().len(),
                "{file_name} should be smaller than the uncompressed reference"
            );
        }
    }

    // Requires both gz (so the .gz path-extension writes BGZF that we re-read) and zstd
    // (so we can plant a zstd payload at a .gz path to verify magic-bytes-win behaviour).
    #[cfg(all(feature = "gz", feature = "zstd"))]
    #[rstest::rstest]
    #[case::zstd_payload_in_gz_path("liar.gz", Some(NifflerFormat::Zstd))]
    #[case::plain_payload_in_zst_path("liar.zst", None)]
    fn test_magic_bytes_override_extension(
        #[case] file_name: &str,
        #[case] write_as: Option<NifflerFormat>,
    ) {
        let lines = vec!["alpha", "beta", "gamma"];
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join(file_name);

        match write_as {
            Some(format) => {
                let raw = std::fs::File::create(&path).unwrap();
                let mut w = niffler::send::get_writer(Box::new(raw), format, Level::Five).unwrap();
                for l in &lines {
                    writeln!(w, "{l}").unwrap();
                }
            }
            None => std::fs::write(&path, b"alpha\nbeta\ngamma\n").unwrap(),
        }

        let read_back = Io::default().read_lines(&path).unwrap();
        assert_eq!(read_back, lines);
    }

    #[cfg(feature = "gz")]
    #[test]
    fn test_multi_member_gzip_round_trip() {
        // Concatenated gzip members (as produced by `cat a.gz b.gz` or bgzip output) must
        // read back as a single stream. Relies on niffler routing gzip through MultiGzDecoder.
        let tempdir = TempDir::new().unwrap();
        let part1 = tempdir.path().join("part1.gz");
        let part2 = tempdir.path().join("part2.gz");
        let combined = tempdir.path().join("combined.gz");

        let io = Io::default();
        io.write_lines(&part1, ["line1", "line2"]).unwrap();
        io.write_lines(&part2, ["line3", "line4"]).unwrap();

        let mut bytes = std::fs::read(&part1).unwrap();
        bytes.extend(std::fs::read(&part2).unwrap());
        std::fs::write(&combined, &bytes).unwrap();

        let lines = io.read_lines(&combined).unwrap();
        assert_eq!(lines, vec!["line1", "line2", "line3", "line4"]);
    }

    #[rstest::rstest]
    #[case::empty(b"" as &[u8], &[] as &[&str])]
    #[case::below_sniff_window(b"x", &["x"])]
    fn test_read_below_sniff_window(#[case] bytes: &[u8], #[case] expected: &[&str]) {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("tiny.txt");
        std::fs::write(&path, bytes).unwrap();
        let lines = Io::default().read_lines(&path).unwrap();
        assert_eq!(lines, expected);
    }

    #[rstest::rstest]
    #[case("a.gz", true)]
    #[case("a.bgz", true)]
    #[case("a.zst", false)]
    #[case("a.txt", false)]
    #[case("noext", false)]
    fn test_is_gzip_path(#[case] path: &str, #[case] expected: bool) {
        #[allow(deprecated)]
        let got = Io::is_gzip_path(Path::new(path));
        assert_eq!(got, expected);
    }

    #[rstest::rstest]
    #[case("a.gz", Compression::Gzip)]
    #[case("a.bgz", Compression::Bgzf)]
    #[case("a.bz2", Compression::Bzip2)]
    #[case("a.xz", Compression::Xz)]
    #[case("a.zst", Compression::Zstd)]
    #[case("a.txt", Compression::None)]
    #[case("noext", Compression::None)]
    fn test_compression_for_path(#[case] path: &str, #[case] expected: Compression) {
        assert_eq!(compression_for_path(Path::new(path)), expected);
    }

    /// Both ends of each codec's level range must round-trip cleanly. We don't assert
    /// "higher level produces strictly smaller output": on small or already-near-optimal
    /// payloads, level 9 can edge out level 1 by a few bytes either way (zstd's higher
    /// levels in particular trade off differently). The point of this test is that the
    /// per-codec clamp in `Io::build_writer` doesn't break correctness at the boundaries.
    #[cfg(any(feature = "gz", feature = "bz2", feature = "xz", feature = "zstd"))]
    #[rstest::rstest]
    #[cfg_attr(feature = "gz", case::gz("level.txt.gz"))]
    #[cfg_attr(feature = "gz", case::bgz("level.txt.bgz"))]
    #[cfg_attr(feature = "bz2", case::bz2("level.txt.bz2"))]
    #[cfg_attr(feature = "xz", case::xz("level.txt.xz"))]
    #[cfg_attr(feature = "zstd", case::zst("level.txt.zst"))]
    fn test_round_trip_at_level_boundaries(#[case] file_name: &str) {
        let lines: Vec<String> = (0..500).map(|i| format!("line-{i}-aaaaaaaaaaaaaaaa")).collect();
        let tempdir = TempDir::new().unwrap();
        let low_path = tempdir.path().join(format!("low-{file_name}"));
        let high_path = tempdir.path().join(format!("high-{file_name}"));

        // level=1 maps to each codec's minimum useful level; level=9 saturates the
        // gzip/bzip range and stays well-defined for xz/zstd via the niffler clamp.
        Io::with_level(1, BUFFER_SIZE).write_lines(&low_path, &lines).unwrap();
        Io::with_level(9, BUFFER_SIZE).write_lines(&high_path, &lines).unwrap();

        let io = Io::default();
        assert_eq!(io.read_lines(&low_path).unwrap(), lines);
        assert_eq!(io.read_lines(&high_path).unwrap(), lines);
    }

    /// BGZF files must end with the 28-byte EOF marker block (an empty BGZF block).
    /// tabix/htslib/IGV use this as a "complete file" sentinel; gzp writes it on `Drop`.
    /// See SAM spec §4.1.2.
    #[cfg(feature = "gz")]
    #[test]
    fn test_bgzf_eof_block_marker() {
        const BGZF_EOF: [u8; 28] = [
            0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43,
            0x02, 0x00, 0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("eof.txt.gz");
        Io::default().write_lines(&path, ["hello", "world"]).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            bytes.ends_with(&BGZF_EOF),
            ".gz output is missing the BGZF EOF block marker (tabix/htslib won't see it as complete)"
        );
    }

    /// zstd's negative "fast mode" levels (-7..=-1) are reachable only by bypassing
    /// niffler. Round-trip via the direct zstd encoder path in `build_writer`.
    #[cfg(feature = "zstd")]
    #[test]
    fn test_negative_zstd_level_round_trip() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("fast.txt.zst");
        let lines: Vec<String> = (0..200).map(|i| format!("line-{i}-aaaaaaaaaaaaaaaa")).collect();
        Io::with_level(-5, BUFFER_SIZE).write_lines(&path, &lines).unwrap();
        let read_back = Io::default().read_lines(&path).unwrap();
        assert_eq!(read_back, lines);
    }

    /// Concatenated zstd frames must read back as one stream (matches `cat a.zst b.zst`).
    #[cfg(feature = "zstd")]
    #[test]
    fn test_concatenated_zstd_frames_round_trip() {
        let tempdir = TempDir::new().unwrap();
        let part1 = tempdir.path().join("p1.zst");
        let part2 = tempdir.path().join("p2.zst");
        let combined = tempdir.path().join("combined.zst");
        let io = Io::default();
        io.write_lines(&part1, ["line1", "line2"]).unwrap();
        io.write_lines(&part2, ["line3", "line4"]).unwrap();
        let mut bytes = std::fs::read(&part1).unwrap();
        bytes.extend(std::fs::read(&part2).unwrap());
        std::fs::write(&combined, &bytes).unwrap();
        let lines = io.read_lines(&combined).unwrap();
        assert_eq!(lines, vec!["line1", "line2", "line3", "line4"]);
    }

    /// Truncating a compressed stream mid-payload must surface as a clean error, not a
    /// panic. We only assert "fails" because each codec maps truncation to its own io
    /// error kind.
    #[cfg(feature = "gz")]
    #[test]
    fn test_truncated_gz_returns_error() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("trunc.txt.gz");
        let io = Io::default();
        let lines: Vec<String> = (0..200).map(|i| format!("line-{i}-aaaaaaaaaaaaaaaa")).collect();
        io.write_lines(&path, &lines).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        // Drop the last 16 bytes to cut into the gzip trailer / final deflate block.
        std::fs::write(&path, &bytes[..bytes.len() - 16]).unwrap();
        assert!(io.read_lines(&path).is_err());
    }

    /// Asking for a codec whose feature isn't enabled should produce
    /// `FgError::UnsupportedCodec`, not a panic. Exercises the disabled-feature arms in
    /// `Io::build_writer`. Only meaningful when at least one codec is off.
    #[cfg(not(feature = "xz"))]
    #[test]
    fn test_disabled_codec_maps_to_unsupported() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("nope.txt.xz");
        match Io::default().new_writer(&path) {
            Err(FgError::UnsupportedCodec { codec }) => assert_eq!(codec, "xz"),
            Err(other) => panic!("expected UnsupportedCodec, got {other:?}"),
            Ok(_) => panic!("expected UnsupportedCodec, got Ok"),
        }
    }

    /// `Io::new_reader` returns a `BufRead`; iterating `.lines()` is the contract every
    /// downstream relies on. Cover plain and compressed paths if available.
    #[test]
    fn test_reader_implements_bufread_lines() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("buf.txt");
        std::fs::write(&path, "a\nb\nc\n").unwrap();
        let reader = Io::default().new_reader(&path).unwrap();
        let lines: Vec<String> = reader.lines().collect::<std::io::Result<_>>().unwrap();
        assert_eq!(lines, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_reading_and_writing_empty_delim_file() {
        let recs: Vec<Rec> = vec![];
        let tmp = TempDir::new().unwrap();
        let csv = tmp.path().join("recs.csv");
        // Use the gz extension when the gz feature is on so this test continues to cover the
        // compressed-write code path; fall back to plain when the feature is disabled.
        #[cfg(feature = "gz")]
        let tsv = tmp.path().join("recs.tsv.gz");
        #[cfg(not(feature = "gz"))]
        let tsv = tmp.path().join("recs.tsv");

        let df = DelimFile::default();
        df.write_csv(&csv, &recs).unwrap();
        df.write_tsv(&tsv, &recs).unwrap();
        let from_csv: Vec<Rec> = df.read_csv(&csv).unwrap();
        let from_tsv: Vec<Rec> = df.read_tsv(&tsv).unwrap();

        assert_eq!(from_csv, recs);
        assert_eq!(from_tsv, recs);
    }

    #[test]
    fn test_reading_and_writing_delim_file() {
        let recs: Vec<Rec> = vec![
            Rec { s: "Hello".to_string(), i: 123, b: true, o: None },
            Rec { s: "A,B,C".to_string(), i: 456, b: false, o: Some(123.45) },
        ];
        let tmp = TempDir::new().unwrap();
        let csv = tmp.path().join("recs.csv");
        // Use the gz extension when the gz feature is on so this test continues to cover the
        // compressed-write code path; fall back to plain when the feature is disabled.
        #[cfg(feature = "gz")]
        let tsv = tmp.path().join("recs.tsv.gz");
        #[cfg(not(feature = "gz"))]
        let tsv = tmp.path().join("recs.tsv");

        let df = DelimFile::default();
        df.write_csv(&csv, &recs).unwrap();
        df.write_tsv(&tsv, &recs).unwrap();
        let from_csv: Vec<Rec> = df.read_csv(&csv).unwrap();
        let from_tsv: Vec<Rec> = df.read_tsv(&tsv).unwrap();

        assert_eq!(from_csv, recs);
        assert_eq!(from_tsv, recs);
    }

    #[test]
    fn test_skip_empty_lines() {
        // Check to see that csv readers skip empty lines
        let lines = vec!["", "", "s,i,b,o", "", "hello,123,true,123.4"];
        let tempdir = TempDir::new().unwrap();

        let csv = tempdir.path().join("bad_header.csv");
        let io = Io::default();
        io.write_lines(&csv, lines).unwrap();

        let df = DelimFile::default();
        let result: Result<Vec<Rec>> = df.read_csv(&csv);
        let from_csv = result.unwrap();
        assert_eq!(from_csv[0], Rec { s: "hello".to_owned(), i: 123, b: true, o: Some(123.4) })
    }

    #[test]
    fn test_header_error() {
        let lines = vec!["s,i,b,o", "hello,123,true,123.4"];
        let tempdir = TempDir::new().unwrap();
        let csv = tempdir.path().join("bad_header.csv");
        let io = Io::default();
        io.write_lines(&csv, lines).unwrap();

        let df = DelimFile::default();
        let result: Result<Vec<RecWithSkipDe>> = df.read_tsv(&csv);
        let err = result.unwrap_err();

        // All fields should be serialized, deserialization expects to skip "o"
        if let FgError::DelimFileHeaderError { expected, found } = err {
            assert_eq!(expected, "s\ti\tb");
            assert_eq!(found, "s,i,b,o");
        } else {
            panic!()
        }

        let lines = vec!["s,i,b", "hello,123,true"];
        let tempdir = TempDir::new().unwrap();
        let csv = tempdir.path().join("bad_header.csv");
        let io = Io::default();
        io.write_lines(&csv, lines).unwrap();

        let df = DelimFile::default();
        let result: Result<Vec<RecWithSkipSe>> = df.read_tsv(&csv);
        let err = result.unwrap_err();

        // All fields but "o" should be serialized, deserialization should expect all fields
        if let FgError::DelimFileHeaderError { expected, found } = err {
            assert_eq!(expected, "s\ti\tb\to");
            assert_eq!(found, "s,i,b");
        } else {
            panic!()
        }
    }

    #[test]
    fn test_header_missing() {
        let lines = vec!["", "hello,123,true,123.4"];
        let tempdir = TempDir::new().unwrap();
        let csv = tempdir.path().join("bad_header.csv");
        let io = Io::default();
        io.write_lines(&csv, &lines).unwrap();

        let df = DelimFile::default();
        let result: Result<Vec<Rec>> = df.read_csv(&csv);
        let err = result.unwrap_err();

        if let FgError::DelimFileHeaderError { expected, found } = err {
            assert_eq!(expected, "s,i,b,o");
            // NB: empty lines are skipped
            assert_eq!(found, lines[1].to_owned());
        } else {
            panic!()
        }
    }
}
