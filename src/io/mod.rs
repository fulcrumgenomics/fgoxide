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
//!     let path = tempdir.path().join("test_file.csv.gz");
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

const WRITER_EXTENSIONS: &[(&str, NifflerFormat)] = &[
    ("gz", NifflerFormat::Gzip),
    ("bgz", NifflerFormat::Gzip),
    ("bz2", NifflerFormat::Bzip),
    ("xz", NifflerFormat::Lzma),
    ("zst", NifflerFormat::Zstd),
];

/// The default buffer size when creating buffered readers/writers
const BUFFER_SIZE: usize = 64 * 1024;

fn level_from_u32(level: u32) -> Level {
    match level {
        0 => Level::Zero,
        1 => Level::One,
        2 => Level::Two,
        3 => Level::Three,
        4 => Level::Four,
        5 => Level::Five,
        6 => Level::Six,
        7 => Level::Seven,
        8 => Level::Eight,
        _ => Level::Nine,
    }
}

fn map_niffler_err(e: niffler::Error) -> FgError {
    match e {
        niffler::Error::IOError(io) => FgError::IoError(io),
        niffler::Error::FileTooShort => FgError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "input is shorter than the 5-byte compression magic-byte window",
        )),
        niffler::Error::FeatureDisabled => FgError::IoError(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "niffler feature disabled for detected codec",
        )),
    }
}

fn writer_format_for_path<P: AsRef<Path>>(p: P) -> NifflerFormat {
    p.as_ref()
        .extension()
        .and_then(|e| e.to_str())
        .and_then(|ext| {
            WRITER_EXTENSIONS.iter().find(|(name, _)| *name == ext).map(|(_, fmt)| *fmt)
        })
        .unwrap_or(NifflerFormat::No)
}

/// Unit-struct that contains associated functions for reading and writing Structs to/from
/// unstructured files.
pub struct Io {
    compression: Level,
    buffer_size: usize,
}

/// Returns a Default implementation that will compress to gzip level 5.
impl Default for Io {
    fn default() -> Self {
        Io::new(5, BUFFER_SIZE)
    }
}

impl Io {
    /// Creates a new Io instance with the given compression level (0-9). Levels above 9
    /// are clamped; zstd's higher levels are not exposed through this constructor.
    pub fn new(compression: u32, buffer_size: usize) -> Io {
        Io { compression: level_from_u32(compression), buffer_size }
    }

    /// Returns true if the path ends with a recognized GZIP file extension.
    #[must_use]
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
    pub fn new_reader<P>(&self, p: P) -> Result<Box<dyn BufRead + Send>>
    where
        P: AsRef<Path>,
    {
        let path = p.as_ref();
        let file = File::open(path).map_err(FgError::IoError)?;
        let buffered = BufReader::with_capacity(self.buffer_size, file);
        match niffler::send::get_reader(Box::new(buffered)) {
            Ok((reader, _format)) => {
                // TODO: warn when `_format` disagrees with the path extension
                // (e.g. a `.vcf.gz` path whose magic bytes are xz).
                Ok(Box::new(BufReader::with_capacity(self.buffer_size, reader)))
            }
            // File shorter than the 5-byte sniff window: treat as plain via a reopen
            // (correct for regular files; FIFOs shorter than 5 bytes lose those bytes).
            Err(niffler::Error::FileTooShort) => {
                let file = File::open(path).map_err(FgError::IoError)?;
                Ok(Box::new(BufReader::with_capacity(self.buffer_size, file)))
            }
            Err(other) => Err(map_niffler_err(other)),
        }
    }

    /// Opens a file for writing. The output codec is chosen from the path extension:
    /// `.gz`/`.bgz` to gzip, `.bz2` to bzip2, `.xz` to xz, `.zst` to zstd, anything else
    /// uncompressed. `.bgz` currently produces plain gzip, not true BGZF.
    pub fn new_writer<P>(&self, p: P) -> Result<BufWriter<Box<dyn Write + Send>>>
    where
        P: AsRef<Path>,
    {
        let format = writer_format_for_path(&p);
        let file = File::create(p.as_ref()).map_err(FgError::IoError)?;
        let raw: Box<dyn Write + Send> = Box::new(file);
        let write =
            niffler::send::get_writer(raw, format, self.compression).map_err(map_niffler_err)?;
        Ok(BufWriter::with_capacity(self.buffer_size, write))
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
    #[case::gz("gzipped.txt.gz", true)]
    #[case::bgz("bgzipped.txt.bgz", true)]
    #[case::bz2("compressed.txt.bz2", true)]
    #[case::xz("compressed.txt.xz", true)]
    #[case::zst("compressed.txt.zst", true)]
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
        assert_eq!(Io::is_gzip_path(Path::new(path)), expected);
    }

    #[test]
    fn test_reading_and_writing_empty_delim_file() {
        let recs: Vec<Rec> = vec![];
        let tmp = TempDir::new().unwrap();
        let csv = tmp.path().join("recs.csv");
        let tsv = tmp.path().join("recs.tsv.gz");

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
        let tsv = tmp.path().join("recs.tsv.gz");

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
