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
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::marker::PhantomData;
use std::path::Path;

use crate::{FgError, Result};
use csv::{
    DeserializeRecordsIntoIter, QuoteStyle, ReaderBuilder, StringRecord, Writer, WriterBuilder,
};
use flate2::bufread::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{de::DeserializeOwned, Serialize};

/// The set of file extensions to treat as GZIPPED
const GZIP_EXTENSIONS: [&str; 2] = ["gz", "bgz"];

/// The default buffer size when creating buffered readers/writers
const BUFFER_SIZE: usize = 64 * 1024;

/// Unit-struct that contains associated functions for reading and writing Structs to/from
/// unstructured files.
pub struct Io {
    compression: Compression,
    buffer_size: usize,
}

/// Returns a Default implementation that will compress to gzip level 5.
impl Default for Io {
    fn default() -> Self {
        Io::new(5, BUFFER_SIZE)
    }
}

impl Io {
    /// Creates a new Io instance with the given compression level.
    pub fn new(compression: u32, buffer_size: usize) -> Io {
        Io { compression: flate2::Compression::new(compression), buffer_size }
    }

    /// Returns true if the path ends with a recognized GZIP file extension
    fn is_gzip_path<P: AsRef<Path>>(p: P) -> bool {
        if let Some(ext) = p.as_ref().extension() {
            match ext.to_str() {
                Some(x) => GZIP_EXTENSIONS.contains(&x),
                None => false,
            }
        } else {
            false
        }
    }

    /// Opens a file for reading.  Transparently handles reading gzipped files based
    /// extension.
    pub fn new_reader<P>(&self, p: P) -> Result<Box<dyn BufRead + Send>>
    where
        P: AsRef<Path>,
    {
        let file = File::open(p.as_ref()).map_err(FgError::IoError)?;
        let buf = BufReader::with_capacity(self.buffer_size, file);

        if Self::is_gzip_path(p) {
            Ok(Box::new(BufReader::with_capacity(self.buffer_size, MultiGzDecoder::new(buf))))
        } else {
            Ok(Box::new(buf))
        }
    }

    /// Opens a file for writing. Transparently handles writing GZIP'd data if the file
    /// ends with a recognized GZIP extension.
    pub fn new_writer<P>(&self, p: P) -> Result<BufWriter<Box<dyn Write + Send>>>
    where
        P: AsRef<Path>,
    {
        let file = File::create(p.as_ref()).map_err(FgError::IoError)?;
        let write: Box<dyn Write + Send> = if Io::is_gzip_path(p) {
            Box::new(GzEncoder::new(file, self.compression))
        } else {
            Box::new(file)
        };

        Ok(BufWriter::with_capacity(self.buffer_size, write))
    }

    /// Reads lines from a file into a Vec
    pub fn read_lines<P>(&self, p: &P) -> Result<Vec<String>>
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
    pub fn write_lines<P, S>(&self, p: &P, lines: impl IntoIterator<Item = S>) -> Result<()>
    where
        P: AsRef<Path>,
        S: AsRef<str>,
    {
        let mut out = self.new_writer(p)?;
        for line in lines {
            out.write_all(line.as_ref().as_bytes()).map_err(FgError::IoError)?;
            out.write_all(&[b'\n']).map_err(FgError::IoError)?;
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

        // If the header is not empty (try to parse it)
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
        let found_header_parts: Vec<&str> = header.iter().collect();
        let expected_header_parts = serde_aux::prelude::serde_introspect::<D>();

        // Expected header fields must be a _subset_ of found header fields
        let ok = expected_header_parts.iter().all(|field| found_header_parts.contains(field));

        if !ok {
            let expected = expected_header_parts.join(&delim);
            return Err(FgError::DelimFileHeaderError {
                expected,
                found: header.as_slice().to_owned(),
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
        path: &P,
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
    pub fn write_tsv<S, P>(&self, path: &P, recs: impl IntoIterator<Item = S>) -> Result<()>
    where
        S: Serialize,
        P: AsRef<Path>,
    {
        self.write(path, recs, b'\t', true)
    }

    /// Writes structs implementing `[Serialize]` to a file with comma separators between fields.
    pub fn write_csv<S, P>(&self, path: &P, recs: impl IntoIterator<Item = S>) -> Result<()>
    where
        S: Serialize,
        P: AsRef<Path>,
    {
        self.write(path, recs, b',', true)
    }

    /// Reads structs implementing `[Deserialize]` from a file with the given separators between fields.
    /// If `quote` is true then fields surrounded by quotes are parsed, otherwise quotes are not
    /// considered.
    pub fn read<D, P>(&self, path: &P, delimiter: u8, quote: bool) -> Result<Vec<D>>
    where
        D: DeserializeOwned,
        P: AsRef<Path>,
    {
        self.new_reader(path, delimiter, quote)?.collect()
    }

    /// Reads structs implementing `[Deserialize]` from a file with tab separators between fields.
    pub fn read_tsv<D, P>(&self, path: &P) -> Result<Vec<D>>
    where
        D: DeserializeOwned,
        P: AsRef<Path>,
    {
        self.read(path, b'\t', true)
    }

    /// Reads structs implementing `[Deserialize]` from a file with tab separators between fields.
    pub fn read_csv<D, P>(&self, path: &P) -> Result<Vec<D>>
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
    fn test_header_error() {
        let recs = vec![
            RecWithSkipDe { s: "Hello".to_string(), i: 123, b: true, o: None },
            RecWithSkipDe { s: "A,B,C".to_string(), i: 456, b: false, o: Some(123.45) },
        ];
        let tmp = TempDir::new().unwrap();
        let csv = tmp.path().join("recs.csv");
        let df = DelimFile::default();
        df.write_csv(&csv, recs).unwrap();

        let result: Result<Vec<RecWithSkipDe>> = df.read_tsv(&csv);
        let err = result.unwrap_err();

        // Serialized CSV should contain all fields, deserializing should skip "o"
        if let FgError::DelimFileHeaderError { expected, found } = err {
            assert_eq!(expected, "s\ti\tb");
            assert_eq!(found, "s,i,b,o");
        } else {
            panic!()
        }

        let recs = vec![
            RecWithSkipSe { s: "Hello".to_string(), i: 123, b: true, o: None },
            RecWithSkipSe { s: "A,B,C".to_string(), i: 456, b: false, o: Some(123.45) },
        ];
        let tmp = TempDir::new().unwrap();
        let csv = tmp.path().join("recs.csv");
        let df = DelimFile::default();
        df.write_csv(&csv, recs).unwrap();

        let result: Result<Vec<RecWithSkipSe>> = df.read_tsv(&csv);
        let err = result.unwrap_err();

        // Serialized CSV should contain should skip "o", deserailize should expect all fields
        if let FgError::DelimFileHeaderError { expected, found } = err {
            assert_eq!(expected, "s\ti\tb\to");
            assert_eq!(found, "s,i,b");
        } else {
            panic!()
        }
    }
}
