//! There are many helper functions that are used repeatedly across projects, such as serializing an
//! iterator of `Serializable` objects to a file. This crate aims to collect those usage patterns,
//! refine the APIs around them, and provide well tested code to be used across projects.
#![forbid(unsafe_code)]

pub mod io;
pub mod iter;
pub mod sam;

use thiserror::Error;

/// Error types for `fgoxide`
#[derive(Error, Debug)]
pub enum FgError {
    #[error("Error invoking underlying IO operation.")]
    IoError(#[from] std::io::Error),

    #[error("Error parsing/formatting delimited data.")]
    ConversionError(#[from] csv::Error),

    #[error("Error parsing delimited data file header.")]
    DelimFileHeaderError { expected: String, found: String },

    #[error("Records for the same template have different query names: {first} vs {second}.")]
    InconsistentTemplateNames { first: String, second: String },

    #[error("Multiple non-secondary, non-supplementary {read} records for template {name}.")]
    MultiplePrimaryAlignments { name: String, read: &'static str },

    #[error("Cannot construct a Template with no records.")]
    EmptyTemplate,

    #[error("Alignment record is missing a query name.")]
    MissingQueryName,
}

/// Result type that should be used everywhere
type Result<A> = std::result::Result<A, FgError>;
