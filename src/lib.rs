//! There are many helper functions that are used repeatedly across projects, such as serializing an
//! iterator of `Serializable` objects to a file. This crate aims to collect those usage patterns,
//! refine the APIs around them, and provide well tested code to be used across projects.
#![forbid(unsafe_code)]

pub mod io;

use thiserror::Error;

/// Error types for [`fgoxide`]
#[derive(Error, Debug)]
pub enum FgError {
    #[error("Error invoking underlying IO operation.")]
    IoError(#[from] std::io::Error),

    #[error("Error parsing/formatting delimited data.")]
    ConversionError(#[from] csv::Error),
}

/// Result type that should be used everywhere
type Result<A> = std::result::Result<A, FgError>;
