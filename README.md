# fgoxide

<p align="center">
  <a href="https://github.com/fulcrumgenomics/fgoxide/actions?query=workflow%3ACheck"><img src="https://github.com/fulcrumgenomics/fgoxide/actions/workflows/build_and_test.yml/badge.svg" alt="Build Status"></a>
  <img src="https://img.shields.io/crates/l/fgoxide.svg" alt="license">
  <a href="https://crates.io/crates/fgoxide"><img src="https://img.shields.io/crates/v/fgoxide.svg?colorB=319e8c" alt="Version info"></a><br>
</p>

Common utilities code used across [Fulcrum Genomics](https://fulcrumgenomics.com/) Rust projects.

## Why?

There are many helper functions that are used repeatedly across projects, such as serializing an iterator of `Serializable` objects to a file.
This crate aims to collect those usage patterns, refine the APIs around them, and provide well tested code to be used across projects.

## Documentation and Examples

Please see the generated [Rust Docs](https://docs.rs/fgoxide-commons).
