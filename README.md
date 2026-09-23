# fgoxide

<p align="center">
  <a href="https://github.com/fulcrumgenomics/fgoxide/actions?query=workflow%3ACheck"><img src="https://github.com/fulcrumgenomics/fgoxide/actions/workflows/build_and_test.yml/badge.svg" alt="Build Status"></a>
  <img src="https://img.shields.io/crates/l/fgoxide.svg" alt="license">
  <a href="https://crates.io/crates/fgoxide"><img src="https://img.shields.io/crates/v/fgoxide.svg?colorB=319e8c" alt="Version info"></a>
  <a href="https://doi.org/10.5281/zenodo.14861901"><img src="https://zenodo.org/badge/DOI/10.5281/zenodo.14861901.svg" alt="DOI"></a><br>
</p>

Common utilities code used across [Fulcrum Genomics](https://fulcrumgenomics.com/) Rust projects.

<p>
<a href="https://fulcrumgenomics.com">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/fulcrumgenomics/fgoxide/main/.github/logos/fulcrumgenomics-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/fulcrumgenomics/fgoxide/main/.github/logos/fulcrumgenomics-light.svg">
  <img alt="Fulcrum Genomics" src="https://raw.githubusercontent.com/fulcrumgenomics/fgoxide/main/.github/logos/fulcrumgenomics-light.svg" height="100">
</picture>
</a>
</p>

[Visit us at Fulcrum Genomics](https://www.fulcrumgenomics.com) to learn more about how we can power your Bioinformatics with fgoxide and beyond.

<a href="mailto:contact@fulcrumgenomics.com?subject=[GitHub inquiry]"><img src="https://img.shields.io/badge/Email_us-%2338b44a.svg?&style=for-the-badge&logo=gmail&logoColor=white"/></a>
<a href="https://www.fulcrumgenomics.com"><img src="https://img.shields.io/badge/Visit_Us-%2326a8e0.svg?&style=for-the-badge&logo=wordpress&logoColor=white"/></a>

## Why?

There are many helper functions that are used repeatedly across projects, such as serializing an iterator of `Serializable` objects to a file.
This crate aims to collect those usage patterns, refine the APIs around them, and provide well tested code to be used across projects.

## Documentation and Examples

Please see the generated [Rust Docs](https://docs.rs/fgoxide).
