#![doc = include_str!("../docs/lib.md")]
#![cfg_attr(docsrs, feature(doc_cfg), warn(rustdoc::broken_intra_doc_links))]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod common;
#[cfg(feature = "counter")]
pub use common::*;
#[cfg(feature = "counter")]
pub mod counter;

/// Rate limiting via the [`trypema`](http://trypema.davidoyinbo.com/) crate.
///
/// This module re-exports all public types from `trypema`, providing
/// sliding-window rate limiting with local, Redis-backed, and hybrid
/// providers. Enable the `trypema` feature to use this module.
#[cfg(feature = "trypema")]
#[cfg_attr(docsrs, doc(cfg(feature = "trypema")))]
pub mod trypema;

mod error;
pub use error::*;
