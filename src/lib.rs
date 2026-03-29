#![doc = include_str!("../docs/lib.md")]
#![cfg_attr(docsrs, feature(doc_cfg), warn(rustdoc::broken_intra_doc_links))]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod common;
pub use common::*;
pub mod counter;

mod error;
pub use error::*;
