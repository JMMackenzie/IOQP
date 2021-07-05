#![feature(stdsimd)]

mod ciff;
pub mod impact;
mod index;
mod list;
mod result;
mod search;
pub mod util;

pub use index::Index;

type score_type = i16;
