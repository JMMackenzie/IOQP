#![feature(stdsimd)]

mod ciff;
pub mod compress;
pub mod impact;
mod index;
mod list;
pub mod query;
mod range;
mod result;
mod score;
mod search;
pub mod util;

pub use compress::SimdBPandStreamVbyte;
pub use compress::Uncompressed;
pub use index::Index;
pub use range::ByteRange;
pub use result::SearchResults;

type ScoreType = i16;
