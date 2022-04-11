#![warn(missing_debug_implementations, rust_2018_idioms)]
#![feature(stdsimd)]

//mod ciff;
pub mod ciff;
pub mod compress;
pub mod impact;
mod index;
mod list;
pub mod query;
mod range;
pub mod score;
mod search;
pub mod util;

pub use compress::SimdBPandStreamVbyte;
pub use compress::Uncompressed;
pub use index::Index;
pub use range::Byte;
pub use search::Results;

// // Configurable: The data type for accumulating scores.
type ScoreType = u16;
