#![feature(stdsimd)]

mod ciff;
pub mod compress;
pub mod impact;
mod index;
mod list;
mod result;
mod search;
pub mod util;

pub use compress::SimdBPandStreamVbyte;
pub use index::Index;

type ScoreType = i16;
