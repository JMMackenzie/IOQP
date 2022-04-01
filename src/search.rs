use std::collections::BinaryHeap;

use crate::{
    compress::{self},
    impact,
    result::*,
    ScoreType,
};

#[derive(Debug)]
pub struct SearchScratch {
    pub impacts: Vec<Vec<impact::Impact>>,
    pub large_decode_buf: compress::LargeBuffer,
    pub decode_buf: compress::Buffer,
    pub accumulators: Vec<ScoreType>,
    pub heap: BinaryHeap<SearchResult>,
}

impl SearchScratch {
    pub fn from_index(max_level: usize, max_weight: usize, max_doc_id: u32) -> Self {
        Self {
            impacts: (0..=max_level * max_weight).map(|_| Vec::new()).collect(),
            accumulators: vec![0; max_doc_id as usize + 1],
            large_decode_buf: [0; compress::LARGE_BLOCK_LEN],
            decode_buf: [0; compress::BLOCK_LEN],
            heap: BinaryHeap::with_capacity(10000),
        }
    }
}
