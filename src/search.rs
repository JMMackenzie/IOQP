use std::collections::BinaryHeap;

use crate::{
    compress::{self},
    impact, ScoreType,
};

#[derive(Debug)]
pub struct Scratch {
    pub impacts: Vec<Vec<impact::Impact>>,
    pub large_decode_buf: compress::LargeBuffer,
    pub decode_buf: compress::Buffer,
    pub accumulators: Vec<ScoreType>,
    pub heap: BinaryHeap<Result>,
}

impl Scratch {
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

use std::cmp::Ordering;
use std::io::Write;

#[derive(Eq, serde::Serialize, Debug)]
pub struct Result {
    pub doc_id: u32,
    pub score: crate::ScoreType,
}

impl Ord for Result {
    fn cmp(&self, other: &Self) -> Ordering {
        other.score.cmp(&self.score)
    }
}

impl PartialOrd for Result {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Result {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

#[derive(serde::Serialize, Debug)]
pub struct Results {
    pub topk: Vec<Result>,
    pub took: std::time::Duration,
    pub qid: usize,
}

impl Results {
    pub fn to_trec_file(&self, id_map: &[String], mut output: &std::fs::File) {
        for (rank, res) in self.topk.iter().enumerate() {
            writeln!(
                output,
                "{} Q0 {} {} {} ioqp",
                self.qid,
                id_map[res.doc_id as usize],
                rank + 1,
                res.score
            )
            .unwrap();
        }
    }

    pub fn _to_tsv_file(&self, id_map: &[String], mut output: &std::fs::File) {
        for (rank, res) in self.topk.iter().enumerate() {
            writeln!(
                output,
                "{} {} {}",
                self.qid,
                id_map[res.doc_id as usize],
                rank + 1
            )
            .unwrap();
        }
    }
}

impl std::fmt::Display for Results {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "query {} took: {}ms [", self.qid, self.took.as_millis())?;
        for (rank, res) in self.topk.iter().enumerate() {
            write!(f, "#{},({},{})", rank + 1, res.doc_id, res.score)?;
            if rank + 1 != self.topk.len() {
                write!(f, ",")?;
            }
        }
        write!(f, "]")
    }
}
