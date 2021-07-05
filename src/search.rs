use std::collections::{BinaryHeap, HashMap};

use crate::{impact, index::Index, result::*};

const ACCUM_DEFAULT_CAPACITY: usize = 100000;

pub struct Searcher<'index> {
    index: &'index Index,
    impacts: Vec<Vec<impact::Impact<'index>>>,
    large_decode_buf: [u32; impact::LARGE_BUFFER_FACTOR * impact::BLOCK_LEN],
    decode_buf: [u32; impact::BLOCK_LEN],
    accumulators: HashMap<u32, u16>,
}

impl<'index> Searcher<'index> {
    pub fn with_index(index: &'index Index) -> Self {
        Self {
            impacts: (0..index.levels()).map(|_| Vec::new()).collect(),
            index,
            accumulators: HashMap::with_capacity(ACCUM_DEFAULT_CAPACITY),
            large_decode_buf: [0; impact::LARGE_BUFFER_FACTOR * impact::BLOCK_LEN],
            decode_buf: [0; impact::BLOCK_LEN],
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn query_rho<S: AsRef<str> + std::fmt::Debug + std::fmt::Display>(
        &mut self,
        tokens: &[S],
        rho: f32,
        k: usize,
    ) -> SearchResults {
        let start = std::time::Instant::now();
        let total_postings = self.determine_impact_groups(tokens);
        let postings_budget = (total_postings as f32 * rho).ceil() as i64;
        self.process_impact_groups(postings_budget);
        let topk = self.determine_topk(k);
        SearchResults {
            topk,
            took: start.elapsed(),
        }
    }

    #[tracing::instrument(skip(self))]
    fn determine_impact_groups<S: AsRef<str> + std::fmt::Debug + std::fmt::Display>(
        &mut self,
        tokens: &[S],
    ) -> usize {
        // determine what to decompress
        self.impacts.iter_mut().for_each(|i| i.clear());
        tokens
            .into_iter()
            .filter_map(|tok| match self.index.impact_list(tok.as_ref()) {
                Some(list) => {
                    let mut start = list.start_byte_offset;
                    Some(
                        list.impacts
                            .iter()
                            .map(|ti| {
                                let stop = start + ti.bytes as usize;
                                self.impacts[ti.impact as usize].push(
                                    impact::Impact::from_encoded_slice(
                                        *ti,
                                        &self.index.list_data[start..stop],
                                    ),
                                );
                                start += ti.bytes as usize;
                                ti.count
                            })
                            .sum::<u32>(),
                    )
                }
                None => {
                    tracing::warn!("unknown query token '{}'", tok);
                    None
                }
            })
            .sum::<u32>() as usize
    }

    #[tracing::instrument(skip(self))]
    fn process_impact_groups(&mut self, mut postings_budget: i64) {
        self.accumulators.clear();
        let impact_iter = self.impacts.iter_mut().map(|i| i.into_iter()).flatten();
        for impact_group in impact_iter {
            if postings_budget < 0 {
                break;
            }
            let num_postings = impact_group.meta_data.count as i64;
            let impact = impact_group.meta_data.impact;
            while let Some(chunk) = impact_group.next_large_chunk(&mut self.large_decode_buf) {
                for doc_id in chunk {
                    let entry = self.accumulators.entry(*doc_id).or_insert(0);
                    *entry += impact;
                }
            }
            while let Some(chunk) = impact_group.next_chunk(&mut self.decode_buf) {
                for doc_id in chunk {
                    let entry = self.accumulators.entry(*doc_id).or_insert(0);
                    *entry += impact;
                }
            }
            postings_budget -= num_postings;
        }
    }

    #[tracing::instrument(skip(self))]
    fn determine_topk(&mut self, k: usize) -> Vec<SearchResult> {
        let mut heap = BinaryHeap::with_capacity(k);
        self.accumulators.iter().for_each(|(&doc_id, &score)| {
            if heap.len() < k {
                heap.push(SearchResult { doc_id, score });
            } else {
                let top = heap.peek().unwrap();
                if top.score < score {
                    heap.push(SearchResult { doc_id, score });
                    heap.pop();
                }
            }
        });
        heap.into_sorted_vec()
    }

    #[tracing::instrument(skip(self))]
    pub fn query_budget<S: AsRef<str> + std::fmt::Debug + std::fmt::Display>(
        &mut self,
        tokens: &[S],
        postings_budget: i64,
        k: usize,
    ) -> SearchResults {
        let start = std::time::Instant::now();
        self.determine_impact_groups(tokens);
        self.process_impact_groups(postings_budget);
        let topk = self.determine_topk(k);
        SearchResults {
            topk,
            took: start.elapsed(),
        }
    }
}
