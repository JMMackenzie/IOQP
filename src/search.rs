use std::collections::BinaryHeap;

use crate::{impact, index::Index, result::*, score_type};

pub struct Searcher<'index> {
    index: &'index Index,
    impacts: Vec<Vec<impact::Impact<'index>>>,
    large_decode_buf: [u32; impact::LARGE_BUFFER_FACTOR * impact::BLOCK_LEN],
    decode_buf: [u32; impact::BLOCK_LEN],
    accumulators: Vec<score_type>,
}

impl<'index> Searcher<'index> {
    pub fn with_index(index: &'index Index) -> Self {
        Self {
            impacts: (0..=index.max_level()).map(|_| Vec::new()).collect(),
            index,
            accumulators: vec![0; index.max_doc_id() + 1],
            large_decode_buf: [0; impact::LARGE_BUFFER_FACTOR * impact::BLOCK_LEN],
            decode_buf: [0; impact::BLOCK_LEN],
        }
    }

    //#[tracing::instrument(skip(self))]
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

    //#[tracing::instrument(skip(self))]
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

    //#[tracing::instrument(skip(self))]
    fn process_impact_groups(&mut self, mut postings_budget: i64) {
        self.accumulators.iter_mut().for_each(|x| *x = 0);
        let impact_iter = self
            .impacts
            .iter_mut()
            .rev()
            .map(|i| i.into_iter())
            .flatten();
        for impact_group in impact_iter {
            if postings_budget < 0 {
                break;
            }
            let num_postings = impact_group.meta_data.count as i64;
            let impact = impact_group.meta_data.impact;
            while let Some(chunk) = impact_group.next_large_chunk(&mut self.large_decode_buf) {
                for doc_id in chunk {
                    self.accumulators[*doc_id as usize] += impact as score_type;
                    // let entry = self.accumulators.entry(*doc_id).or_insert(0);
                    // *entry += impact;
                }
            }
            while let Some(chunk) = impact_group.next_chunk(&mut self.decode_buf) {
                for doc_id in chunk {
                    self.accumulators[*doc_id as usize] += impact as score_type;
                    // let entry = self.accumulators.entry(*doc_id).or_insert(0);
                    // *entry += impact;
                }
            }
            postings_budget -= num_postings;
        }
    }

    //#[tracing::instrument(skip(self))]
    fn determine_topk(&mut self, k: usize) -> Vec<SearchResult> {
        let mut heap = BinaryHeap::with_capacity(k + 1);
        self.accumulators[..k]
            .iter()
            .enumerate()
            .for_each(|(doc_id, score)| {
                heap.push(SearchResult {
                    doc_id: doc_id as u32,
                    score: *score,
                });
            });

        self.accumulators[k..]
            .iter()
            .enumerate()
            .for_each(|(doc_id, &score)| {
                let top = heap.peek().unwrap();
                if top.score < score {
                    heap.push(SearchResult {
                        doc_id: doc_id as u32,
                        score,
                    });
                    heap.pop();
                }
            });
        heap.into_sorted_vec()
    }

    //#[tracing::instrument(skip(self))]
    fn determine_topk_chunks(&mut self, k: usize) -> Vec<SearchResult> {
        let mut heap = BinaryHeap::with_capacity(k + 1);
        self.accumulators[..k]
            .iter()
            .enumerate()
            .for_each(|(doc_id, score)| {
                heap.push(SearchResult {
                    doc_id: doc_id as u32,
                    score: *score,
                });
            });

        const CHUNK_SIZE: u32 = 2048;

        let mut doc_id = 0;
        self.accumulators[k..]
            .chunks(CHUNK_SIZE as usize)
            .for_each(|scores| {
                let threshold = heap.peek().unwrap().score;
                //let max = scores.iter().max().unwrap();
                let max_or_thres = crate::util::determine_max(scores, threshold);
                if max_or_thres > threshold {
                    scores.iter().for_each(|&score| {
                        let top = heap.peek().unwrap();
                        if top.score < score {
                            heap.push(SearchResult { doc_id, score });
                            heap.pop();
                        }
                        doc_id += 1;
                    });
                } else {
                    doc_id += CHUNK_SIZE;
                }
            });
        heap.into_sorted_vec()
    }

    //#[tracing::instrument(skip(self))]
    pub fn query_budget<S: AsRef<str> + std::fmt::Debug + std::fmt::Display>(
        &mut self,
        tokens: &[S],
        postings_budget: i64,
        k: usize,
    ) -> SearchResults {
        let start = std::time::Instant::now();
        self.determine_impact_groups(tokens);
        self.process_impact_groups(postings_budget);
        let topk = self.determine_topk_chunks(k);
        SearchResults {
            topk,
            took: start.elapsed(),
        }
    }
}
