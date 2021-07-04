use std::collections::HashMap;

use crate::{impact, index::Index};

pub struct Searcher<'index> {
    index: &'index Index,
    impacts: Vec<Vec<impact::Impact<'index>>>,
    decode_buf: [u32; impact::BLOCK_LEN],
    accumulators: HashMap<u32, u16>,
}

impl<'index> Searcher<'index> {
    pub fn with_index(index: &'index Index) -> Self {
        Self {
            index,
            impacts: vec![],
            accumulators: HashMap::with_capacity(100000),
            decode_buf: [0u32; impact::BLOCK_LEN],
        }
    }

    pub fn query_rho(&mut self, tokens: &[&str], rho: f32) {
        let postings_budget = self.index.num_postings() as f32 * rho;
        self.query_budget(tokens, postings_budget as i64)
    }

    pub fn query_budget(&mut self, tokens: &[&str], mut postings_budget: i64) {
        // determine what to decompress
        self.impacts.iter_mut().for_each(|i| i.clear());
        tokens
            .into_iter()
            .for_each(|tok| match self.index.impact_list(tok) {
                Some(list) => {
                    let mut start = list.start_byte_offset;
                    list.impacts.iter().for_each(|ti| {
                        let stop = start + ti.bytes as usize;
                        self.impacts[ti.impact as usize].push(impact::Impact::from_encoded_slice(
                            *ti,
                            &self.index.list_data[start..stop],
                        ));
                        start += ti.bytes as usize;
                    });
                }
                None => tracing::warn!("unknown query token '{}'", tok),
            });

        // process in impact order
        self.accumulators.clear();
        let impact_iter = self.impacts.iter_mut().map(|i| i.into_iter()).flatten();
        for impact_group in impact_iter {
            if postings_budget < 0 {
                break;
            }
            let num_postings = impact_group.meta_data.count as i64;
            let impact = impact_group.meta_data.impact;
            while let Some(chunk) = impact_group.next_chunk(&mut self.decode_buf) {
                for doc_id in chunk {
                    let entry = self.accumulators.entry(*doc_id).or_default();
                    *entry += impact;
                }
            }
            postings_budget -= num_postings;
        }
    }
}
