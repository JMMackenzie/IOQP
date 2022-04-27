use rayon::iter::IntoParallelIterator;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::hash::BuildHasherDefault;
use tracing::info;
use twox_hash::XxHash64;

use indicatif::ParallelProgressIterator;
use indicatif::ProgressIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::cmp::Reverse;

use crate::ciff;
use crate::impact;
use crate::list;
use crate::query::{Term, MAX_TERM_WEIGHT};
use crate::range::Byte;
use crate::score;
use crate::search;
use crate::util;
use crate::ScoreType;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Index<C: crate::compress::Compressor> {
    docmap: Vec<String>,
    vocab: HashMap<String, list::List, BuildHasherDefault<XxHash64>>,
    #[serde(with = "serde_bytes")]
    pub list_data: Vec<u8>,
    num_levels: usize,
    max_level: usize,
    max_doc_id: u32,
    max_term_weight: usize,
    num_postings: usize,
    impact_type: std::marker::PhantomData<C>,
    #[serde(skip)]
    search_bufs: parking_lot::Mutex<Vec<search::Scratch>>,
}

impl<Compressor: crate::compress::Compressor> Index<Compressor> {
    /// Creates index from ciff file, quantziing it first
    ///
    /// # Panics
    /// Panics if doc data does not match plist data
    /// # Errors
    /// - Can't open ciff file
    pub fn from_ciff_file<P: AsRef<std::path::Path> + std::fmt::Debug>(
        input_file_name: P,
        quant_bits: u32,
        scorer: impl score::Scorer,
    ) -> anyhow::Result<Self> {
        let ciff_reader = ciff::Reader::from_file(input_file_name)?;

        let pb_docmap =
            util::progress_bar("determine docmap", ciff_reader.header.num_docs as usize);
        let avg_doclen = ciff_reader.header.average_doclength;
        let num_plists = ciff_reader.header.num_postings_lists as usize;
        let num_postings = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut docmap = Vec::new();
        let mut doclen = Vec::new();
        let mut step = 1;

        info!("({}) iterate the CIFF data and build the docmap", step);
        step += 1;
        let mut max_doc_id = 0;
        for doc_record in ciff_reader.doc_record_iter().progress_with(pb_docmap) {
            docmap.push(doc_record.collection_docid);
            doclen.push(f64::from(doc_record.doclength) / avg_doclen);
            max_doc_id = max_doc_id.max(doc_record.docid as u32);
        }
        let num_docs = doclen.len() as u32;

        let max_score = if !scorer.needs_quantization() {
            1.0
        } else {
            info!(
                "({}) iterate the CIFF data and score stuff to determine max score",
                step
            );
            step += 1;
            let max_score = determine_max_score(
                num_plists,
                &ciff_reader,
                scorer,
                &doclen,
                num_docs,
                &num_postings,
            );
            info!("\tmax score => {}", max_score);
            max_score
        };

        info!(
            "({}) Iterate the CIFF data again score + quantize + encode",
            step
        );
        step += 1;
        let encoded_data = Self::quantize_and_encode(
            max_score,
            quant_bits,
            num_plists,
            &ciff_reader,
            scorer,
            &doclen,
            num_docs,
        );

        info!("({}) determine uniq impact levels ", step);
        step += 1;
        let pb_uniq_lvls = util::progress_bar("create index", encoded_data.len());
        let uniq_levels: HashSet<u16> = encoded_data
            .par_iter()
            .progress_with(pb_uniq_lvls)
            .flat_map(|pl| pl.1 .0.impacts.par_iter().map(|l| l.impact))
            .collect();

        if docmap.len() != (max_doc_id + 1) as usize {
            anyhow::bail!("Document map length does not match the maximum document identifier. Is your CIFF file corrupt?");
        }

        info!("({}) concatenate final index structure", step);
        step += 1;
        let pb_write = util::progress_bar("create index", encoded_data.len());
        let mut vocab: HashMap<_, _, BuildHasherDefault<XxHash64>> =
            std::collections::HashMap::default();
        let mut list_data =
            Vec::with_capacity(encoded_data.iter().map(|(_, (_, data))| data.len()).sum());
        for (term, (mut list, term_data)) in encoded_data.into_iter().progress_with(pb_write) {
            list.start_byte_offset = list_data.len();
            vocab.insert(term, list);
            list_data.extend_from_slice(&term_data);
        }

        info!("({}) instantiate search objects", step);
        step += 1;
        let num_levels = uniq_levels.len();
        let max_level = uniq_levels.into_iter().max().unwrap() as usize;
        let search_bufs = parking_lot::Mutex::new(
            (0..2048)
                .map(|_| search::Scratch::from_index(max_level, MAX_TERM_WEIGHT, max_doc_id))
                .collect(),
        );

        info!("({}) create final index object", step);
        Ok(Index {
            docmap,
            vocab,
            list_data,
            num_levels,
            max_level,
            max_doc_id,
            max_term_weight: MAX_TERM_WEIGHT,
            num_postings: num_postings.load(std::sync::atomic::Ordering::Relaxed),
            impact_type: std::marker::PhantomData,
            search_bufs,
        })
    }

    /// Write IOQP index to file
    ///
    /// # Errors
    /// - fails if file can't be created
    /// - fails if index can't be serialized
    pub fn write_to_file<P: AsRef<std::path::Path> + std::fmt::Debug>(
        &self,
        output_file_name: P,
    ) -> anyhow::Result<()> {
        let output_file = std::fs::File::create(output_file_name)?;
        let output_file = std::io::BufWriter::new(output_file);
        bincode::serialize_into(output_file, &self)?;
        Ok(())
    }

    /// Read IOQP index from file
    ///
    /// # Errors
    /// - fails if file does not exist
    /// - fails if index can't be deserialized
    pub fn read_from_file<P: AsRef<std::path::Path> + std::fmt::Debug>(
        index_file_name: P,
    ) -> anyhow::Result<Self> {
        let input_file = std::fs::File::open(index_file_name)?;
        let input_file = std::io::BufReader::new(input_file);
        let index = bincode::deserialize_from::<_, Self>(input_file)?;
        Ok(index)
    }

    pub fn impact_list(&self, token: &str) -> Option<&list::List> {
        self.vocab.get(token)
    }

    pub fn num_postings(&self) -> usize {
        self.num_postings
    }

    pub fn levels(&self) -> usize {
        self.num_levels
    }

    pub fn max_level(&self) -> usize {
        self.max_level
    }

    pub fn max_doc_id(&self) -> usize {
        self.max_doc_id as usize
    }

    pub fn docmap(&self) -> &Vec<String> {
        &self.docmap
    }

    fn determine_impact_segments(&self, data: &mut search::Scratch, tokens: &[Term]) -> usize {
        // determine what to decompress
        data.impacts.iter_mut().for_each(std::vec::Vec::clear);
        tokens
            .iter()
            .filter_map(|tok| match self.impact_list(&tok.token) {
                Some(list) => {
                    let mut start = list.start_byte_offset;
                    Some(
                        list.impacts
                            .iter()
                            .map(|ti| {
                                let stop = start + ti.bytes as usize;
                                data.impacts[ti.impact as usize * tok.freq as usize].push(
                                    impact::Impact::from_encoded_slice_weighted(
                                        *ti,
                                        Byte::new(start, stop),
                                        tok.freq as u16,
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

    fn process_impact_segments(&self, data: &mut search::Scratch, mut postings_budget: i64) {
        let accumulators = &mut data.accumulators;
        let chunks = &mut data.chunk;
        accumulators.iter_mut().for_each(|x| *x = 0);
        chunks.iter_mut().for_each(|x| *x = 0);
        let impact_iter = data.impacts.iter_mut().rev().flat_map(|i| i.iter_mut());
        for impact_group in impact_iter {
            if postings_budget < 0 {
                break;
            }
            let num_postings = impact_group.count() as i64;
            let impact = impact_group.impact();
            while let Some(chunk) = impact_group
                .next_large_chunk::<Compressor>(&self.list_data, &mut data.large_decode_buf)
            {
                chunk.iter().cloned().for_each(|doc_id| {
                    let doc_id = doc_id as usize;
                    let chunk_id = doc_id >> search::CHUNK_SHIFT;
                    let accum = unsafe { accumulators.get_unchecked_mut(doc_id) };
                    *accum += impact as ScoreType;
                    let chnk = unsafe { chunks.get_unchecked_mut(chunk_id) };
                    *chnk = (*chnk).max(*accum);
                });
            }
            while let Some(chunk) =
                impact_group.next_chunk::<Compressor>(&self.list_data, &mut data.decode_buf)
            {
                chunk.iter().cloned().for_each(|doc_id| {
                    let doc_id = doc_id as usize;
                    let chunk_id = doc_id >> search::CHUNK_SHIFT;
                    let accum = unsafe { accumulators.get_unchecked_mut(doc_id) };
                    *accum += impact as ScoreType;
                    let chnk = unsafe { chunks.get_unchecked_mut(chunk_id) };
                    *chnk = (*chnk).max(*accum);
                });
            }
            postings_budget -= num_postings;
        }
    }

    fn determine_topk_chunks(&self, data: &mut search::Scratch, k: usize) -> Vec<search::Result> {
        let heap = &mut data.heap;
        let accumulators = &data.accumulators;
        let chunks = &data.chunk;
        heap.clear();

        // Calculate how many chunks we need to look at to populate k docs into the heap
        let init_heap_chunks = ((k as f32) / (search::CHUNK_SIZE as f32)).ceil() as usize;
        let init_heap_docs = search::CHUNK_SIZE * init_heap_chunks;

        // Push the first k documents
        accumulators[..k]
            .iter()
            .enumerate()
            .for_each(|(doc_id, score)| {
                heap.push(search::Result {
                    doc_id: doc_id as u32,
                    score: *score,
                });
            });


        // Check the remaining init_heap_docs - k entries
        let mut threshold = heap.peek().unwrap().score;
        accumulators[k..init_heap_docs]
            .iter()
            .enumerate()
            .for_each(|(doc_id, score)| {
                if threshold < *score {
                    heap.push(search::Result {
                        doc_id: (doc_id + k) as u32,
                        score: *score,
                    });
                    heap.pop();
                    threshold = heap.peek().unwrap().score;
                }
            });


        let mut threshold = heap.peek().unwrap().score;
        let mut doc_id = init_heap_docs;
        chunks
            .iter()
            .skip(init_heap_chunks)
            .zip(accumulators.chunks(search::CHUNK_SIZE as usize).skip(init_heap_chunks))
            .for_each(|(&chunk_max, scores)| {
                if chunk_max > threshold {
                    scores.iter().for_each(|&score| {
                        if threshold < score {
                            heap.push(search::Result {
                                doc_id: doc_id as u32,
                                score,
                            });
                            heap.pop();
                            threshold = heap.peek().unwrap().score;
                        }
                        doc_id += 1;
                    });
                } else {
                    doc_id += search::CHUNK_SIZE;
                }
            });
        // only alloc happens here
        let mut result = Vec::with_capacity(heap.len());
        while let Some(elem) = heap.pop() {
            result.push(elem);
        }
        result.into_iter().rev().collect()
    }

    pub fn query_fraction(
        &self,
        tokens: &[Term],
        rho: f32,
        query_id: Option<usize>,
        k: usize,
    ) -> search::Results {
        let start = std::time::Instant::now();

        let mut search_buf = self.search_bufs.lock().pop().unwrap_or_else(|| {
            search::Scratch::from_index(self.max_level, self.max_term_weight, self.max_doc_id)
        });

        let total_postings = self.determine_impact_segments(&mut search_buf, tokens);
        let postings_budget = (total_postings as f32 * rho).ceil() as i64;
        self.process_impact_segments(&mut search_buf, postings_budget);
        let topk = self.determine_topk_chunks(&mut search_buf, k);

        self.search_bufs.lock().push(search_buf);
        search::Results {
            topk,
            took: start.elapsed(),
            qid: query_id.unwrap_or_default(),
        }
    }

    pub fn query_fixed(
        &self,
        tokens: &[Term],
        postings_budget: i64,
        query_id: Option<usize>,
        k: usize,
    ) -> search::Results {
        let start = std::time::Instant::now();

        let mut search_buf = self.search_bufs.lock().pop().unwrap_or_else(|| {
            search::Scratch::from_index(self.max_level, self.max_term_weight, self.max_doc_id)
        });

        self.determine_impact_segments(&mut search_buf, tokens);
        self.process_impact_segments(&mut search_buf, postings_budget);
        let topk = self.determine_topk_chunks(&mut search_buf, k);

        self.search_bufs.lock().push(search_buf);
        search::Results {
            topk,
            took: start.elapsed(),
            qid: query_id.unwrap_or_default(),
        }
    }

    pub fn query_warmup(&self, tokens: &[Term]) {
        let postings_budget = 0;
        let mut search_buf = self.search_bufs.lock().pop().unwrap_or_else(|| {
            search::Scratch::from_index(self.max_level, self.max_term_weight, self.max_doc_id)
        });
        self.determine_impact_segments(&mut search_buf, tokens);
        self.process_impact_segments(&mut search_buf, postings_budget);
        self.search_bufs.lock().push(search_buf);
    }

    fn quantize_and_encode(
        max_score: f32,
        quant_bits: u32,
        num_plists: usize,
        ciff_reader: &ciff::Reader,
        scorer: impl score::Scorer,
        doclen: &[f64],
        num_docs: u32,
    ) -> Vec<(String, (list::List, Vec<u8>))> {
        let quantizer = score::LinearQuantizer::new(max_score, quant_bits);
        let pb_quantizer = util::progress_bar("quantize/encode postings", num_plists);
        (0..num_plists)
            .into_par_iter()
            .progress_with(pb_quantizer)
            .map(|idx| {
                let plist = ciff_reader.postings_list(idx);
                let mut posting_map: BTreeMap<Reverse<u16>, Vec<u32>> = BTreeMap::new();
                let list_len = plist.postings.len() as u32;
                let mut doc_id: u32 = 0;
                for ciff::Posting { docid, tf } in &plist.postings {
                    doc_id += *docid as u32;
                    let freq = scorer.score(
                        *tf as u32,
                        list_len,
                        doclen[doc_id as usize] as f32,
                        num_docs,
                    );
                    let impact = if scorer.needs_quantization() {
                        u16::try_from(quantizer.quantize(freq)).expect("impact < u16::max")
                    } else {
                        freq as u16
                    };
                    let entry = posting_map.entry(Reverse(impact)).or_default();
                    entry.push(doc_id);
                }
                let final_postings: Vec<(u16, Vec<u32>)> = posting_map
                    .into_iter()
                    .map(|(impact, docs)| (impact.0, docs))
                    .collect();
                let encoded_data = list::List::encode::<Compressor>(&final_postings);
                (plist.term, encoded_data)
            })
            .collect()
    }
}

fn determine_max_score(
    num_plists: usize,
    ciff_reader: &ciff::Reader,
    scorer: impl score::Scorer,
    doclen: &[f64],
    num_docs: u32,
    num_postings: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> f32 {
    let pb_score = util::progress_bar("score postings", num_plists);
    (0..num_plists)
        .into_par_iter()
        .progress_with(pb_score)
        .map(|idx| {
            let plist = ciff_reader.postings_list(idx);
            let list_len = plist.postings.len() as u32;
            let mut max_score: f32 = 0.0;
            let mut doc_id: u32 = 0;
            for ciff::Posting { docid, tf } in &plist.postings {
                doc_id += *docid as u32;
                let score = scorer.score(
                    *tf as u32,
                    list_len,
                    doclen[doc_id as usize] as f32,
                    num_docs,
                );
                max_score = max_score.max(score);
            }
            num_postings.fetch_add(plist.postings.len(), std::sync::atomic::Ordering::Relaxed);
            ordered_float::OrderedFloat(max_score)
        })
        .max()
        .expect("max_score")
        .into_inner()
}
