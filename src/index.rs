use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::BuildHasherDefault;
use twox_hash::XxHash64;

use indicatif::ParallelProgressIterator;
use indicatif::ProgressIterator;
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::IntoParallelRefMutIterator;
use rayon::iter::ParallelIterator;
use std::cmp::Reverse;

use crate::ciff;
use crate::impact;
use crate::list;
use crate::range::ByteRange;
use crate::result::SearchResult;
use crate::score;
use crate::search;
use crate::search::SearchScratch;
use crate::util;
use crate::ScoreType;
use crate::SearchResults;
use crate::query::{MAX_QUERY_WEIGHT, Term};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Index<C: crate::compress::Compressor> {
    docmap: Vec<String>,
    vocab: HashMap<String, list::List, BuildHasherDefault<XxHash64>>,
    #[serde(with = "serde_bytes")]
    pub list_data: Vec<u8>,
    num_levels: usize,
    max_level: usize,
    max_doc_id: u32,
    max_query_weight: usize,
    num_postings: usize,
    impact_type: std::marker::PhantomData<C>,
    #[serde(skip)]
    search_bufs: parking_lot::Mutex<Vec<search::SearchScratch>>,
}

impl<Compressor: crate::compress::Compressor> Index<Compressor> {
    pub fn quantize_from_ciff_file<P: AsRef<std::path::Path> + std::fmt::Debug>(
        input_file_name: P,
        quant_bits: u32,
        bm25_k1: f32,
        bm25_b: f32,
    ) -> anyhow::Result<Self> {
        let ciff_file = std::fs::File::open(input_file_name)?;
        let mut ciff_file = std::io::BufReader::new(ciff_file);
        let mut ciff_reader = ciff::Reader::new(&mut ciff_file)?;
        let pb_plist = util::progress_bar("read ciff", ciff_reader.num_postings_lists());
        let avg_doclen = ciff_reader.average_doclength();
        let mut num_postings = 0;
        let mut max_doc_id = 0;
        let mut docmap = Vec::new();

        let mut temp_idx = Vec::new();
        let mut temp_lexicon = Vec::new();
        let mut temp_doclen = Vec::new();

        // (1) Iterate the CIFF data and build the temporary index
        loop {
            match ciff_reader.next() {
                Some(ciff::CiffRecord::PostingsList(plist)) => {
                    pb_plist.inc(1);
                    let mut t_plist = Vec::new();
                    let term = plist.get_term().to_string();
                    temp_lexicon.push(term);
                    let postings = plist.get_postings();
                    let mut doc_id: u32 = 0;
                    for posting in postings {
                        doc_id += posting.get_docid() as u32;
                        max_doc_id = max_doc_id.max(doc_id);
                        let tf = posting.get_tf() as f32; // Use f32 so we can overwrite later...
                        t_plist.push((doc_id, tf));
                        num_postings += 1;
                    }
                    temp_idx.push(t_plist);
                }
                Some(ciff::CiffRecord::Document {
                    external_id,
                    length,
                    ..
                }) => {
                    docmap.push(external_id);
                    temp_doclen.push(length as f64 / avg_doclen);
                }
                None => break,
            }
        }
        pb_plist.finish_and_clear();

        let pb_score = util::progress_bar("score postings", temp_idx.len());
        // Init the scorer
        let scorer = score::BM25::new(bm25_k1, bm25_b, max_doc_id);

        // (2) We now have all postings as tuple pairs. Let's score/store them.
        let max_score = temp_idx
            .par_iter_mut()
            .progress_with(pb_score)
            .map(|plist| {
                let list_len = plist.len() as u32;
                let mut max_score: f32 = 0.0;
                for (docid, freq) in plist.iter_mut() {
                    *freq =
                        scorer.score(*freq as u32, list_len, temp_doclen[*docid as usize] as f32);
                    max_score = max_score.max(*freq);
                }
                ordered_float::OrderedFloat(max_score)
            })
            .max()
            .expect("max_score")
            .into_inner();

        let pb_quantizer = util::progress_bar("quantize postings", temp_idx.len());

        // (3) We now have the index-wide max_score, and the score for each impact
        // Quantize and organize
        let quantizer = score::LinearQuantizer::new(max_score, quant_bits);

        let all_postings: Vec<_> = temp_idx
            .par_iter()
            .enumerate()
            .progress_with(pb_quantizer)
            .map(|(idx, plist)| {
                let mut posting_map: BTreeMap<Reverse<u16>, Vec<u32>> = BTreeMap::new();
                for (docid, score) in plist.iter() {
                    let impact = quantizer.quantize(*score) as u16;
                    let entry = posting_map.entry(Reverse(impact)).or_default();
                    entry.push(*docid);
                }
                let final_postings: Vec<(u16, Vec<u32>)> = posting_map
                    .into_iter()
                    .map(|(impact, docs)| (impact.0, docs))
                    .collect();
                (temp_lexicon[idx].clone(), final_postings)
            })
            .collect();
        let uniq_levels: HashSet<u16> = all_postings
            .par_iter()
            .flat_map(|pl| pl.1.par_iter().map(|l| l.0))
            .collect();

        let pb_encode = util::progress_bar("encode postings", all_postings.len());
        let encoded_data: Vec<(String, (list::List, Vec<u8>))> = all_postings
            .into_par_iter()
            .progress_with(pb_encode)
            .map(|(term, input)| (term, list::List::encode::<Compressor>(&input)))
            .collect();

        if docmap.len() != (max_doc_id + 1) as usize {
            anyhow::bail!("Document map length does not match the maximum document identifier. Is your CIFF file corrupt?");
        }

        let pb_write = util::progress_bar("create index", encoded_data.len());
        let mut vocab: HashMap<_, _, BuildHasherDefault<XxHash64>> = Default::default();
        let mut list_data =
            Vec::with_capacity(encoded_data.iter().map(|(_, (_, data))| data.len()).sum());
        for (term, (mut list, term_data)) in encoded_data.into_iter().progress_with(pb_write) {
            list.start_byte_offset = list_data.len();
            vocab.insert(term, list);
            list_data.extend_from_slice(&term_data);
        }

        let num_levels = uniq_levels.len();
        let max_level = uniq_levels.into_iter().max().unwrap() as usize;
        let search_bufs = parking_lot::Mutex::new(
            (0..2048)
                .map(|_| search::SearchScratch::from_index(max_level, MAX_QUERY_WEIGHT, max_doc_id))
                .collect(),
        );

        Ok(Index {
            docmap,
            vocab,
            list_data,
            num_levels,
            max_level,
            max_doc_id,
            max_query_weight: MAX_QUERY_WEIGHT,
            num_postings,
            impact_type: std::marker::PhantomData,
            search_bufs,
        })
    }

    pub fn from_ciff_file<P: AsRef<std::path::Path> + std::fmt::Debug>(
        input_file_name: P,
    ) -> anyhow::Result<Self> {
        let ciff_file = std::fs::File::open(input_file_name)?;
        let mut ciff_file = std::io::BufReader::new(ciff_file);
        let mut ciff_reader = ciff::Reader::new(&mut ciff_file)?;
        let pb_plist = util::progress_bar("read ciff", ciff_reader.num_postings_lists());
        let mut all_postings = Vec::new();
        let mut uniq_levels: HashSet<u16> = HashSet::new();
        let mut num_postings = 0;
        let mut max_doc_id = 0;
        let mut docmap = Vec::new();

        // Iterate the CIFF data and build the index
        loop {
            match ciff_reader.next() {
                Some(ciff::CiffRecord::PostingsList(plist)) => {
                    pb_plist.inc(1);
                    let term = plist.get_term().to_string();
                    let postings = plist.get_postings();
                    let mut posting_map: BTreeMap<Reverse<u16>, Vec<u32>> = BTreeMap::new();
                    let mut doc_id: u32 = 0;
                    for posting in postings {
                        doc_id += posting.get_docid() as u32;
                        max_doc_id = max_doc_id.max(doc_id);
                        let impact = posting.get_tf() as u16;
                        let entry = posting_map.entry(Reverse(impact)).or_default();
                        entry.push(doc_id);
                        num_postings += 1;
                    }
                    uniq_levels.extend(posting_map.keys().map(|r| r.0));
                    let final_postings: Vec<(u16, Vec<u32>)> = posting_map
                        .into_iter()
                        .map(|(impact, docs)| (impact.0, docs))
                        .collect();
                    all_postings.push((term, final_postings));
                }
                Some(ciff::CiffRecord::Document { external_id, .. }) => {
                    docmap.push(external_id);
                }
                None => break,
            }
        }
        pb_plist.finish_and_clear();

        let pb_encode = util::progress_bar("encode postings", all_postings.len());
        let encoded_data: Vec<(String, (list::List, Vec<u8>))> = all_postings
            .into_par_iter()
            .progress_with(pb_encode)
            .map(|(term, input)| (term, list::List::encode::<Compressor>(&input)))
            .collect();

        if docmap.len() != (max_doc_id + 1) as usize {
            anyhow::bail!("Document map length does not match the maximum document identifier. Is your CIFF file corrupt?");
        }

        let pb_write = util::progress_bar("create index", encoded_data.len());
        let mut vocab: HashMap<_, _, BuildHasherDefault<XxHash64>> = Default::default();
        let mut list_data =
            Vec::with_capacity(encoded_data.iter().map(|(_, (_, data))| data.len()).sum());
        for (term, (mut list, term_data)) in encoded_data.into_iter().progress_with(pb_write) {
            list.start_byte_offset = list_data.len();
            vocab.insert(term, list);
            list_data.extend_from_slice(&term_data);
        }

        let num_levels = uniq_levels.len();
        let max_level = uniq_levels.into_iter().max().unwrap() as usize;
        let search_bufs = parking_lot::Mutex::new(
            (0..2048)
                .map(|_| search::SearchScratch::from_index(max_level, MAX_QUERY_WEIGHT, max_doc_id))
                .collect(),
        );

        Ok(Index {
            docmap,
            vocab,
            list_data,
            num_levels,
            max_level,
            max_doc_id,
            max_query_weight: MAX_QUERY_WEIGHT,
            num_postings,
            impact_type: std::marker::PhantomData,
            search_bufs,
        })
    }

    pub fn write_to_file<P: AsRef<std::path::Path> + std::fmt::Debug>(
        &self,
        output_file_name: P,
    ) -> anyhow::Result<()> {
        let output_file = std::fs::File::create(output_file_name)?;
        let output_file = std::io::BufWriter::new(output_file);
        bincode::serialize_into(output_file, &self)?;
        Ok(())
    }

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

    fn determine_impact_segments( //<S: AsRef<Term> + std::fmt::Debug + std::fmt::Display>(
        &self,
        data: &mut SearchScratch,
        tokens: &[Term],
    ) -> usize {
        // determine what to decompress
        data.impacts.iter_mut().for_each(|i| i.clear());
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
                                        ByteRange::new(start, stop),
                                        tok.freq as u16
                                    ),
                                );
                                start += ti.bytes as usize;
                                ti.count
                            })
                            .sum::<u32>(),
                    )
                }
                None => {
                    //println!("unknown query token '{}'", tok);
                    None
                }
            })
            .sum::<u32>() as usize
    }

    fn process_impact_segments(&self, data: &mut SearchScratch, mut postings_budget: i64) {
        data.accumulators.iter_mut().for_each(|x| *x = 0);
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
                for doc_id in chunk {
                    data.accumulators[*doc_id as usize] += impact as ScoreType;
                    // let entry = self.accumulators.entry(*doc_id).or_insert(0);
                    // *entry += impact;
                }
            }
            while let Some(chunk) =
                impact_group.next_chunk::<Compressor>(&self.list_data, &mut data.decode_buf)
            {
                for doc_id in chunk {
                    data.accumulators[*doc_id as usize] += impact as ScoreType;
                    // let entry = self.accumulators.entry(*doc_id).or_insert(0);
                    // *entry += impact;
                }
            }
            postings_budget -= num_postings;
        }
    }

    fn determine_topk(&self, data: &mut SearchScratch, k: usize) -> Vec<SearchResult> {
        let mut heap = BinaryHeap::with_capacity(k + 1);
        let block_offset = k;
        data.accumulators[..k]
            .iter()
            .enumerate()
            .for_each(|(doc_id, score)| {
                heap.push(SearchResult {
                    doc_id: doc_id as u32,
                    score: *score,
                });
            });
        data.accumulators[k..]
            .iter()
            .enumerate()
            .for_each(|(doc_id, &score)| {
                let top = heap.peek().unwrap();
                if top.score < score {
                    heap.push(SearchResult {
                        doc_id: (doc_id + block_offset) as u32,
                        score,
                    });
                    heap.pop();
                }
            });
        heap.into_sorted_vec()
    }

    fn determine_topk_chunks(&self, data: &mut SearchScratch, k: usize) -> Vec<SearchResult> {
        let heap = &mut data.heap;
        let accumulators = &mut data.accumulators;

        heap.clear();
        let block_offset = k;
        accumulators[..k]
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
        accumulators[k..]
            .chunks(CHUNK_SIZE as usize)
            .for_each(|scores| {
                let threshold = heap.peek().unwrap().score;
                //let max = scores.iter().max().unwrap();
                let max_or_thres = unsafe { crate::util::determine_max(scores, threshold) };
                if max_or_thres > threshold {
                    scores.iter().for_each(|&score| {
                        let top = heap.peek().unwrap();
                        if top.score < score {
                            heap.push(SearchResult {
                                doc_id: (doc_id + block_offset as u32),
                                score,
                            });
                            heap.pop();
                        }
                        doc_id += 1;
                    });
                } else {
                    doc_id += CHUNK_SIZE;
                }
            });

        // only alloc happens here
        let mut result = Vec::new();
        while let Some(elem) = heap.pop() {
            result.push(elem);
        }
        result
    }

    pub fn query_fraction( //<S: AsRef<Term> + std::fmt::Debug + std::fmt::Display>(
        &self,
        tokens: &[Term],
        rho: f32,
        query_id: Option<usize>,
        k: usize,
    ) -> SearchResults {
        let start = std::time::Instant::now();

        let mut search_buf =
            self.search_bufs.lock().pop().unwrap_or_else(|| {
                search::SearchScratch::from_index(self.max_level, self.max_query_weight, self.max_doc_id)
            });

        let total_postings = self.determine_impact_segments(&mut search_buf, tokens);
        let postings_budget = (total_postings as f32 * rho).ceil() as i64;
        self.process_impact_segments(&mut search_buf, postings_budget);
        let topk = self.determine_topk(&mut search_buf, k);

        self.search_bufs.lock().push(search_buf);
        SearchResults {
            topk,
            took: start.elapsed(),
            qid: query_id.unwrap_or_default(),
        }
    }

    pub fn query_fixed( //<S: AsRef<str> + std::fmt::Debug + std::fmt::Display>(
        &self,
        tokens: &[Term],
        postings_budget: i64,
        query_id: Option<usize>,
        k: usize,
    ) -> SearchResults {
        let start = std::time::Instant::now();

        let mut search_buf =
            self.search_bufs.lock().pop().unwrap_or_else(|| {
                search::SearchScratch::from_index(self.max_level, self.max_query_weight, self.max_doc_id)
            });

        self.determine_impact_segments(&mut search_buf, tokens);
        self.process_impact_segments(&mut search_buf, postings_budget);
        let topk = self.determine_topk_chunks(&mut search_buf, k);

        self.search_bufs.lock().push(search_buf);
        SearchResults {
            topk,
            took: start.elapsed(),
            qid: query_id.unwrap_or_default(),
        }
    }
}
