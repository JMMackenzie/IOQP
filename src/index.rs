use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use indicatif::ParallelProgressIterator;
use indicatif::ProgressIterator;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::cmp::Reverse;

use crate::ciff;
use crate::list;
use crate::score;
use crate::search;
use crate::util;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Index<C: crate::compress::Compressor> {
    docmap: Vec<String>,
    vocab: HashMap<String, list::List>,
    #[serde(with = "serde_bytes")]
    pub list_data: Vec<u8>,
    num_levels: usize,
    max_level: usize,
    max_doc_id: u32,
    num_postings: usize,
    impact_type: std::marker::PhantomData<C>,
}

impl<Compressor: crate::compress::Compressor> Index<Compressor> {

    pub fn quantize_from_ciff_file<P: AsRef<std::path::Path> + std::fmt::Debug>(
        input_file_name: P,
        bm25_k1: f32,
        bm25_b: f32,
    ) -> anyhow::Result<Self> {
        let ciff_file = std::fs::File::open(input_file_name)?;
        let mut ciff_file = std::io::BufReader::new(ciff_file);
        let mut ciff_reader = ciff::Reader::new(&mut ciff_file)?;
        let pb_plist = util::progress_bar("read ciff", ciff_reader.num_postings_lists());
        let avg_doclen = ciff_reader.average_doclength(); 
        let mut all_postings = Vec::new();
        let mut uniq_levels: HashSet<u16> = HashSet::new();
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
                        t_plist.push( (doc_id, tf) );
                        num_postings += 1;
                    }
                    temp_idx.push(t_plist);
                },
                Some(ciff::CiffRecord::Document{ external_id, length, .. } ) => {
                    docmap.push(external_id);
                    temp_doclen.push( length as f64 / avg_doclen);
                },
                None => break,
            }
        }
        pb_plist.finish_and_clear();

        let pb_score = util::progress_bar("score postings", temp_idx.len());
        // Init the scorer
        let scorer = score::BM25::new(bm25_k1, bm25_b, max_doc_id);
        let mut max_score:f32 = 0.0;
 
        // (2) We now have all postings as tuple pairs. Let's score/store them.
        for plist in temp_idx.iter_mut() {
            let list_len = plist.len() as u32;
            for (docid, freq) in plist.iter_mut() {
                *freq = scorer.score(*freq as u32, list_len, temp_doclen[*docid as usize] as f32);
                max_score = max_score.max(*freq);
            }
            pb_score.inc(1);
        }
        pb_score.finish_and_clear();



        let pb_quantizer = util::progress_bar("quantize postings", temp_idx.len());

        // (3) We now have the index-wide max_score, and the score for each impact
        // Quantize and organize
        let quantizer = score::LinearQuantizer::new(max_score); 

        for (idx, plist) in temp_idx.iter().enumerate() {

            let mut posting_map: BTreeMap<Reverse<u16>, Vec<u32>> = BTreeMap::new();
            
            for (docid, score) in plist.iter() {
                let impact = quantizer.quantize(*score) as u16;
                let entry = posting_map.entry(Reverse(impact)).or_default();
                entry.push(*docid);
                num_postings += 1;
            }
                    
            uniq_levels.extend(posting_map.keys().map(|r| r.0));
            let final_postings: Vec<(u16, Vec<u32>)> = posting_map
                    .into_iter()
                    .map(|(impact, docs)| (impact.0, docs))
                    .collect();
            all_postings.push((temp_lexicon[idx].clone(), final_postings));
            pb_quantizer.inc(1);
        }
        pb_quantizer.finish_and_clear();


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
        let mut vocab = HashMap::new();
        let mut list_data =
            Vec::with_capacity(encoded_data.iter().map(|(_, (_, data))| data.len()).sum());
        for (term, (mut list, term_data)) in encoded_data.into_iter().progress_with(pb_write) {
            list.start_byte_offset = list_data.len();
            vocab.insert(term, list);
            list_data.extend_from_slice(&term_data);
        }

        Ok(Index {
            docmap,
            vocab,
            list_data,
            num_levels: uniq_levels.len(),
            max_level: uniq_levels.into_iter().max().unwrap() as usize,
            max_doc_id,
            num_postings,
            impact_type: std::marker::PhantomData,
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
                },
                Some(ciff::CiffRecord::Document{ external_id, .. } ) => {
                    docmap.push(external_id);
                },
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
        let mut vocab = HashMap::new();
        let mut list_data =
            Vec::with_capacity(encoded_data.iter().map(|(_, (_, data))| data.len()).sum());
        for (term, (mut list, term_data)) in encoded_data.into_iter().progress_with(pb_write) {
            list.start_byte_offset = list_data.len();
            vocab.insert(term, list);
            list_data.extend_from_slice(&term_data);
        }

        Ok(Index {
            docmap,
            vocab,
            list_data,
            num_levels: uniq_levels.len(),
            max_level: uniq_levels.into_iter().max().unwrap() as usize,
            max_doc_id,
            num_postings,
            impact_type: std::marker::PhantomData,
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
    
    pub fn searcher(&self) -> search::Searcher<'_, Compressor> {
        search::Searcher::with_index(&self)
    }

}
