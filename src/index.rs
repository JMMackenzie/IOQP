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
use crate::search;
use crate::util;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Index<C: crate::compress::Compressor> {
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
        while let Some(ciff::CiffRecord::PostingsList(plist)) = ciff_reader.next() {
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
        pb_plist.finish_and_clear();

        let pb_encode = util::progress_bar("encode postings", all_postings.len());
        let encoded_data: Vec<(String, (list::List, Vec<u8>))> = all_postings
            .into_par_iter()
            .progress_with(pb_encode)
            .map(|(term, input)| (term, list::List::encode::<Compressor>(&input)))
            .collect();

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

    pub fn searcher(&self) -> search::Searcher<'_, Compressor> {
        search::Searcher::with_index(&self)
    }
}
