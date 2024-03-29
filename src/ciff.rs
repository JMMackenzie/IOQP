use bytes::Buf;
use prost::Message;

pub mod format {
    include!(concat!(env!("OUT_DIR"), "/ciff.rs"));
}

pub use format::DocRecord;
pub use format::Posting;
pub use format::PostingsList;

#[derive(Debug)]
pub struct Reader {
    input: memmap2::Mmap,
    pub header: format::Header,
    plist_data: Vec<std::ops::Range<usize>>,
    doc_data: Vec<std::ops::Range<usize>>,
}

impl Reader {
    /// Quantize the score
    ///
    /// # Errors
    /// Fails if ciff file does not exist
    pub fn from_file<P: AsRef<std::path::Path>>(ciff_path: P) -> anyhow::Result<Self> {
        let ciff_file = std::fs::File::open(ciff_path.as_ref())?;
        let input = unsafe { memmap2::Mmap::map(&ciff_file)? };
        let mut reader = input.as_ref();
        let header = format::Header::decode_length_delimited(&mut reader)?;
        let num_plists = header.num_postings_lists as usize;
        let num_docs = header.num_docs as usize;
        let mut plist_data = Vec::with_capacity(num_plists);
        let mut doc_data = Vec::with_capacity(num_docs);
        tracing::info!("parse ciff file to determine message offsets");
        let pb = crate::util::progress_bar(
            "determine msg positions",
            plist_data.capacity() + doc_data.capacity(),
        );
        for _ in 0..num_plists {
            let len = prost::decode_length_delimiter(&mut reader)?;
            let offset = input.len() - reader.remaining();
            reader.advance(len);
            plist_data.push(std::ops::Range {
                start: offset,
                end: offset + len,
            });
            pb.inc(1);
        }
        for _ in 0..header.num_docs {
            let len = prost::decode_length_delimiter(&mut reader)?;
            let offset = input.len() - reader.remaining();
            reader.advance(len);
            doc_data.push(std::ops::Range {
                start: offset,
                end: offset + len,
            });
            pb.inc(1);
        }
        Ok(Self {
            input,
            header,
            plist_data,
            doc_data,
        })
    }

    #[must_use]
    pub fn postings_list(&self, idx: usize) -> PostingsList {
        let location = &self.plist_data[idx];
        let msg_buf = &self.input[location.start..location.end];
        PostingsList::decode(msg_buf).expect("error reading message")
    }

    #[must_use]
    pub fn doc_record(&self, idx: usize) -> DocRecord {
        let location = &self.doc_data[idx];
        let msg_buf = &self.input[location.start..location.end];
        DocRecord::decode(msg_buf).expect("error reading message")
    }

    #[must_use]
    pub fn plist_iter(&'_ self) -> PostingsListIter<'_> {
        PostingsListIter {
            input: &self.input,
            plist_data: &self.plist_data,
            cur: 0,
        }
    }

    #[must_use]
    pub fn doc_record_iter(&'_ self) -> DocRecordIter<'_> {
        DocRecordIter {
            input: &self.input,
            doc_data: &self.doc_data,
            cur: 0,
        }
    }
}

#[derive(Debug)]
pub struct PostingsListIter<'a> {
    input: &'a memmap2::Mmap,
    plist_data: &'a [std::ops::Range<usize>],
    cur: usize,
}

impl<'a> Iterator for PostingsListIter<'a> {
    type Item = PostingsList;
    fn next(&mut self) -> Option<Self::Item> {
        self.cur += 1;
        match self.plist_data.get(self.cur - 1) {
            None => None,
            Some(location) => {
                let msg_buf = &self.input[location.start..location.end];
                Some(PostingsList::decode(msg_buf).expect("error reading message"))
            }
        }
    }
}

impl<'a> ExactSizeIterator for PostingsListIter<'a> {
    fn len(&self) -> usize {
        self.plist_data.len()
    }
}

#[derive(Debug)]
pub struct DocRecordIter<'a> {
    input: &'a memmap2::Mmap,
    doc_data: &'a [std::ops::Range<usize>],
    cur: usize,
}

impl<'a> Iterator for DocRecordIter<'a> {
    type Item = DocRecord;
    fn next(&mut self) -> Option<Self::Item> {
        self.cur += 1;
        match self.doc_data.get(self.cur - 1) {
            None => None,
            Some(location) => {
                let msg_buf = &self.input[location.start..location.end];
                Some(DocRecord::decode(msg_buf).expect("error reading message"))
            }
        }
    }
}

impl<'a> ExactSizeIterator for DocRecordIter<'a> {
    fn len(&self) -> usize {
        self.doc_data.len()
    }
}
