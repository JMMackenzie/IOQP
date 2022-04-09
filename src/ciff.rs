pub mod proto;

pub enum Record {
    PostingsList(proto::PostingsList),
    Document {
        doc_id: u32,
        external_id: String,
        length: u32,
    },
}

pub struct Reader<'a> {
    input: protobuf::CodedInputStream<'a>,
    num_records: usize,
    num_postings_lists: usize,
    avg_doclen: f64,
    postings_left: usize,
    docs_left: usize,
}

impl<'a> Reader<'a> {
    pub fn new<T: 'a + std::io::BufRead + std::fmt::Debug>(
        input: &'a mut T,
    ) -> anyhow::Result<Reader<'a>> {
        let mut input = protobuf::CodedInputStream::from_buffered_reader(input);
        let header = input.read_message::<proto::Header>()?;
        Ok(Reader {
            input,
            postings_left: header.get_num_postings_lists() as usize,
            avg_doclen: header.get_average_doclength() as f64,
            docs_left: header.get_num_docs() as usize,
            num_postings_lists: header.get_num_postings_lists() as usize,
            num_records: header.get_num_postings_lists() as usize + header.get_num_docs() as usize,
        })
    }

    pub fn num_postings_lists(&self) -> usize {
        self.num_postings_lists
    }

    pub fn average_doclength(&self) -> f64 {
        self.avg_doclen
    }
}

impl<'a> ExactSizeIterator for Reader<'a> {
    fn len(&self) -> usize {
        self.num_records
    }
}

impl<'a> Iterator for Reader<'a> {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        if self.postings_left != 0 {
            self.postings_left -= 1;
            return match self.input.read_message::<proto::PostingsList>() {
                Ok(record) => Some(Record::PostingsList(record)),
                Err(e) => {
                    println!("Error parsing CIFF postingslist: {}", e);
                    None
                }
            };
        }
        if self.docs_left != 0 {
            self.docs_left -= 1;
            return match self.input.read_message::<proto::DocRecord>() {
                Ok(record) => {
                    Some(Record::Document {
                        doc_id: record.get_docid() as u32, // todo fix this...
                        external_id: record.get_collection_docid().to_string(),
                        length: record.get_doclength() as u32, // todo fix this...
                    })
                }
                Err(e) => {
                    println!("Error parsing CIFF document: {}", e);
                    None
                }
            };
        }
        None
    }
}
