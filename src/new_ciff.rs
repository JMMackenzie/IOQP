use bytes::Buf;
use prost::Message;

pub mod format {
    include!(concat!(env!("OUT_DIR"), "/ciff.rs"));
}

#[derive(Debug)]
pub enum CiffMessage {
    Header(format::Header),
    PostingsList(format::PostingsList),
    DocRecord(format::DocRecord),
}

#[derive(Debug)]
pub struct Reader {
    pub header: format::Header,
    decoded_messages: crossbeam::channel::Receiver<CiffMessage>,
}

impl Reader {
    pub fn from_file<P: AsRef<std::path::Path>>(ciff_path: P) -> anyhow::Result<Self> {
        let header = {
            let ciff_file = std::fs::File::open(ciff_path.as_ref())?;
            let input = unsafe { memmap2::Mmap::map(&ciff_file)? };
            format::Header::decode_length_delimited(&input[..])
        }?;
        let (sender, r) = crossbeam::channel::bounded(20000);
        let ciff_path = ciff_path.as_ref().to_path_buf();
        std::thread::spawn(move || read_messages(sender, ciff_path));
        Ok(Self {
            decoded_messages: r,
            header,
        })
    }
}

fn read_messages<P: AsRef<std::path::Path>>(
    sender: crossbeam::channel::Sender<CiffMessage>,
    ciff_path: P,
) {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let ciff_file = std::fs::File::open(ciff_path).expect("can't open ciff file");
    let input = unsafe { memmap2::Mmap::map(&ciff_file).expect("can't mmap ciff file") };
    let mut input = input.as_ref();
    let header =
        format::Header::decode_length_delimited(&mut input).expect("error reading message");
    pool.scope(|s| {
        for _ in 0..header.num_postings_lists {
            let msg_len =
                prost::decode_length_delimiter(&mut input).expect("error reading msg len");
            let msg_buf = input.copy_to_bytes(msg_len);
            let local_sender = sender.clone();
            s.spawn(move |_s1| {
                let postings_list =
                    format::PostingsList::decode(msg_buf).expect("error reading message");
                local_sender
                    .send(CiffMessage::PostingsList(postings_list))
                    .expect("error can't send message");
            });
        }
    });
    pool.scope(|s| {
        for _ in 0..header.num_docs {
            let msg_len =
                prost::decode_length_delimiter(&mut input).expect("error reading msg len");
            let msg_buf = input.copy_to_bytes(msg_len);
            let local_sender = sender.clone();
            s.spawn(move |_s1| {
                let postings_list =
                    format::DocRecord::decode(msg_buf).expect("error reading message");
                local_sender
                    .send(CiffMessage::DocRecord(postings_list))
                    .expect("error can't send message");
            });
        }
    });
}

impl Iterator for Reader {
    type Item = CiffMessage;
    fn next(&mut self) -> Option<Self::Item> {
        self.decoded_messages.recv().ok()
    }
}
