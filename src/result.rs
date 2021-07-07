use std::io::Write;
use std::cmp::Ordering;

#[derive(Eq)]
pub struct SearchResult {
    pub doc_id: u32,
    pub score: crate::ScoreType,
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> Ordering {
        other.score.cmp(&self.score)
    }
}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

pub struct SearchResults {
    pub topk: Vec<SearchResult>,
    pub took: std::time::Duration,
    pub qid: usize,
}

impl SearchResults {

    pub fn to_trec_file(&self, mut output: &std::fs::File) {
        for (rank, res) in self.topk.iter().enumerate() {
            writeln!(output, "{} Q0 {} {} {} ioqp", self.qid, res.doc_id, rank+1, res.score).unwrap();
        }
    }

    pub fn _to_tsv_file(&self, mut output: &std::fs::File) {
        for (rank, res) in self.topk.iter().enumerate() {
            writeln!(output, "{} {} {}", self.qid, res.doc_id, rank+1).unwrap();
        }
    }
}

impl std::fmt::Display for SearchResults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "query {} took: {}ms [", self.qid, self.took.as_millis())?;
        for (rank, res) in self.topk.iter().enumerate() {
            write!(f, "#{},({},{})", rank + 1, res.doc_id, res.score)?;
            if rank + 1 != self.topk.len() {
                write!(f, ",")?
            }
        }
        write!(f, "]")
    }
}
