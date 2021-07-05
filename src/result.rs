use std::cmp::Ordering;

#[derive(Eq)]
pub struct SearchResult {
    pub doc_id: u32,
    pub score: u16,
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
}
