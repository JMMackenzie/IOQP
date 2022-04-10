use core::f32;

pub trait Scorer: Send + Sync + Copy {
    fn score(self, term_freq: u32, doc_freq: u32, norm_doc_len: f32, num_docs: u32) -> f32;
}

#[derive(Clone, Copy, Debug)]
pub struct BM25 {
    k1: f32,
    b: f32,
}

impl BM25 {
    #[must_use]
    pub fn new(k1: f32, b: f32) -> BM25 {
        BM25 { k1, b }
    }

    fn term_idf(self, doc_freq: u32, num_docs: u32) -> f32 {
        let u_idf = (((num_docs - doc_freq) as f32 + 0.5) / ((doc_freq as f32) + 0.5)).ln();
        u_idf.max(1.0E-6_f32) * (1.0 + self.k1)
    }

    fn doc_term_weight(self, term_freq: u32, norm_doc_len: f32) -> f32 {
        let f_tf = term_freq as f32;
        f_tf / (f_tf + self.k1 * (1.0 - self.b + self.b * norm_doc_len))
    }
}

impl Scorer for BM25 {
    fn score(self, term_freq: u32, doc_freq: u32, norm_doc_len: f32, num_docs: u32) -> f32 {
        self.term_idf(doc_freq, num_docs) * self.doc_term_weight(term_freq, norm_doc_len)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Identity {}

impl Identity {
    #[must_use]
    pub fn new() -> Identity {
        Identity {}
    }
}

impl Default for Identity {
    fn default() -> Self {
        Identity::new()
    }
}

impl Scorer for Identity {
    fn score(self, term_freq: u32, _doc_freq: u32, _norm_doc_len: f32, _num_docs: u32) -> f32 {
        term_freq as f32
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LinearQuantizer {
    global_max: f32,
    scale: f32,
}

impl LinearQuantizer {
    #[must_use]
    pub fn new(global_max: f32, quant_bits: u32) -> LinearQuantizer {
        LinearQuantizer {
            global_max,
            scale: (1_u32 << (quant_bits)) as f32 / global_max,
        }
    }

    /// Quantize the score
    ///
    /// # Panics
    /// Panics is score is outside 0 <= score <= global max
    #[must_use]
    pub fn quantize(self, score: f32) -> u32 {
        assert!(score >= 0_f32 && score <= self.global_max);
        (score * self.scale).ceil() as u32
    }
}
