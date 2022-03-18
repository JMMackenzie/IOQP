
#[derive(Clone, Copy)]
pub struct BM25 {
    k1: f32,
    b: f32,
    num_docs: u32,
}

impl BM25 {

    pub fn new (k1: f32, b: f32, num_docs: u32) -> BM25 {
        BM25 {
            k1,
            b,
            num_docs,
        }
    }

    fn term_idf(self, doc_freq: u32) -> f32 {
        let u_idf = (((self.num_docs - doc_freq) as f32 + 0.5) / ((doc_freq as f32) + 0.5)).ln();
        u_idf.max(1.0E-6 as f32) * (1.0 + self.k1)
    }

    fn doc_term_weight(self, term_freq: u32, norm_doc_len: f32) -> f32 {
        let f_tf = term_freq as f32;
        f_tf / (f_tf + self.k1 * (1.0 - self.b + self.b * norm_doc_len))
    }

    pub fn score(self, term_freq: u32, doc_freq: u32, norm_doc_len: f32) -> f32 {
        self.term_idf(doc_freq) * self.doc_term_weight(term_freq, norm_doc_len)
    }
}

const QUANT_BITS:u32 = 8;

#[derive(Clone, Copy)]
pub struct LinearQuantizer {
    global_max: f32,
    scale: f32,
}

impl LinearQuantizer {

    pub fn new(global_max: f32) -> LinearQuantizer {
        LinearQuantizer {
            global_max,
            scale: (1_u32 << (QUANT_BITS)) as f32 / global_max,
        }
    }

    pub fn quantize(self, score: f32) -> u32 {
        assert!(score >= 0_f32 && score <= self.global_max);
        (score * self.scale).ceil() as u32
    }
}
