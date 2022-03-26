use std::io::BufRead;
use std::collections::HashMap;
use std::cmp::Ordering;

pub const MAX_TERM_WEIGHT:usize = 32;

#[derive(Eq, Clone, serde::Serialize, serde::Deserialize, Debug)]
pub struct Term {
    pub token: String,
    pub freq: u32,
}

impl PartialEq for Term {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl Ord for Term {
    fn cmp(&self, other: &Self) -> Ordering {
        other.token.cmp(&self.token)
    }
}

impl PartialOrd for Term {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Query {
    pub id: usize,
    pub tokens: Vec<Term>,
}

impl std::str::FromStr for Query {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        let id = parts[0].parse::<usize>()?;
        let terms: Vec<String> = parts[1].split_whitespace().map(|s| s.to_owned()).collect();
        let mut token_freqs : HashMap<String, u32> = HashMap::new();
        for t in terms.iter() {
            *token_freqs.entry(t.to_string()).or_insert(0) += 1;
        }
        let mut tokens = Vec::new();
        for (token, freq) in token_freqs {
            tokens.push(Term { token, freq });
        }
        Ok( Query { id, tokens })
    }
}

impl Query {

    // Constructor which rescales query weights uniformly into [1, max_weight]
    pub fn with_rescale(id: usize, mut tokens: Vec<Term>, max_weight: usize) -> Self {
        let max_tok_weight = tokens.iter().map(|p| p.freq).max().unwrap() as usize;
        if max_tok_weight <= max_weight {
            return Self { id, tokens }
        }
        for i in tokens.iter_mut() {
            i.freq = (max_weight as f32 * (i.freq as f32) / (max_tok_weight as f32)).ceil() as u32;
        }
        Self { id, tokens }
    }

    // Rescales query terms in-place uniformly into [1, max_weight]
    pub fn rescale(&mut self, max_weight: usize) {
        let max_tok_weight = self.tokens.iter().map(|p| p.freq).max().unwrap() as usize;
        if max_tok_weight > max_weight {
            for i in self.tokens.iter_mut() {
                i.freq = (max_weight as f32 * (i.freq as f32) / (max_tok_weight as f32)).ceil() as u32;
            }
        }
    }

}

pub fn read_queries<P: AsRef<std::path::Path> + std::fmt::Debug>(
    qry_file: P,
    weighted: bool,
) -> anyhow::Result<Vec<Query>> {
    let qry_file = std::fs::File::open(qry_file)?;
    let qry_file = std::io::BufReader::new(qry_file);
    let mut queries:Vec<Query> = qry_file
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| l.parse::<Query>().ok())
        .collect();

    //  Re-scale to max range or 1
    for query in &mut queries {
        if weighted {
            query.rescale(MAX_TERM_WEIGHT);
        } else {
            query.rescale(1);
        }
    }

    Ok(queries)
}
