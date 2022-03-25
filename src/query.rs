use std::io::BufRead;
use std::collections::HashMap;

pub const MAX_QUERY_WEIGHT:usize = 32;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Term {
    pub token: String,
    pub freq: u32,
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
            let freq = freq.min(MAX_QUERY_WEIGHT as u32);
            tokens.push(Term { token, freq });
        }
        Ok(Query { id, tokens })
    }
}

pub fn read_queries<P: AsRef<std::path::Path> + std::fmt::Debug>(
    qry_file: P,
) -> anyhow::Result<Vec<Query>> {
    let qry_file = std::fs::File::open(qry_file)?;
    let qry_file = std::io::BufReader::new(qry_file);
    let queries = qry_file
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| l.parse::<Query>().ok())
        .collect();
    Ok(queries)
}


