use std::borrow::ToOwned;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::BufRead;

pub const MAX_TERM_WEIGHT: usize = 32;

#[derive(Eq, Clone, serde::Serialize, serde::Deserialize, Debug)]
pub struct Term {
    pub token: String,
    pub freq: u32,
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(tok:{}, freq:{})", &self.token, self.freq)
    }
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

#[derive(Eq, serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Query {
    pub id: usize,
    pub tokens: Vec<Term>,
}

impl PartialEq for Query {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.tokens == other.tokens
    }
}

impl std::str::FromStr for Query {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.splitn(2, ":").collect();
        let id = parts[0].parse::<usize>()?;
        let terms: Vec<String> = parts[1].split_whitespace().map(ToOwned::to_owned).collect();
        let mut token_freqs: HashMap<String, u32> = HashMap::new();
        for t in &terms {
            *token_freqs.entry(t.to_string()).or_insert(0) += 1;
        }
        let mut tokens = Vec::new();
        for (token, freq) in token_freqs {
            tokens.push(Term { token, freq });
        }
        tokens.sort();
        Ok(Query { id, tokens })
    }
}

impl Query {
    /// Constructor which rescales query weights uniformly into [1, `max_weight`]
    ///
    /// # Panics
    /// Panics if there are no tokens in the query
    #[must_use]
    pub fn with_rescale(id: usize, mut tokens: Vec<Term>, max_weight: usize) -> Self {
        let max_tok_weight = tokens.iter().map(|p| p.freq).max().unwrap() as usize;
        if max_tok_weight <= max_weight {
            return Self { id, tokens };
        }
        for i in &mut tokens {
            i.freq = (max_weight as f32 * (i.freq as f32) / (max_tok_weight as f32)).ceil() as u32;
        }
        Self { id, tokens }
    }

    // Rescales query terms in-place uniformly into [1, `max_weight`]
    ///
    /// # Panics
    /// Panics if there are no tokens in the query
    pub fn rescale(&mut self, max_weight: usize) {
        let max_tok_weight = self.tokens.iter().map(|p| p.freq).max().unwrap() as usize;
        if max_tok_weight > max_weight {
            for i in &mut self.tokens {
                i.freq =
                    (max_weight as f32 * (i.freq as f32) / (max_tok_weight as f32)).ceil() as u32;
            }
        }
    }
}

// Rescales query terms in-place uniformly into [1, `max_weight`]
///
/// # Errors
///
/// - Can't open open file
///
pub fn read_queries<P: AsRef<std::path::Path> + std::fmt::Debug>(
    qry_file: P,
    weighted: bool,
) -> anyhow::Result<Vec<Query>> {
    let qry_file = std::fs::File::open(qry_file)?;
    let qry_file = std::io::BufReader::new(qry_file);
    let mut queries: Vec<Query> = qry_file
        .lines()
        .filter_map(std::result::Result::ok)
        .filter_map(|l| l.parse::<Query>().ok())
        .filter(|q| q.tokens.len() > 0) // throws away any 0-length queries
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

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn parse_multi_colon() {
        let query = "111:this has no colon";
        let expected = Query {
            id: 111,
            tokens: vec![
                Term {
                    token: "this".to_string(),
                    freq: 1,
                },
                Term {
                    token: "no".to_string(),
                    freq: 1,
                },
                Term {
                    token: "has".to_string(),
                    freq: 1,
                },
                Term {
                    token: "colon".to_string(),
                    freq: 1,
                },
            ],
        };
        assert_eq!(expected, query.parse::<Query>().unwrap());

        let query = "112:this has : one colon";
        let expected = Query {
            id: 112,
            tokens: vec![
                Term {
                    token: "this".to_string(),
                    freq: 1,
                },
                Term {
                    token: "one".to_string(),
                    freq: 1,
                },
                Term {
                    token: "has".to_string(),
                    freq: 1,
                },
                Term {
                    token: "colon".to_string(),
                    freq: 1,
                },
                Term {
                    token: ":".to_string(),
                    freq: 1,
                },
            ],
        };
        assert_eq!(expected, query.parse::<Query>().unwrap());

        let query = "113:this has : : : many : : : colons";
        let expected = Query {
            id: 113,
            tokens: vec![
                Term {
                    token: "this".to_string(),
                    freq: 1,
                },
                Term {
                    token: "many".to_string(),
                    freq: 1,
                },
                Term {
                    token: "has".to_string(),
                    freq: 1,
                },
                Term {
                    token: "colons".to_string(),
                    freq: 1,
                },
                Term {
                    token: ":".to_string(),
                    freq: 6,
                },
            ],
        };
        assert_eq!(expected, query.parse::<Query>().unwrap());
    }
}
