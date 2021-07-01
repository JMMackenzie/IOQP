mod posting;

use std::collections::HashMap;

#[derive(serde::Serialize, serde::Deserialize)]
struct TermMetaData {
    start: usize,
    len: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Index {
    vocab: HashMap<String, TermMetaData>,
    #[serde(with = "serde_bytes")]
    list_data: Vec<u8>,
}

impl Index {
    #[tracing::instrument]
    pub fn from_ciff_file<P: AsRef<std::path::Path> + std::fmt::Debug>(
        input_file_name: P,
    ) -> anyhow::Result<Self> {
        Ok(Index {
            vocab: HashMap::new(),
            list_data: Vec::new(),
        })
    }
}
