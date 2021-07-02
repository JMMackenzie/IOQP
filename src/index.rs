mod impact;
mod list;

use std::collections::HashMap;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct TermMetaData {
    start: usize,
    len: usize,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
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

    #[tracing::instrument(skip(self))]
    pub fn write_to_file<P: AsRef<std::path::Path> + std::fmt::Debug>(
        &self,
        output_file_name: P,
    ) -> anyhow::Result<()> {
        let output_file = std::fs::File::create(output_file_name)?;
        let output_file = std::io::BufWriter::new(output_file);
        bincode::serialize_into(output_file, &self)?;

        Ok(())
    }
}
