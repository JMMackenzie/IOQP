mod posting;

pub struct Index {}

impl Index {
    #[tracing::instrument]
    pub fn from_ciff_file<P: AsRef<std::path::Path> + std::fmt::Debug>(
        input_file_name: P,
    ) -> anyhow::Result<Self> {
        Ok(Index {})
    }
}
