use std::ops::Index;

#[derive(Debug)]
pub struct Byte {
    start: usize,
    stop: usize,
}

impl Byte {
    #[must_use]
    pub fn new(start: usize, stop: usize) -> Self {
        Self { start, stop }
    }

    #[must_use]
    pub fn from_slice(data: &[u8]) -> Self {
        Self {
            start: 0,
            stop: data.len(),
        }
    }

    pub fn advance(&mut self, bytes: usize) {
        self.start += bytes;
    }
}

impl Index<&Byte> for [u8] {
    type Output = [u8];

    fn index(&self, range: &Byte) -> &Self::Output {
        &self[..][range.start..range.stop]
    }
}
