use std::ops::Index;

#[derive(Debug)]
pub struct ByteRange {
    start: usize,
    stop: usize,
}

impl ByteRange {
    pub fn new(start: usize, stop: usize) -> Self {
        Self { start, stop }
    }

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

impl Index<&ByteRange> for [u8] {
    type Output = [u8];

    fn index(&self, range: &ByteRange) -> &Self::Output {
        &self[..][range.start..range.stop]
    }
}
