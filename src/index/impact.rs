use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use bitpacking::BitPacker;

type SimdbpCompressor = bitpacking::BitPacker4x;
const BLOCK_LEN: usize = SimdbpCompressor::BLOCK_LEN;

struct Header<'list> {
    impact: u16,
    // for each block we store the num_bits_per_int value in 8-bits
    // from that we can compute the compressed size
    block_bits: &'list [u8],
}

pub struct Impact<'list> {
    header: Header<'list>,
    data_bytes: &'list [u8],
}

impl<'list> Impact<'list> {
    pub fn from_encoded_slice(mut data_bytes: &mut &'list [u8]) -> Impact<'list> {
        // (1) read impact
        let impact = data_bytes.read_u16::<LittleEndian>().unwrap();
        // (2) read num blocks
        let num_blocks = data_bytes.read_u32::<LittleEndian>().unwrap();
        let total_bytes = data_bytes.read_u32::<LittleEndian>().unwrap();
        let (block_bits, data_bytes) = data_bytes.split_at(num_blocks as usize);
        Impact {
            header: Header { impact, block_bits },
            data_bytes: &data_bytes[..total_bytes as usize],
        }
    }

    pub fn into_encoded_vec(impact: u16, docs: &[u32]) -> Vec<u8> {
        let mut output = vec![];
        // (1) write impact
        output.write_u16::<LittleEndian>(impact).unwrap();
        // (2) write num blocks
        let num_blocks: u32 = ((docs.len() + docs.len().rem_euclid(BLOCK_LEN)) / BLOCK_LEN) as u32;
        output.write_u32::<LittleEndian>(num_blocks).unwrap();

        // (3) write bpi for each block
        let mut initial: u32 = 0;
        let bitpacker = SimdbpCompressor::new();
        docs.chunks(BLOCK_LEN).for_each(|chunk| {
            if chunk.len() == BLOCK_LEN {
                let num_block_bits = bitpacker.num_bits_sorted(initial, chunk);
                dbg!(num_block_bits);
                output.write_u8(num_block_bits).unwrap();
                initial = *chunk.last().unwrap();
            } else {
                output.write_u8(0).unwrap();
            }
        });

        // (4) compress each block
        let mut initial: u32 = 0;
        let mut compressed = vec![0u8; 4 * BLOCK_LEN];
        docs.chunks(BLOCK_LEN).for_each(|chunk| {
            if chunk.len() == BLOCK_LEN {
                let num_block_bits = bitpacker.num_bits_sorted(initial, chunk);
                let compressed_len =
                    bitpacker.compress(&chunk, &mut compressed[..], num_block_bits);
                output.extend_from_slice(&compressed[..compressed_len]);
                initial = *chunk.last().unwrap();
            } else {
                output.write_u8(0).unwrap();
                let compressed_len =
                    streamvbyte::encode_delta_to_buf(&chunk, &mut compressed[..], initial).unwrap();
                output.extend_from_slice(&compressed[..compressed_len]);
            }
        });
        output
    }
}

impl<'list> Iterator for Impact<'list> {
    type Item = &'list [u8];
    fn next(&mut self) -> Option<Self::Item> {
        match self.data_bytes.is_empty() {
            true => None,
            false => Some(self.data_bytes),
        }
    }
}
