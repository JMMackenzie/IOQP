use bitpacking::BitPacker;
use byteorder::{ReadBytesExt, WriteBytesExt};

#[cfg(not(target_feature = "avx2"))]
type SimdbpCompressor = bitpacking::BitPacker4x;
#[cfg(target_feature = "avx2")]
type SimdbpCompressor = bitpacking::BitPacker8x;

pub const BLOCK_LEN: usize = SimdbpCompressor::BLOCK_LEN;
pub const BLOCK_LEN_M1: usize = BLOCK_LEN - 1;
pub const LARGE_BLOCK_LEN: usize = 64 * BLOCK_LEN;

pub type LargeBuffer = [u32; LARGE_BLOCK_LEN];
pub type Buffer = [u32; BLOCK_LEN];

pub trait Compressor {
    fn compress_sorted_full(initial: u32, input: &[u32], output: &mut [u8]) -> usize;
    fn compress_sorted(initial: u32, input: &[u32], output: &mut [u8]) -> usize;
    fn decompress_sorted_full(initial: u32, input: &[u8], output: &mut [u32]) -> usize;
    fn decompress_sorted(initial: u32, input: &[u8], output: &mut [u32]) -> usize;
}

#[derive(Debug)]
pub struct SimdBPandStreamVbyte;

impl Compressor for SimdBPandStreamVbyte {
    fn compress_sorted_full(initial: u32, input: &[u32], mut output: &mut [u8]) -> usize {
        let bitpacker = SimdbpCompressor::new();
        let num_block_bits = bitpacker.num_bits_sorted(initial, input);
        output.write_u8(num_block_bits).unwrap();
        let bytes = bitpacker.compress_sorted(initial, input, &mut *output, num_block_bits);
        bytes + 1
    }
    fn compress_sorted(initial: u32, input: &[u32], output: &mut [u8]) -> usize {
        streamvbyte::encode_delta_to_buf(input, &mut *output, initial).unwrap()
    }
    fn decompress_sorted_full(initial: u32, input: &[u8], output: &mut [u32]) -> usize {
        let bitpacker = SimdbpCompressor::new();
        let num_bits = unsafe { *input.get_unchecked(0) };
        let compressed_len = (num_bits as usize * BLOCK_LEN) >> 3;
        let compressed = unsafe { input.get_unchecked(1..=compressed_len) };
        let bytes = bitpacker.decompress_sorted(initial, compressed, output, num_bits);
        bytes + 1
    }
    fn decompress_sorted(initial: u32, input: &[u8], output: &mut [u32]) -> usize {
        streamvbyte::decode_delta(input, output, initial)
    }
}

use byteorder::LittleEndian;

#[derive(Debug)]
pub struct Uncompressed;

impl Compressor for Uncompressed {
    fn compress_sorted_full(_initial: u32, input: &[u32], mut output: &mut [u8]) -> usize {
        for val in input {
            output.write_u32::<LittleEndian>(*val).unwrap();
        }
        input.len() * std::mem::size_of::<u32>()
    }
    fn compress_sorted(_initial: u32, input: &[u32], mut output: &mut [u8]) -> usize {
        for val in input {
            output.write_u32::<LittleEndian>(*val).unwrap();
        }
        input.len() * std::mem::size_of::<u32>()
    }
    fn decompress_sorted_full(_initial: u32, mut input: &[u8], output: &mut [u32]) -> usize {
        for out in output.iter_mut() {
            *out = input.read_u32::<LittleEndian>().unwrap();
        }
        output.len() * std::mem::size_of::<u32>()
    }
    fn decompress_sorted(_initial: u32, mut input: &[u8], output: &mut [u32]) -> usize {
        for out in output.iter_mut() {
            *out = input.read_u32::<LittleEndian>().unwrap();
        }
        output.len() * std::mem::size_of::<u32>()
    }
}
