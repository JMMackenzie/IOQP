use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use bitpacking::BitPacker;

type SimdbpCompressor = bitpacking::BitPacker4x;
pub const BLOCK_LEN: usize = SimdbpCompressor::BLOCK_LEN;
pub const BLOCK_LEN_M1: usize = BLOCK_LEN - 1;

#[derive(Copy, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MetaData {
    pub impact: u16,
    pub count: u32,
    pub bytes: u32,
}

pub struct Impact<'index> {
    pub meta_data: MetaData,
    pub remaining_u32s: usize,
    pub bytes: &'index [u8],
    pub initial: u32,
    pub decompressor: SimdbpCompressor,
}

impl<'index> Impact<'index> {
    pub fn from_encoded_slice(meta_data: MetaData, bytes: &'index [u8]) -> Impact<'index> {
        Impact {
            remaining_u32s: meta_data.count as usize,
            meta_data,
            initial: 0,
            bytes,
            decompressor: SimdbpCompressor::new(),
        }
    }

    pub fn encode(impact: u16, docs: &[u32]) -> (MetaData, Vec<u8>) {
        let mut output = vec![];
        // (3) write bpi for each block and compress
        let bitpacker = SimdbpCompressor::new();
        let mut initial: u32 = 0;
        let mut compressed = vec![0u8; 8 * BLOCK_LEN];
        docs.chunks(BLOCK_LEN).for_each(|chunk| {
            let compressed_len = match chunk.len() {
                // full blocks -> SIMDBP
                BLOCK_LEN => {
                    let num_block_bits = bitpacker.num_bits_sorted(initial, chunk);
                    output.write_u8(num_block_bits).unwrap();
                    bitpacker.compress_sorted(initial, &chunk, &mut compressed[..], num_block_bits)
                }
                // non-full block -> streamvbyte
                _ => {
                    streamvbyte::encode_delta_to_buf(&chunk, &mut compressed[..], initial).unwrap()
                }
            };
            output.extend_from_slice(&compressed[..compressed_len]);
            initial = *chunk.last().unwrap();
        });
        (
            MetaData {
                impact,
                count: docs.len() as u32,
                bytes: output.len() as u32,
            },
            output,
        )
    }

    pub fn next_chunk<'buf>(
        &mut self,
        output_buf: &'buf mut [u32; BLOCK_LEN],
    ) -> Option<&'buf [u32]> {
        // nothing decoded left. decode more
        match self.remaining_u32s {
            0 => return None,
            1..=BLOCK_LEN_M1 => {
                let out_buf_start = BLOCK_LEN - self.remaining_u32s;
                self.remaining_u32s = 0;
                streamvbyte::decode_delta(
                    self.bytes,
                    &mut output_buf[out_buf_start..],
                    self.initial,
                );
                self.bytes = &self.bytes[self.bytes.len()..];
                Some(&output_buf[out_buf_start..])
            }
            _ => {
                // full block
                self.remaining_u32s -= BLOCK_LEN;
                let num_bits = self.bytes.read_u8().unwrap();
                let compressed_len = num_bits as usize * BLOCK_LEN / 8;
                self.decompressor.decompress_sorted(
                    self.initial,
                    &self.bytes[..compressed_len],
                    &mut output_buf[..],
                    num_bits,
                );
                self.bytes = &self.bytes[compressed_len..];
                self.initial = output_buf[BLOCK_LEN - 1];
                Some(&output_buf[..])
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn recover_small() {
        let org_impact = 123;
        let docs: Vec<u32> = vec![
            55, 101, 184, 419, 669, 812, 1067, 1181, 1261, 1428500555, 1428500722, 1428557200,
            1428557376, 1428557549, 1428557592, 1428557709, 1428557820, 1428557937, 1428558096,
            1428559010, 1428559129, 1428559322, 1428559577, 1428559578, 1428559729, 1428559870,
            1428559888, 1428559965, 1428560218, 1428560448, 1428560586, 1428560587, 1428560842,
            1428561077, 1428561121, 1428561353, 1428561354, 1428575668, 1428575779, 1428576005,
            1428576032, 1428576138, 1428576165, 1428576207, 1428576367, 1428576473, 1428576579,
            1428576704, 1428576856, 1428577030, 1428577091, 3995637692, 3995637709, 3995637774,
            3995638028, 3995638181, 3995638217, 3995638404, 3995638654, 3995638693, 3995638915,
            3995639146, 3995639367, 3995639387, 3995639545, 3995639728, 3995639948, 3995640111,
            3995640241, 3995640330, 3995640389, 3995640390, 3995640486, 3995640595, 3995640702,
            3995640738, 3995640803, 3995640846, 3995640972, 3995641111, 3995641351, 3995641487,
            3995641595, 3995641779, 3995642034, 3995642079, 3995642154, 3995642344, 3995642533,
            3995642714, 3995642931, 3995643143, 3995643284, 3995643401, 3995643556, 3995643689,
            3995643770, 3995644018, 3995644057, 3995644233, 3995644252, 3995644271, 3995644433,
            3995644650, 3995644797, 3995644952, 3995645207, 3995645353, 3995645516, 3995645752,
            3995645962, 3995646204, 3995646340, 3995646423, 3995646666, 3995646844, 4204496253,
            4204496255, 4204496472, 4204496677, 4204496828, 4204496878, 4204497115, 4204497244,
            4204497293, 4204497356, 4204497474, 4204497652,
        ];
        let (meta_data, encoded) = Impact::encode(org_impact, &docs);

        let data = &encoded[..];
        let mut decode_buf = [0u32; BLOCK_LEN];
        let mut recovered = Impact::from_encoded_slice(meta_data, &data);
        let mut doc_iter = docs.into_iter();
        while let Some(chunk) = recovered.next_chunk(&mut decode_buf) {
            for num in chunk {
                assert_eq!(Some(*num), doc_iter.next());
            }
        }
        assert_eq!(None, doc_iter.next());
    }

    #[derive(Debug, Clone)]
    pub(crate) struct ImpactList {
        pub impact: u16,
        pub docs: Vec<u32>,
    }

    impl quickcheck::Arbitrary for ImpactList {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let seq_len = u16::arbitrary(g) as usize;
            let mut increasing_seq = Vec::<u32>::with_capacity(seq_len);
            let mut last: u32 = 0;
            for _ in 0..seq_len {
                let gap_type = u8::arbitrary(g);
                let gap = match gap_type {
                    0 => u32::arbitrary(g),
                    1 => u16::arbitrary(g) as u32,
                    _ => u8::arbitrary(g) as u32,
                };
                match last.checked_add(gap) {
                    Some(next_num) => {
                        if next_num != last {
                            increasing_seq.push(next_num);
                            last = next_num;
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
            ImpactList {
                impact: u16::arbitrary(g),
                docs: increasing_seq,
            }
        }
    }

    #[quickcheck_macros::quickcheck]
    fn successfully_decode_impact(impact_list: ImpactList) -> bool {
        let impact = impact_list.impact;
        let docs = impact_list.docs;
        let (meta_data, encoded) = Impact::encode(impact, &docs);
        let data = &encoded[..];
        let mut decode_buf = [0u32; BLOCK_LEN];
        let mut recovered = Impact::from_encoded_slice(meta_data, &data);
        let mut doc_iter = docs.into_iter();
        let mut all_good = true;
        while let Some(chunk) = recovered.next_chunk(&mut decode_buf) {
            for num in chunk {
                if Some(*num) != doc_iter.next() {
                    all_good = false;
                    break;
                }
            }
        }
        if doc_iter.next() != None {
            all_good = false;
        }
        all_good
    }

    #[quickcheck_macros::quickcheck]
    fn encoded_size_correct(impact_list: ImpactList) -> bool {
        let impact = impact_list.impact;
        let docs = impact_list.docs;
        let (meta_data, encoded) = Impact::encode(impact, &docs);
        meta_data.bytes as usize == encoded.len()
    }

    #[test]
    fn ensure_64bit_impact_metadata_size() {
        assert_eq!(std::mem::size_of::<MetaData>(), 12);
    }
}