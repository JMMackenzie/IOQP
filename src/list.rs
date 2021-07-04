use crate::impact;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct List {
    pub impacts: smallvec::SmallVec<[impact::MetaData; 32]>,
    pub start_byte_offset: usize,
}

impl List {
    pub fn encode(input: &[(u16, Vec<u32>)]) -> (List, Vec<u8>) {
        let mut output = vec![];
        let mut impacts = smallvec::SmallVec::new();
        for (meta_data, data) in input
            .into_iter()
            .map(|(impact, docs)| impact::Impact::encode(*impact, &docs))
        {
            impacts.push(meta_data);
            output.extend_from_slice(&data);
        }
        (
            List {
                impacts,
                start_byte_offset: 0,
            },
            output,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[quickcheck_macros::quickcheck]
    fn successfully_decode_many_impactlists_len(
        impact_lists: Vec<crate::impact::tests::ImpactList>,
    ) -> bool {
        let input: Vec<(u16, Vec<u32>)> = impact_lists
            .into_iter()
            .map(|il| (il.impact, il.docs))
            .collect();

        let (encoded_list, _) = List::encode(&input);
        encoded_list.impacts.len() == input.len()
    }

    #[quickcheck_macros::quickcheck]
    fn successfully_decode_many_impactlists_count(
        impact_lists: Vec<crate::impact::tests::ImpactList>,
    ) -> bool {
        let input: Vec<(u16, Vec<u32>)> = impact_lists
            .into_iter()
            .map(|il| (il.impact, il.docs))
            .collect();

        let (encoded_list, _) = List::encode(&input);
        let mut all_good: bool = true;
        for (encoded_list, input_list) in encoded_list.impacts.iter().zip(input) {
            if encoded_list.count as usize != input_list.1.len() {
                all_good = false;
                break;
            }
        }
        all_good
    }

    #[quickcheck_macros::quickcheck]
    fn successfully_decode_many_impactlists_compressed_size(
        impact_lists: Vec<crate::impact::tests::ImpactList>,
    ) -> bool {
        let input: Vec<(u16, Vec<u32>)> = impact_lists
            .into_iter()
            .map(|il| (il.impact, il.docs))
            .collect();

        let (encoded_list, encoded_data) = List::encode(&input);
        let total_size: usize = encoded_list.impacts.iter().map(|m| m.bytes as usize).sum();
        total_size == encoded_data.len()
    }

    #[quickcheck_macros::quickcheck]
    fn successfully_decode_many_impactlists_content(
        impact_lists: Vec<crate::impact::tests::ImpactList>,
    ) -> bool {
        let input: Vec<(u16, Vec<u32>)> = impact_lists
            .into_iter()
            .map(|il| (il.impact, il.docs))
            .collect();

        let (encoded_list, encoded_data) = List::encode(&input);

        let mut cur_offset: usize = 0;
        let mut all_good = true;
        encoded_list
            .impacts
            .into_iter()
            .zip(input.into_iter())
            .for_each(|(meta_data, (_, docs))| {
                let stop = cur_offset + meta_data.bytes as usize;
                let data = &encoded_data[cur_offset..stop];
                let mut decode_buf = [0u32; impact::BLOCK_LEN];
                let mut recovered = impact::Impact::from_encoded_slice(meta_data, &data);
                let mut doc_iter = docs.into_iter();
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
                cur_offset = stop;
            });
        all_good
    }
}
