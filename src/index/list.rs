use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use std::convert::TryInto;

use super::impact::Impact;

struct ImpactList<'index> {
    num_levels: u16,
    data_bytes: &'index [u8],
}

impl<'index> ImpactList<'index> {
    pub fn from_encoded_slice(mut data_bytes: &mut &'index [u8]) -> ImpactList<'index> {
        ImpactList {
            num_levels: data_bytes.read_u16::<byteorder::LittleEndian>().unwrap(),
            data_bytes,
        }
    }

    pub fn into_encoded_vec(impacts: Vec<(u16, Vec<u32>)>) -> Vec<u8> {
        let mut output = vec![];
        output
            .write_u16::<byteorder::LittleEndian>(
                impacts
                    .len()
                    .try_into()
                    .expect("more than 2^16 impact levels"),
            )
            .unwrap();

        for slice in impacts
            .into_iter()
            .map(|(impact, docs)| Impact::into_encoded_vec(impact, &docs))
        {
            output.extend_from_slice(&slice);
        }

        output
    }
}

impl<'index> Iterator for ImpactList<'index> {
    type Item = Impact<'index>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.data_bytes.is_empty() {
            true => None,
            false => Some(Impact::from_encoded_slice(&mut self.data_bytes)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_simple() {
        let first = (123, (1..12344).step_by(8).collect());
        let second = (456, vec![41, 423, 453, 464, 4128]);
        let lists = vec![first, second];
        let output = ImpactList::into_encoded_vec(lists);
        dbg!(output);
    }
}
