use criterion::BatchSize;
use criterion::Throughput;
use ioqp::impact::Impact;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use rand::Rng;

type Compressor = ioqp::compress::SimdBPandStreamVbyte;

fn decode_list(
    meta_data: ioqp::impact::MetaData,
    data: &[u8],
    large_decode_buf: &mut [u32; ioqp::compress::LARGE_BLOCK_LEN],
    decode_buf: &mut [u32; ioqp::compress::BLOCK_LEN],
) -> u64 {
    let mut impact = Impact::from_encoded_slice(meta_data, ioqp::Byte::from_slice(data));
    let mut sum: u64 = 0;
    while let Some(chunk) = impact.next_large_chunk::<Compressor>(data, large_decode_buf) {
        for doc_id in chunk {
            sum += *doc_id as u64;
        }
    }
    while let Some(chunk) = impact.next_chunk::<Compressor>(data, decode_buf) {
        chunk.iter().for_each(|doc_id| {
            sum += *doc_id as u64;
        });
    }
    sum
}

fn create_list(
    seq_len: usize,
    gap_range: core::ops::Range<u32>,
) -> (ioqp::impact::MetaData, Vec<u8>) {
    let mut increasing_seq = Vec::<u32>::with_capacity(seq_len as usize);
    let mut rng = rand::thread_rng();
    let mut last: u32 = 0;
    for _ in 0..seq_len {
        let gap = rng.gen_range(gap_range.clone());
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
    Impact::encode::<Compressor>(1, &increasing_seq)
}

fn bench_decode_impacts(c: &mut Criterion) {
    let mut decode_buf = [0u32; ioqp::compress::BLOCK_LEN];
    let mut large_decode_buf = [0u32; ioqp::compress::LARGE_BLOCK_LEN];

    static K: usize = 1000;
    let mut group = c.benchmark_group("decode");
    for size in [K, 4 * K, 16 * K, 64 * K, 256 * K, 1024 * K].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || create_list(size, 1..1000),
                |(meta_data, data)| {
                    decode_list(meta_data, &data, &mut large_decode_buf, &mut decode_buf)
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_decode_impacts);
criterion_main!(benches);
