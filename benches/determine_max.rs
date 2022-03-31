#![feature(stdsimd)]

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};

#[cfg(target_feature = "avx2")]
/// determine max fast
///
/// # Safety
///
/// this works hopefully
pub fn determine_max_avx2(scores: &[i16], threshold: i16) -> i16 {
    unsafe {
        use std::arch::x86_64::*;
        union SimdToArray {
            array: [i16; 16],
            simd: __m256i,
        }
        let mut threshold = SimdToArray {
            simd: _mm256_set1_epi16(threshold),
        };
        scores.chunks_exact(16).for_each(|chunk| {
            let data_chunk = _mm256_loadu_epi16(&chunk[0]);
            threshold.simd = _mm256_max_epi16(data_chunk, threshold.simd);
        });
        *threshold.array.iter().max().unwrap()
    }
}

pub fn determine_max_simple_cloned(scores: &[i16], threshold: i16) -> i16 {
    scores.iter().cloned().max().unwrap().max(threshold)
}

pub fn determine_max_simple(scores: &[i16], threshold: i16) -> i16 {
    *scores.iter().max().unwrap().max(&threshold)
}

fn bench_decode_impacts(c: &mut Criterion) {
    use rand::distributions::{Distribution, Uniform};
    let mut rng = rand::thread_rng();
    let score_range = Uniform::from(1i16..10000i16);
    let num_scores = 256 * 256 * 256;
    let scores: Vec<i16> = score_range.sample_iter(&mut rng).take(num_scores).collect();
    let mut group = c.benchmark_group("determine_max");
    group.bench_with_input(
        BenchmarkId::new("determine_max_avx2", num_scores),
        &scores,
        |b, s| {
            b.iter(|| determine_max_avx2(&s, 10));
        },
    );
    group.bench_with_input(
        BenchmarkId::new("determine_max_simple", num_scores),
        &scores,
        |b, s| {
            b.iter(|| determine_max_simple(&s, 10));
        },
    );
    group.bench_with_input(
        BenchmarkId::new("determine_max_simple_cloned", num_scores),
        &scores,
        |b, s| {
            b.iter(|| determine_max_simple_cloned(&s, 10));
        },
    );
    group.finish();
}

criterion_group!(benches, bench_decode_impacts);
criterion_main!(benches);
