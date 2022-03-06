pub fn progress_bar(name: &str, limit: usize) -> indicatif::ProgressBar {
    let pb = indicatif::ProgressBar::new(limit as u64);
    pb.set_draw_delta(limit as u64 / 200);
    pb.set_style(indicatif::ProgressStyle::default_bar().template(
        &format!("{}: {}",name,"{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len}, ETA {eta}, SPEED: {per_sec})")));
    pb
}

#[cfg(target_feature = "avx2")]
pub unsafe fn determine_max(scores: &[i16], threshold: i16) -> i16 {
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

#[cfg(not(target_feature = "avx2"))]
pub unsafe fn determine_max(scores: &[i16], threshold: i16) -> i16 {
    use std::arch::x86_64::*;
    union SimdToArray {
        array: [i16; 8],
        simd: __m128i,
    }
    let mut threshold = SimdToArray {
        simd: _mm_set_epi16(
                  threshold, threshold, threshold, threshold, threshold, threshold, threshold,
                  threshold,
              ),
    };
    scores.chunks_exact(8).for_each(|chunk| {
        let data_chunk = _mm_loadu_epi16(&chunk[0]);
        threshold.simd = _mm_max_epi16(data_chunk, threshold.simd);
    });
    *threshold.array.iter().max().unwrap()
}
