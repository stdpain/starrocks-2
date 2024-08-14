#pragma once

#include <emmintrin.h>
#include <immintrin.h>

#include <cstddef>
#include <cstdint>

#include "util/phmap/phmap.h"

namespace SIMD {
inline int selection_to_branchless(uint16_t from, uint16_t to, uint8_t* selection, uint16_t* selected_idx) {
    uint16_t selected_size = 0;

    const __m128i all0 = _mm_setzero_si128();
    constexpr size_t kBatchNums = 16;
    __m256i start = _mm256_set1_epi16(from);
    __m256i inc = _mm256_set1_epi16(16);
    __m256i delta = _mm256_setr_epi16(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    delta = _mm256_add_epi16(delta, start);

    while (from + kBatchNums <= to) {
        const auto f = _mm_loadu_si128(reinterpret_cast<const __m128i*>(selection + from));
        const auto mask = _mm_movemask_epi8(_mm_cmpgt_epi8(f, all0));

        if (mask == 0) {
            // nothing to do
        } else if (mask == 0xffff) {
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(selected_idx + selected_size), delta);
            for (int j = 0; j < 16; j++) {
                DCHECK_EQ(selected_idx[selected_size + j], from + j);
            }
            selected_size += kBatchNums;
        } else {
            phmap::priv::BitMask<uint32_t, 16> bitmask(mask);
            for (auto idx : bitmask) {
                DCHECK(selection[from + idx]);
                selected_idx[selected_size++] = from + idx;
            }
        }
        from += kBatchNums;
        delta = _mm256_add_epi32(inc, delta);
    }
    _mm256_zeroupper();

    for (uint16_t i = from; i < to; ++i) {
        selected_idx[selected_size] = i;
        selected_size += selection[i];
    }
    return selected_size;
}
} // namespace SIMD