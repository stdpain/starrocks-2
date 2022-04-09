
#include <benchmark/benchmark.h>

#include <cstdlib>
#include <limits>
#include <random>

#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized/aggregate/agg_hash_map.h"

namespace starrocks::vectorized {

static constexpr int32_t bench_chunk_size = 4096;
template <class Gen, class TColumn>
void fill_column(TColumn& column, size_t sz) {
    for (size_t i = 0; i < sz; ++i) {
        column.append(Gen::next_value());
    }
}

void random_set_null(NullableColumn* column, size_t sz) {
    for (size_t i = 0; i < sz; ++i) {
        if (rand() % 2 == 0) {
            column->set_null(i);
        }
    }
}

template <class KeyType, class HashTable, class Gen, int64_t sz>
void do_bench(benchmark::State& state) {
    // void do_bench(size_t sz0) {
    std::default_random_engine d(0);
    srand(0);
    // prepare
    auto key = KeyType(bench_chunk_size);
    key.has_null_column = true;
    key.fixed_byte_size = 8;
    Buffer<AggDataPtr> agg_states(bench_chunk_size);
    MemPool pool;
    for (auto _ : state) {
        for (int64_t i = 0; i < sz; i += bench_chunk_size) {
            state.PauseTiming();
            auto key1column = Int32Column::create(bench_chunk_size);
            auto key2column = Int32Column::create(bench_chunk_size);
            key1column->reset_column();
            key2column->reset_column();
            fill_column<Gen>(*key1column, bench_chunk_size);
            fill_column<Gen>(*key2column, bench_chunk_size);
            auto nullable_column = NullableColumn::create(key2column, NullColumn::create(bench_chunk_size));
            random_set_null(nullable_column.get(), bench_chunk_size);
            Columns columns = {key1column, nullable_column};
            state.ResumeTiming();
            // compute group by columns
            key.compute_agg_states(
                    bench_chunk_size, columns, &pool,
                    [&]() -> AggDataPtr {
                        AggDataPtr agg_state = pool.allocate_aligned(16, 16);
                        return agg_state;
                    },
                    &agg_states);
            state.PauseTiming();
            for (int i = 0; i < bench_chunk_size; ++i) {
                benchmark::DoNotOptimize(*agg_states[i]);
            }
        }
    }
}

#define DECLARE_BENCH(sz)                                                                        \
    static void BM_agg_hash_map_##sz(benchmark::State& state) {                                  \
        do_bench<AggHashMapWithSerializedKeyFixedSize<FixedSize8SliceAggHashMap<PhmapSeed1>>,    \
                 FixedSize8SliceAggHashMap<PhmapSeed1>, RandomGenerator<int32_t, sz>, 1>(state); \
    }

#define DECLARE_BENCH_ORIGIN(sz)                                                                    \
    static void BM_agg_hash_origin_map_##sz(benchmark::State& state) {                              \
        do_bench<AggHashMapWithSerializedKeyOriginFixedSize<FixedSize8SliceAggHashMap<PhmapSeed1>>, \
                 FixedSize8SliceAggHashMap<PhmapSeed1>, RandomGenerator<int32_t, sz>, 1>(state);    \
    }

// #define DECLARE_ALL(sz)              \
//     DECLARE_BENCH_ORIGIN(sz)         \
//     BENCHMARK(BM_agg_hash_origin_map_##sz);

// #define DECLARE_ALL(sz)              \
//     DECLARE_BENCH(sz)         \
//     BENCHMARK(BM_agg_hash_map_##sz);

#define DECLARE_ALL(sz)              \
    DECLARE_BENCH(sz)                \
    DECLARE_BENCH_ORIGIN(sz)         \
    BENCHMARK(BM_agg_hash_map_##sz); \
    BENCHMARK(BM_agg_hash_origin_map_##sz);

DECLARE_ALL(8)

} // namespace starrocks::vectorized

BENCHMARK_MAIN();

// int main(int argc, char*argv[]) {
//     using namespace starrocks::vectorized;
//     int loog_size = atoi(argv[1]);
//     do_bench<AggHashMapWithSerializedKeyOriginFixedSize<FixedSize8SliceAggHashMap<PhmapSeed1>>,
//                  FixedSize8SliceAggHashMap<PhmapSeed1>, RandomGenerator<int32_t, 1>, 1>(loog_size);
//     return 0;
// }
