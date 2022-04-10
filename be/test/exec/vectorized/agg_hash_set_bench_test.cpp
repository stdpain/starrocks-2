
#include <benchmark/benchmark.h>

#include <cstdlib>
#include <limits>
#include <random>

#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized/aggregate/agg_hash_map.h"
#include "exec/vectorized/aggregate/agg_hash_set.h"

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

template <class HashSetKey, class Gen, int64_t sz>
void do_bench(benchmark::State& state) {
    // void do_bench(size_t sz0) {
    std::default_random_engine d(0);
    srand(0);
    // prepare
    auto key = HashSetKey(bench_chunk_size);
    key.has_null_column = true;
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
            // auto nullable_column = NullableColumn::create(key2column, NullColumn::create(bench_chunk_size));
            // random_set_null(nullable_column.get(), bench_chunk_size);
            // Columns columns = {key1column, nullable_column};
            Columns columns = {key1column, key2column};
            state.ResumeTiming();
            // compute group by columns
            key.build_set(bench_chunk_size, columns, &pool);
            state.PauseTiming();
            for (int i = 0; i < bench_chunk_size; ++i) {
            }
        }
    }
}

using SerializedKeyAggHashSetFixedOrigin = AggHashSetOfSerializedKeyFixedSize<FixedSize8SliceAggHashSet<PhmapSeed1>>;
using SerializedKeyAggHashSetFixedDiff = AggHashSetOfSerializedKeyFixedSizeDiff<FixedSize8SliceAggHashSet<PhmapSeed1>>;

#define DECLARE_BENCH(sz)                                                                   \
    static void BM_agg_hash_set_##sz(benchmark::State& state) {                             \
        do_bench<SerializedKeyAggHashSetFixedDiff, RandomGenerator<int32_t, sz>, 1>(state); \
    }

#define DECLARE_BENCH_ORIGIN(sz)                                                              \
    static void BM_agg_hash_origin_set_##sz(benchmark::State& state) {                        \
        do_bench<SerializedKeyAggHashSetFixedOrigin, RandomGenerator<int32_t, sz>, 1>(state); \
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
    BENCHMARK(BM_agg_hash_set_##sz); \
    BENCHMARK(BM_agg_hash_origin_set_##sz);

DECLARE_ALL(8)
DECLARE_ALL(64)
DECLARE_ALL(1024)
DECLARE_ALL(4096)
DECLARE_ALL(8192)
DECLARE_ALL(16335)
DECLARE_ALL(65535)
DECLARE_ALL(655350)

} // namespace starrocks::vectorized

BENCHMARK_MAIN();

// int main(int argc, char*argv[]) {
//     using namespace starrocks::vectorized;
//     int loog_size = atoi(argv[1]);
//     do_bench<AggHashMapWithSerializedKeyOriginFixedSize<FixedSize8SliceAggHashMap<PhmapSeed1>>,
//                  FixedSize8SliceAggHashMap<PhmapSeed1>, RandomGenerator<int32_t, 1>, 1>(loog_size);
//     return 0;
// }
