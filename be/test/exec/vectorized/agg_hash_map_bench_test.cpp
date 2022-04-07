
#include <benchmark/benchmark.h>

#include <cstdlib>
#include <limits>
#include <random>

#include "exec/vectorized/aggregate/agg_hash_map.h"

namespace starrocks::vectorized {

static constexpr int32_t bench_chunk_size = 4096;
template <class Gen, class TColumn>
void fill_column(TColumn& column, size_t sz) {
    for (size_t i = 0; i < sz; ++i) {
        column.append(Gen::next_value());
    }
}

template <class KeyType, class HashTable, class Gen, int64_t sz>
void do_bench(benchmark::State& state) {
    std::default_random_engine d(0);
    srand(0);
    // prepare
    auto key = KeyType(bench_chunk_size);
    Buffer<AggDataPtr> agg_states(bench_chunk_size);
    MemPool pool;
    for (auto _ : state) {
        for (int64_t i = 0; i < sz; i += bench_chunk_size) {
            state.PauseTiming();
            auto keycolumn = KeyType::ColumnType::create(bench_chunk_size);
            keycolumn->reset_column();
            fill_column<Gen>(*keycolumn, bench_chunk_size);
            Columns columns = {keycolumn};
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

#define DECLARE_BENCH(sz)                                                                                        \
    static void BM_agg_hash_map_##sz(benchmark::State& state) {                                                  \
        do_bench<AggHashMapWithOneNumberKey<TYPE_INT, Int32AggHashMap<PhmapSeed1>>, Int32AggHashMap<PhmapSeed1>, \
                 RandomGenerator<int32_t, sz>, 1>(state);                                                        \
    }

#define DECLARE_PREFETCH_BENCH(sz)                                                          \
    static void BM_agg_hash_map_prefetch_##sz(benchmark::State& state) {                    \
        do_bench<AggHashMapWithOneNumberKeyPREFETCH<TYPE_INT, Int32AggHashMap<PhmapSeed1>>, \
                 Int32AggHashMap<PhmapSeed1>, RandomGenerator<int32_t, sz>, 1>(state);      \
    }

#define DECLARE_ORIGIN_BENCH(sz)                                                                                       \
    static void BM_agg_hash_map_origin_##sz(benchmark::State& state) {                                                 \
        do_bench<AggHashMapWithOneNumberKeyOrigin<TYPE_INT, Int32AggHashMap<PhmapSeed1>>, Int32AggHashMap<PhmapSeed1>, \
                 RandomGenerator<int32_t, sz>, 1>(state);                                                              \
    }

#define DECLARE_ALL(sz)                       \
    DECLARE_BENCH(sz)                         \
    DECLARE_PREFETCH_BENCH(sz)                \
    DECLARE_ORIGIN_BENCH(sz)                  \
    BENCHMARK(BM_agg_hash_map_##sz);          \
    BENCHMARK(BM_agg_hash_map_prefetch_##sz); \
    BENCHMARK(BM_agg_hash_map_origin_##sz);

DECLARE_ALL(8)
DECLARE_ALL(64)
DECLARE_ALL(512)
DECLARE_ALL(1024)
DECLARE_ALL(2048)
DECLARE_ALL(4096)
DECLARE_ALL(65535)
DECLARE_ALL(655350)
DECLARE_ALL(6553500)

} // namespace starrocks::vectorized

BENCHMARK_MAIN();
