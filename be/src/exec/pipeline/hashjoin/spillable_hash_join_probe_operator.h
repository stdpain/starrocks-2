// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/hash_join_components.h"
#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"
#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "exec/spill/partition.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

struct NoBlockCountDownLatch {
    void reset(int32_t total) { _count_down = total; }

    void count_down() {
        _count_down--;
        DCHECK_GE(_count_down, 0);
    }

    bool ready() const { return _count_down == 0; }

private:
    std::atomic_int32_t _count_down{};
};

struct IncreaseGuard {
    IncreaseGuard() = default;
    // IncreaseGuard(std::atomic_int32_t* resource_) : resource(resource_) {}
    IncreaseGuard(std::atomic_int32_t* resource_, std::atomic_int32_t* desc_) : resource(resource_), desc(desc_) {}
    ~IncreaseGuard() { reset(); }

    IncreaseGuard(IncreaseGuard&& other) noexcept : resource(other.resource), desc(other.desc) {
        other.resource = nullptr;
        other.desc = nullptr;
    }
    IncreaseGuard& operator=(IncreaseGuard&& other) noexcept {
        std::swap(other.resource, resource);
        std::swap(other.desc, desc);
        return *this;
    }

    void check_empty() {
        CHECK_EQ(resource, nullptr);
        CHECK_EQ(desc, nullptr);
    }

    void reset() {
        if (resource != nullptr) {
            (*resource)++;
            (*desc)--;
            resource = nullptr;
            desc = nullptr;
        }
    }

    DISALLOW_COPY(IncreaseGuard);

private:
    std::atomic_int32_t* resource = nullptr;
    std::atomic_int32_t* desc = nullptr;
};

struct SpillableHashJoinProbeMetrics {
    RuntimeProfile::Counter* hash_partitions = nullptr;
    RuntimeProfile::Counter* probe_shuffle_timer = nullptr;
    RuntimeProfile::HighWaterMarkCounter* prober_peak_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* build_partition_peak_memory_usage = nullptr;
};

class SpillableHashJoinProbeOperatorFactory;
class SpillableHashJoinProbeOperator final : public HashJoinProbeOperator {
public:
    template <class... Args>
    SpillableHashJoinProbeOperator(Args&&... args) : HashJoinProbeOperator(std::forward<Args>(args)...) {}
    ~SpillableHashJoinProbeOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool need_input() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    Status set_finished(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    void set_probe_spiller(std::shared_ptr<spill::Spiller> spiller) { _probe_spiller = std::move(spiller); }

private:
    using SpillerReaderPtr = std::shared_ptr<spill::SpillerReader>;

    bool spilled() const { return _join_builder->spiller()->spilled(); }

    SpillableHashJoinProbeOperator* as_mutable() const { return const_cast<SpillableHashJoinProbeOperator*>(this); }

    // acquire next build-side partitions
    void _acquire_next_partitions();
    void _acquire_private_partitions();
    // acquired shared partition in broadcast join to resue the hash table
    void _acquire_shared_partition();
    // acquire the partition in memory
    size_t _acquire_partition_from_all_partitions();

    // indicates that all partitions to be processed are complete
    bool _all_partition_finished() const;

    Status _load_processing_partition_hash_table(RuntimeState* state);
    Status _load_partition_build_side(RuntimeState* state, const SpillerReaderPtr& reader, HashJoinBuilder* builder);
    Status _load_shared_partition(RuntimeState* state, const SpillerReaderPtr& reader, HashJoinBuilder* builder,
                                  int32_t partition_id);

    void _update_status(Status&& status) const;

    Status _status() const;

    Status _push_probe_chunk(RuntimeState* state, const ChunkPtr& chunk);

    Status _restore_probe_partition(RuntimeState* state);

    // some DCHECK for hash table/partition num_rows
    void _check_partitions();

    SpillableHashJoinProbeOperatorFactory* factory() const {
        return down_cast<SpillableHashJoinProbeOperatorFactory*>(_factory);
    }

private:
    SpillableHashJoinProbeMetrics metrics;

    std::vector<const SpillPartitionInfo*> _build_partitions;
    std::unordered_map<int32_t, const SpillPartitionInfo*> _pid_to_build_partition;
    std::vector<const SpillPartitionInfo*> _processing_partitions;
    std::unordered_set<int32_t> _processed_partitions;
    int32_t _prev_large_partition_id = -1;

    std::vector<SpillerReaderPtr> _current_reader;
    std::vector<bool> _probe_read_eofs;
    std::vector<bool> _probe_post_eofs;
    bool _has_probe_remain = true;
    std::shared_ptr<spill::Spiller> _probe_spiller;

    ObjectPool _component_pool;
    std::vector<HashJoinProber*> _probers;
    std::vector<HashJoinBuilder*> _builders;
    std::unordered_map<int32_t, int32_t> _pid_to_process_id;

    bool _is_finished = false;
    bool _is_finishing = false;

    NoBlockCountDownLatch _latch;
    IncreaseGuard _large_partition_guard;
    mutable std::mutex _mutex;
    mutable Status _operator_status;

    std::shared_ptr<spill::IOTaskExecutor> _executor;
    bool _need_post_probe = false;
};

// now large partitions only used in broadcast join
struct LargePartitionManager {
    std::once_flag init_flag;
    // protect total_probers
    std::mutex counter_mutex;
    std::vector<int32_t> large_partitions;
    std::unordered_set<int32_t> large_partitions_set;
    //
    size_t current_partition_sequence = 0;
    //
    std::atomic_int32_t refs{};
    std::atomic_int32_t released_refs{};

    int32_t total_probers{};

    // partition
    std::mutex hash_table_mutex;
    int32_t loaded_partition_version = -1;

    // critical area with decrease prober
    int32_t get_next_partition() {
        std::lock_guard guard(counter_mutex);
        CHECK_LE(refs, total_probers);
        if (refs == total_probers) {
            CHECK_EQ(released_refs, 0);
            current_partition_sequence++;
            refs = 0;
        }
        released_refs++;
        return large_partitions[current_partition_sequence];
    }

    bool large_partition_has_ref() const { return refs != total_probers; }

    bool is_large_partition(int32_t partition_id) const { return large_partitions_set.count(partition_id); }

    // prepare sequential execution
    void increase_prober() { total_probers++; }

    void decrease_prober() {
        CHECK(total_probers > 0) << "init prober should be called before decrease_prober()";
        std::lock_guard guard(counter_mutex);
        total_probers--;
    }
};

class SpillableHashJoinProbeOperatorFactory : public HashJoinProbeOperatorFactory {
public:
    template <class... Args>
    SpillableHashJoinProbeOperatorFactory(Args&&... args) : HashJoinProbeOperatorFactory(std::forward<Args>(args)...){};

    ~SpillableHashJoinProbeOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    bool is_shared_hash_table() const {
        return _hash_joiner_factory->hash_join_param()._distribution_mode == TJoinDistributionMode::BROADCAST;
    }
    size_t dop() const { return _hash_joiner_factory->prober_dop(); }

    template <class LargePartitionLoader>
    void init_large_partition_once(LargePartitionLoader&& loader) {
        std::call_once(_large_partition_handler.init_flag, [this, &loader]() { loader(_large_partition_handler); });
    }
    const std::vector<int32_t>& large_partitions() const { return _large_partition_handler.large_partitions; }
    IncreaseGuard next_partition(int32_t* next_partition) {
        *next_partition = _large_partition_handler.get_next_partition();
        return {&_large_partition_handler.refs, &_large_partition_handler.released_refs};
    }
    LargePartitionManager& large_partition_handler() { return _large_partition_handler; }

private:
    LargePartitionManager _large_partition_handler;

    std::shared_ptr<spill::SpilledOptions> _spill_options;
    std::shared_ptr<spill::SpillerFactory> _spill_factory = std::make_shared<spill::SpillerFactory>();
};

} // namespace starrocks::pipeline