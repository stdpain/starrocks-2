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

#include "exec/spill/spiller.h"

#include <butil/iobuf.h>
#include <fmt/core.h>
#include <glog/logging.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

#include "column/chunk.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/sort_exec_exprs.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/spilled_stream.h"
#include "exec/spill/spiller.hpp"
#include "exec/spill/spiller_path_provider.h"
#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"

namespace starrocks {
SpillProcessMetrics::SpillProcessMetrics(RuntimeProfile* profile) {
    spill_timer = ADD_TIMER(profile, "SpillTime");
    spill_rows = ADD_COUNTER(profile, "SpilledRows", TUnit::UNIT);
    flush_timer = ADD_TIMER(profile, "SpillFlushTimer");
    write_io_timer = ADD_TIMER(profile, "SpillWriteIOTimer");
    restore_rows = ADD_COUNTER(profile, "SpillRestoreRows", TUnit::UNIT);
    restore_timer = ADD_TIMER(profile, "SpillRestoreTimer");
    shuffle_timer = ADD_TIMER(profile, "SpillShuffleTimer");
    split_partition_timer = ADD_TIMER(profile, "SplitPartitionTimer");
}
// Not thread safe
class ColumnSpillFormater : public SpillFormater {
public:
    ColumnSpillFormater(ChunkBuilder chunk_builder) : _chunk_builder(std::move(chunk_builder)) {}
    Status spill_as_fmt(SpillFormatContext& context, std::unique_ptr<WritableFile>& writable,
                        const ChunkPtr& chunk) const noexcept override;
    StatusOr<ChunkUniquePtr> restore_from_fmt(SpillFormatContext& context,
                                              std::unique_ptr<RawInputStreamWrapper>& readable) const override;
    Status flush(std::unique_ptr<WritableFile>& writable) const override;

private:
    size_t _spill_size(const ChunkPtr& chunk) const;
    ChunkBuilder _chunk_builder;
};

size_t ColumnSpillFormater::_spill_size(const ChunkPtr& chunk) const {
    size_t serialize_sz = 0;
    for (const auto& column : chunk->columns()) {
        serialize_sz += serde::ColumnArraySerde::max_serialized_size(*column);
    }
    return serialize_sz + sizeof(serialize_sz);
}

Status ColumnSpillFormater::spill_as_fmt(SpillFormatContext& context, std::unique_ptr<WritableFile>& writable,
                                         const ChunkPtr& chunk) const noexcept {
    size_t serialize_sz = _spill_size(chunk);
    context.io_buffer.resize(serialize_sz);
    DCHECK_GT(serialize_sz, 4);

    auto* buff = reinterpret_cast<uint8_t*>(context.io_buffer.data());
    UNALIGNED_STORE64(buff, serialize_sz);
    buff += sizeof(serialize_sz);

    chunk->check_or_die();
    auto chunk_serialize = _chunk_builder();
    const auto& slot_id_to_index = chunk_serialize->get_slot_id_to_index_map();
    std::map<int, int> index_to_slot_id;
    for (auto [slot_id, index] : slot_id_to_index) {
        index_to_slot_id[index] = slot_id;
    }
    for (size_t i = 0; i < chunk->num_columns(); ++i) {
        auto column = chunk->get_column_by_slot_id(index_to_slot_id[i]);
        buff = serde::ColumnArraySerde::serialize(*column, buff);
    }

    RETURN_IF_ERROR(writable->append(context.io_buffer));
    return Status::OK();
}

StatusOr<ChunkUniquePtr> ColumnSpillFormater::restore_from_fmt(SpillFormatContext& context,
                                                               std::unique_ptr<RawInputStreamWrapper>& readable) const {
    size_t serialize_sz;
    RETURN_IF_ERROR(readable->read_fully(&serialize_sz, sizeof(serialize_sz)));
    DCHECK_GT(serialize_sz, sizeof(serialize_sz));
    context.io_buffer.resize(serialize_sz);
    auto buff = reinterpret_cast<uint8_t*>(context.io_buffer.data());
    RETURN_IF_ERROR(readable->read_fully(buff, serialize_sz - sizeof(serialize_sz)));

    auto chunk = _chunk_builder();
    const uint8_t* read_cursor = buff;
    for (const auto& column : chunk->columns()) {
        read_cursor = serde::ColumnArraySerde::deserialize(read_cursor, column.get());
    }
    chunk->check_or_die();
    return chunk;
}

Status ColumnSpillFormater::flush(std::unique_ptr<WritableFile>& writable) const {
    if (!config::experimental_spill_skip_sync) {
        RETURN_IF_ERROR(writable->flush(WritableFile::FLUSH_ASYNC));
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<SpillFormater>> SpillFormater::create(SpillFormaterType type, ChunkBuilder chunk_builder) {
    if (type == SpillFormaterType::SPILL_BY_COLUMN) {
        return std::make_unique<ColumnSpillFormater>(std::move(chunk_builder));
    } else {
        return Status::InternalError(fmt::format("unsupported spill type:{}", type));
    }
}

Status Spiller::prepare(RuntimeState* state) {
    // prepare
    ASSIGN_OR_RETURN(_spill_fmt, SpillFormater::create(_opts.spill_type, _opts.chunk_builder));

    if (_opts.init_partition_nums > 0) {
        _writer = std::make_unique<PartitionedSpillerWriter>(this, state);
    } else {
        _writer = std::make_unique<RawSpillerWriter>(this, state);
    }

    RETURN_IF_ERROR(_writer->prepare(state));

    _reader = std::make_unique<SpillerReader>(this);

    if (!_opts.is_unordered) {
        DCHECK(_opts.init_partition_nums == -1);
    }

    return Status::OK();
}

Status Spiller::set_partition(const std::vector<const SpillPartitionInfo*>& parititons) {
    DCHECK_GT(_opts.init_partition_nums, 0);
    RETURN_IF_ERROR(down_cast<PartitionedSpillerWriter*>(_writer.get())->reset_partition(parititons));
    return Status::OK();
}

Status Spiller::open(RuntimeState* state) {
    std::lock_guard guard(_mutex);
    if (_has_opened) {
        return Status::OK();
    }

    // init path provider
    ASSIGN_OR_RETURN(_path_provider, _opts.path_provider_factory());
    RETURN_IF_ERROR(_path_provider->open(state));
    _has_opened = true;

    return Status::OK();
}

void Spiller::update_spilled_task_status(Status&& st) {
    std::lock_guard guard(_mutex);
    if (_spilled_task_status.ok() && !st.ok()) {
        _spilled_task_status = std::move(st);
    }
}

std::vector<std::unique_ptr<SpillerReader>> Spiller::get_partition_spill_reader(
        const std::vector<const SpillPartitionInfo*>& partitions) {
    std::vector<std::unique_ptr<SpillerReader>> res;

    for (auto partition : partitions) {
        res.emplace_back(std::make_unique<SpillerReader>(this));
        std::shared_ptr<SpilledInputStream> stream;
        std::queue<SpillRestoreTaskPtr> tasks;
        // TODO check return status
        CHECK(_writer->acquire_stream(partition, &stream, &tasks).ok());
        res.back()->acquire_tasks(std::move(stream), std::move(tasks));
    }

    return res;
}

Status Spiller::_acquire_input_stream(RuntimeState* state) {
    std::shared_ptr<SpilledInputStream> input_stream;
    std::queue<SpillRestoreTaskPtr> restore_tasks;

    RETURN_IF_ERROR(_writer->acquire_stream(&input_stream, &restore_tasks));
    RETURN_IF_ERROR(_reader->acquire_tasks(std::move(input_stream), std::move(restore_tasks)));

    return Status::OK();
}
} // namespace starrocks