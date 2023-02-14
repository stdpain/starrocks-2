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
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/spill/common.h"
#include "exec/spill/executor.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/spilled_stream.h"
#include "exec/spill/spiller_factory.h"
#include "exec/spill/spiller_path_provider.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/blocking_queue.hpp"

namespace starrocks {
enum class SpillFormaterType { NONE, SPILL_BY_COLUMN };

using ChunkBuilder = std::function<ChunkUniquePtr()>;

struct SpilledOptions {
    SpilledOptions() : SpilledOptions(-1) {}

    SpilledOptions(int init_partition_nums_)
            : init_partition_nums(init_partition_nums_), is_unordered(true), sort_exprs(nullptr), sort_desc(nullptr) {}

    SpilledOptions(SortExecExprs* sort_exprs_, const SortDescs* sort_desc_)
            : init_partition_nums(-1), is_unordered(false), sort_exprs(sort_exprs_), sort_desc(sort_desc_) {}

    const int init_partition_nums;
    const std::vector<ExprContext*> partiton_exprs;

    bool is_unordered;
    const SortExecExprs* sort_exprs;
    const SortDescs* sort_desc;

    size_t mem_table_pool_size{};
    size_t spill_file_size{};
    SpillFormaterType spill_type{};
    SpillPathProviderFactory path_provider_factory;
    ChunkBuilder chunk_builder;

    const size_t min_spilled_size = 1 * 1024 * 1024;
};

struct SpillFormatContext {
    std::string io_buffer;
};

enum class SpillStrategy {
    NO_SPILL,
    SPILL_ALL,
};

// thread safe
class SpillFormater {
public:
    virtual ~SpillFormater() = default;
    virtual Status spill_as_fmt(SpillFormatContext& context, std::unique_ptr<WritableFile>& writable,
                                const ChunkPtr& chunk) const noexcept = 0;
    virtual StatusOr<ChunkUniquePtr> restore_from_fmt(SpillFormatContext& context,
                                                      std::unique_ptr<RawInputStreamWrapper>& readable) const = 0;
    virtual Status flush(std::unique_ptr<WritableFile>& writable) const = 0;
    static StatusOr<std::unique_ptr<SpillFormater>> create(SpillFormaterType type, ChunkBuilder chunk_builder);
};

class Spiller;
using FlushAllCallBack = std::function<Status()>;

class SpillerWriter {
public:
    SpillerWriter(Spiller* spiller, RuntimeState* state) : _spiller(spiller), _runtime_state(state) {}
    virtual ~SpillerWriter() = default;
    virtual bool is_full() = 0;
    virtual bool has_pending_data() = 0;

    virtual Status spill(RuntimeState* state, const ChunkPtr& chunk, IOTaskExecutor& executor,
                         const MemTrackerGuard& guard) = 0;
    virtual Status flush(RuntimeState* state, IOTaskExecutor& executor, const MemTrackerGuard& guard) = 0;

    virtual Status prepare(RuntimeState* state) = 0;

    virtual Status set_flush_all_call_back(FlushAllCallBack callback, RuntimeState* state, IOTaskExecutor& executor,
                                           const MemTrackerGuard& guard) = 0;
    // acquire next
    virtual Status acquire_next_stream(std::shared_ptr<SpilledInputStream>* stream,
                                       std::queue<SpillRestoreTaskPtr>* tasks) = 0;

    template <class T>
    T as() {
        return down_cast<T>(this);
    }

protected:
    Status _decrease_running_flush_tasks();

    Spiller* _spiller;
    RuntimeState* _runtime_state;
    std::atomic_uint64_t _running_flush_tasks{};

    FlushAllCallBack _flush_all_callback;
};

class SpillerReader {
public:
    SpillerReader(Spiller* spiller) : _spiller(spiller) {}

    virtual ~SpillerReader() = default;

    Status acquire_tasks(std::shared_ptr<SpilledInputStream> stream, std::queue<SpillRestoreTaskPtr> task) {
        std::lock_guard guard(_mutex);
        _current_stream = std::move(stream);
        _restore_tasks = std::move(task);
        _total_restore_tasks += _restore_tasks.size();
        return Status::OK();
    }

    template <class TaskExecutor, class MemGuard>
    StatusOr<ChunkPtr> t_restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status t_trigger_restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    StatusOr<ChunkPtr> restore(RuntimeState* state, IOTaskExecutor& executor, const MemTrackerGuard& guard);

    Status trigger_restore(RuntimeState* state, IOTaskExecutor& executor, const MemTrackerGuard& guard);

    bool has_output_data() { return _current_stream && _current_stream->is_ready(); }

protected:
    Spiller* _spiller;
    std::atomic_uint64_t _total_restore_tasks{};
    std::atomic_uint64_t _running_restore_tasks{};
    std::atomic_uint64_t _finished_restore_tasks{};

    std::mutex _mutex;
    std::shared_ptr<SpilledInputStream> _current_stream;
    std::queue<SpillRestoreTaskPtr> _restore_tasks;
    SpillFormatContext _spill_read_ctx;
};

class Spiller {
public:
    Spiller(SpilledOptions opts, const std::shared_ptr<SpillerFactory>& factory)
            : _opts(std::move(opts)), _parent(factory) {}
    virtual ~Spiller() { TRACE_SPILL_LOG << "SPILLER:" << this << " call destructor"; }

    // some init work
    Status prepare(RuntimeState* state);

    // no thread-safe
    // TaskExecutor: Executor for runing io tasks
    // MemGuard: interface for record/update memory usage in io tasks
    template <class TaskExecutor, class MemGuard>
    Status spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor, MemGuard&& guard);

    // restore chunk
    template <class TaskExecutor, class MemGuard>
    StatusOr<ChunkPtr> restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status trigger_restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    bool is_full() { return _writer->is_full(); }

    bool has_pending_data() { return _writer->has_pending_data(); }

    // all data has been sent
    // prepared for as read
    template <class TaskExecutor, class MemGuard>
    Status flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    Status set_flush_all_call_back(const FlushAllCallBack& callback, RuntimeState* state, IOTaskExecutor& executor,
                                   const MemTrackerGuard& guard) {
        auto flush_call_back = [this, callback, state, &executor, guard]() {
            RETURN_IF_ERROR(callback());
            if (spilled()) {
                RETURN_IF_ERROR(_acquire_input_stream(state));
                RETURN_IF_ERROR(trigger_restore(state, executor, guard));
            }
            return Status::OK();
        };
        return _writer->set_flush_all_call_back(flush_call_back, state, executor, guard);
    }

    bool has_output_data() { return _reader->has_output_data(); }

    size_t spilled_append_rows() { return _spilled_append_rows; }

    size_t restore_read_rows() { return _restore_read_rows; }

    bool spilled() { return spilled_append_rows() > 0; }

    bool restore_finished() { return _running_restore_tasks == 0; }

    const auto& options() { return _opts; }

    const auto& path_provider() { return _path_provider; }

    const auto& spill_fmt() { return _spill_fmt; }

    void update_spilled_task_status(Status&& st);

    Status task_status() { return _spilled_task_status; }

    // open stage
    // should be called in executor threads (not pipeline threads)
    Status open(RuntimeState* state);

private:
    Status _acquire_input_stream(RuntimeState* state);

    Status _decrease_running_flush_tasks();

private:
    SpilledOptions _opts;
    std::weak_ptr<SpillerFactory> _parent;

    std::unique_ptr<SpillerWriter> _writer;
    std::unique_ptr<SpillerReader> _reader;

    bool _has_opened = false;
    std::shared_ptr<SpillerPathProvider> _path_provider;

    std::mutex _mutex;

    std::unique_ptr<SpillFormater> _spill_fmt;
    Status _spilled_task_status;

    // stats
    std::atomic_uint64_t _total_restore_tasks{};
    std::atomic_uint64_t _running_restore_tasks{};
    std::atomic_uint64_t _finished_restore_tasks{};

    std::atomic_uint64_t _running_flush_tasks{};

    size_t _spilled_append_rows{};
    size_t _restore_read_rows{};
};

class RawSpillerWriter final : public SpillerWriter {
private:
    const auto& options() { return _spiller->options(); }

    MemTablePtr _acquire_mem_table_from_pool() {
        std::lock_guard guard(_mutex);
        if (_mem_table_pool.empty()) {
            return nullptr;
        }
        auto res = std::move(_mem_table_pool.front());
        _mem_table_pool.pop();
        return res;
    }

public:
    RawSpillerWriter(Spiller* spiller, RuntimeState* state) : SpillerWriter(spiller, state) {}
    ~RawSpillerWriter() override = default;

    bool is_full() override {
        std::lock_guard guard(_mutex);
        return _mem_table_pool.empty() && _mem_table == nullptr;
    }

    bool has_pending_data() override {
        std::lock_guard guard(_mutex);
        return _mem_table_pool.size() != options().mem_table_pool_size;
    }

    Status spill(RuntimeState* state, const ChunkPtr& chunk, IOTaskExecutor& executor,
                 const MemTrackerGuard& guard) override;

    Status flush(RuntimeState* state, IOTaskExecutor& executor, const MemTrackerGuard& guard) override;

    Status set_flush_all_call_back(FlushAllCallBack callback, RuntimeState* state, IOTaskExecutor& executor,
                                   const MemTrackerGuard& guard) override {
        _running_flush_tasks++;
        _flush_all_callback = std::move(callback);
        return _decrease_running_flush_tasks();
    }

    template <class TaskExecutor, class MemGuard>
    Status t_spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status t_flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    Status prepare(RuntimeState* state) override;

    Status flush_task(RuntimeState* state, const MemTablePtr& mem_table);

    void acquire_mem_table() {
        if (_mem_table == nullptr) {
            _mem_table = _acquire_mem_table_from_pool();
        }
    }

    const auto& mem_table() const { return _mem_table; }
    auto& writable() { return _writable; }

    Status acquire_next_stream(std::shared_ptr<SpilledInputStream>* stream,
                               std::queue<SpillRestoreTaskPtr>* tasks) override;

private:
    SpilledFileGroup _file_group;
    MemTablePtr _mem_table;
    std::unique_ptr<WritableFile> _writable;
    std::queue<MemTablePtr> _mem_table_pool;
    std::mutex _mutex;
    SpillFormatContext _spill_read_ctx;
};

using SpillHashColumn = UInt32Column;

} // namespace starrocks