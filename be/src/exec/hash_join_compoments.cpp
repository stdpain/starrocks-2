#include "column/vectorized_fwd.h"
#include "exec/hash_join_components.h"
#include "exec/hash_joiner.h"

namespace starrocks {

void HashJoinProber::push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk) {
    DCHECK(!_probe_chunk);
    _probe_chunk = std::move(chunk);
    _current_probe_has_remain = true;
    _hash_joiner.prepare_probe_key_columns(&_key_columns, _probe_chunk);
}

StatusOr<ChunkPtr> HashJoinProber::probe_chunk(RuntimeState* state, JoinHashTable* hash_table) {
    auto chunk = std::make_shared<Chunk>();
    TRY_CATCH_ALLOC_SCOPE_START()
    DCHECK(_current_probe_has_remain && _probe_chunk);
    RETURN_IF_ERROR(hash_table->probe(state, _key_columns, &_probe_chunk, &chunk, &_current_probe_has_remain));
    if (!_current_probe_has_remain) {
        _probe_chunk = nullptr;
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    return chunk;
}

StatusOr<ChunkPtr> HashJoinProber::probe_remain(RuntimeState* state, JoinHashTable* hash_table, bool* has_remain) {
    auto chunk = std::make_shared<Chunk>();
    TRY_CATCH_ALLOC_SCOPE_START()
    RETURN_IF_ERROR(hash_table->probe_remain(state, &chunk, &_current_probe_has_remain));
    *has_remain = _current_probe_has_remain;
    TRY_CATCH_ALLOC_SCOPE_END()
    return chunk;
}

void HashJoinProber::reset() {
    _probe_chunk->reset();
    _current_probe_has_remain = false;
}

void HashJoinBuilder::create(const HashTableParam& param) {
    _ht.create(param);
}

void HashJoinBuilder::close() {
    _ht.close();
}

void HashJoinBuilder::reset_probe(RuntimeState* state) {
    _key_columns.clear();
    _ht.reset_probe_state(state);
}

Status HashJoinBuilder::append_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (UNLIKELY(_ht.get_row_count() + chunk->num_rows() >= max_hash_table_element_size)) {
        return Status::NotSupported(strings::Substitute("row count of right table in hash join > $0", UINT32_MAX));
    }

    _hash_joiner.prepare_build_key_columns(&_key_columns, chunk);
    // copy chunk of right table
    SCOPED_TIMER(_hash_joiner.build_metrics().copy_right_table_chunk_timer);
    TRY_CATCH_BAD_ALLOC(_ht.append_chunk(state, chunk, _key_columns));
    return Status::OK();
}

Status HashJoinBuilder::build(RuntimeState* state) {
    SCOPED_TIMER(_hash_joiner.build_metrics().build_ht_timer);
    TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_ht.build(state)));
    _ready = true;
    return Status::OK();
}

} // namespace starrocks