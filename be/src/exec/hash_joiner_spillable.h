#include "exec/hash_joiner.h"

namespace starrocks {

// TODO:
class HashJoinerSpillable {
public:
    explicit HashJoinerSpillable(const HashJoinerParam& param);
    ~HashJoinerSpillable();

private:
    const THashJoinNode& _hash_join_node;
    ObjectPool* _pool;

    RuntimeState* _runtime_state = nullptr;
    TJoinOp::type _join_type = TJoinOp::INNER_JOIN;

    ChunkPtr _probe_input_chunk;
    const std::vector<bool>& _is_null_safes;

    const std::vector<ExprContext*>& _build_expr_ctxs;
    // Equal conjuncts in Join On.
    const std::vector<ExprContext*>& _probe_expr_ctxs;
    // Conjuncts in Join On except equal conjuncts.
    const std::vector<ExprContext*>& _other_join_conjunct_ctxs;
    // Conjuncts in Join followed by a filter predicate, usually in Where and Having.
    const std::vector<ExprContext*>& _conjunct_ctxs;

    const RowDescriptor& _build_row_descriptor;
    const RowDescriptor& _probe_row_descriptor;
    const RowDescriptor& _row_descriptor;
    const TPlanNodeType::type _build_node_type;
    const TPlanNodeType::type _probe_node_type;
    const bool _build_conjunct_ctxs_is_empty;
    const std::set<SlotId>& _output_slots;

    bool _is_closed = false;
};
} // namespace starrocks