#include "exprs/vectorized/vcolumn_ref.h"

namespace starrocks::vectorized {
VColumnRef::VColumnRef(const TExprNode& node)
        : Expr(node, true), _column_id(node.vslot_ref.slot_id), _is_nullable(node.vslot_ref.nullable) {}

ColumnPtr VColumnRef::evaluate(ExprContext* context, Chunk* ptr) {
    ColumnPtr& column = (ptr)->get_column_by_slot_id(_column_id);
    return column;
}

} // namespace starrocks::vectorized