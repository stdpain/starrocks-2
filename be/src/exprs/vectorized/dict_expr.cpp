#include "exprs/vectorized/dict_expr.h"

namespace starrocks::vectorized {
DictExpr::DictExpr(const TExprNode& node) : Expr(node, false) {}
} // namespace starrocks::vectorized