// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "glog/logging.h"

namespace starrocks::vectorized {
class DictExpr final : public Expr {
public:
    DictExpr(const TExprNode& node);
    Expr* clone(ObjectPool* pool) const override { return pool->add(new DictExpr(*this)); }
    ColumnPtr evaluate(ExprContext* context, Chunk* ptr) override { __builtin_unreachable(); }
    SlotId slot_id() {
        DCHECK_EQ(children().size(), 2);
        return down_cast<const ColumnRef*>(get_child(0))->slot_id();
    }
};
} // namespace starrocks::vectorized