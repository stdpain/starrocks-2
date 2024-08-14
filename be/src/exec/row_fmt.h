#pragma once

#include "column/column_visitor_adapter.h"

namespace starrocks {
class RowFmtVisitor final : public ColumnVisitorMutableAdapter<RowFmtVisitor> {
public:
    Status do_visit(NullableColumn* column) { return Status::OK(); }

    template <typename T>
    Status do_visit(BinaryColumnBase<T>* column) {
        return Status::OK();
    }
};
} // namespace starrocks