#pragma once

#include "column/array_column.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "fmt/format.h"

namespace starrocks {
class SchemaCheckVisitor final : public ColumnVisitorAdapter<SchemaCheckVisitor> {
public:
    explicit SchemaCheckVisitor(const Column* column) : ColumnVisitorAdapter(this), _column(column) {}

    Status do_visit(const NullableColumn& column) {
        if (dynamic_cast<const NullableColumn*>(_column) == nullptr) {
            return Status::InternalError(fmt::format("unmatch type: nullable vs {}", typeid(*_column).name()));
        }
        SchemaCheckVisitor check_data_column(down_cast<const NullableColumn*>(_column)->data_column().get());
        return column.data_column()->accept(&check_data_column);
    }

    Status do_visit(const ConstColumn& column) {
        if (dynamic_cast<const ConstColumn*>(_column) == nullptr) {
            return Status::InternalError(fmt::format("unmatch type: const vs {}", typeid(*_column).name()));
        }
        SchemaCheckVisitor check_data_column(down_cast<const ConstColumn*>(_column)->data_column().get());
        return column.data_column()->accept(&check_data_column);
    }

    Status do_visit(const ArrayColumn& column) {
        if (dynamic_cast<const ArrayColumn*>(_column) == nullptr) {
            return Status::InternalError(fmt::format("unmatch type: array vs {}", typeid(*_column).name()));
        }
        SchemaCheckVisitor check_data_column(down_cast<const ArrayColumn*>(_column)->elements_column().get());
        return column.elements_column()->accept(&check_data_column);
    }

    Status do_visit(const MapColumn& column) { return Status::OK(); }

    Status do_visit(const StructColumn& column) { return Status::OK(); }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& column) {
        if (dynamic_cast<const BinaryColumnBase<T>*>(_column) == nullptr) {
            return Status::InternalError(fmt::format("unmatch type: binary vs {}", typeid(*_column).name()));
        }
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        if (dynamic_cast<const FixedLengthColumnBase<T>*>(_column) == nullptr) {
            return Status::InternalError(fmt::format("unmatch type: int vs {}", typeid(*_column).name()));
        }
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const ObjectColumn<T>& column) {
        if (dynamic_cast<const ObjectColumn<T>*>(_column) == nullptr) {
            return Status::InternalError(fmt::format("unmatch type: object vs {}", typeid(*_column).name()));
        }
        return Status::OK();
    }

    Status do_visit(const JsonColumn& column) {
        if (dynamic_cast<const JsonColumn*>(_column) == nullptr) {
            return Status::InternalError(fmt::format("unmatch type: json vs {}", typeid(*_column).name()));
        }
        return Status::OK();
    }

private:
    const Column* _column;
};
} // namespace starrocks