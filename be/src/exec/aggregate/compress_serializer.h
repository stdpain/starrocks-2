#pragma once

#include <any>

#include "column/column.h"

namespace starrocks {
void bitcompress_serialize(const Columns& columns, const std::vector<std::any>& bases, const std::vector<int>& offsets,
                           size_t num_rows, size_t fixed_key_size, void* buffer);
void bitcompress_deserialize(Columns& columns, const std::vector<std::any>& bases, const std::vector<int>& offsets,
                             const std::vector<int>& used_bits, size_t num_rows, size_t fixed_key_size, void* buffer);
} // namespace starrocks
