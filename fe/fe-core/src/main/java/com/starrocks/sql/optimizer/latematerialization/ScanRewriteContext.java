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

package com.starrocks.sql.optimizer.latematerialization;

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Context for rewriting a scan operator to support late materialization.
 * Contains information about which columns to fetch immediately and which to defer.
 */
public class ScanRewriteContext {
    // Columns that should be fetched immediately (not lazy)
    private final Set<ColumnRefOperator> immediateFetchColumns;
    
    // All original columns from the scan
    private final Map<ColumnRefOperator, Column> allColumns;
    
    // Row ID column for this scan
    private ColumnRefOperator rowIdColumn;
    
    // Reference columns for fetching (scanRangeId, rowId)
    private List<ColumnRefOperator> fetchRefColumns;
    
    public ScanRewriteContext(Set<ColumnRefOperator> immediateFetchColumns,
                             Map<ColumnRefOperator, Column> allColumns) {
        this.immediateFetchColumns = immediateFetchColumns;
        this.allColumns = allColumns;
    }
    
    public Set<ColumnRefOperator> getImmediateFetchColumns() {
        return immediateFetchColumns;
    }
    
    public Map<ColumnRefOperator, Column> getAllColumns() {
        return allColumns;
    }
    
    public ColumnRefOperator getRowIdColumn() {
        return rowIdColumn;
    }
    
    public void setRowIdColumn(ColumnRefOperator rowIdColumn) {
        this.rowIdColumn = rowIdColumn;
    }
    
    public List<ColumnRefOperator> getFetchRefColumns() {
        return fetchRefColumns;
    }
    
    public void setFetchRefColumns(List<ColumnRefOperator> fetchRefColumns) {
        this.fetchRefColumns = fetchRefColumns;
    }
    
    /**
     * Check if all columns need immediate fetching (no lazy materialization needed)
     */
    public boolean needsAllColumns() {
        return immediateFetchColumns.size() == allColumns.size();
    }
}
