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
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;

import java.util.Arrays;
import java.util.List;

/**
 * Manages creation and tracking of row ID columns for late materialization.
 * 
 * <p>Late materialization requires synthetic columns to identify rows for deferred fetching:
 * <ul>
 *   <li><b>_row_id</b>: The primary row identifier within a scan range (BIGINT)</li>
 *   <li><b>_row_source_id</b>: Distinguishes different scan operators (INT)</li>
 *   <li><b>_scan_range_id</b>: Identifies individual scan ranges within an operator (INT)</li>
 * </ul>
 * 
 * <p>These columns are added to the scan operator output and used by Fetch/Lookup
 * operators to retrieve deferred columns later in the query plan.
 * 
 * <p><b>Example:</b>
 * <pre>{@code
 * RowIdColumnManager manager = new RowIdColumnManager(optimizerContext);
 * ColumnRefOperator rowSourceId = manager.createRowSourceIdColumn(table);
 * List<ColumnRefOperator> fetchRefs = manager.createFetchRefColumns(table, existingCols);
 * // fetchRefs contains: [_scan_range_id, _row_id]
 * }</pre>
 */
public class RowIdColumnManager {
    // Column name constants - these must match backend expectations
    private static final String ROW_ID = "_row_id";
    private static final String ROW_SOURCE_ID = "_row_source_id";
    private static final String SCAN_RANGE_ID = "_scan_range_id";
    
    private final OptimizerContext optimizerContext;
    
    public RowIdColumnManager(OptimizerContext optimizerContext) {
        this.optimizerContext = optimizerContext;
    }
    
    /**
     * Creates a row source ID column reference.
     * This column distinguishes different scan operators.
     */
    public ColumnRefOperator createRowSourceIdColumn(Table table) {
        Column column = new Column(ROW_SOURCE_ID, IntegerType.INT, true);
        ColumnRefOperator columnRef = optimizerContext.getColumnRefFactory()
                .create(ROW_SOURCE_ID, IntegerType.INT, true);
        optimizerContext.getColumnRefFactory()
                .updateColumnRefToColumns(columnRef, column, table);
        return columnRef;
    }
    
    /**
     * Creates a scan range ID column reference.
     * This column identifies individual scan ranges.
     */
    public ColumnRefOperator createScanRangeIdColumn(Table table) {
        Column column = new Column(SCAN_RANGE_ID, IntegerType.INT, true);
        ColumnRefOperator columnRef = optimizerContext.getColumnRefFactory()
                .create(SCAN_RANGE_ID, IntegerType.INT, true);
        optimizerContext.getColumnRefFactory()
                .updateColumnRefToColumns(columnRef, column, table);
        return columnRef;
    }
    
    /**
     * Creates or finds existing row ID column reference.
     * This is the primary column for identifying rows.
     */
    public ColumnRefOperator createOrFindRowIdColumn(Table table, 
                                                     java.util.Map<ColumnRefOperator, Column> existingColumns) {
        // Check if _row_id already exists
        for (java.util.Map.Entry<ColumnRefOperator, Column> entry : existingColumns.entrySet()) {
            if (entry.getValue().getName().equalsIgnoreCase(ROW_ID)) {
                return entry.getKey();
            }
        }
        
        // Create new _row_id column
        Column column = new Column(ROW_ID, IntegerType.BIGINT, true);
        ColumnRefOperator columnRef = optimizerContext.getColumnRefFactory()
                .create(ROW_ID, IntegerType.BIGINT, true);
        optimizerContext.getColumnRefFactory()
                .updateColumnRefToColumns(columnRef, column, table);
        return columnRef;
    }
    
    /**
     * Creates all necessary fetch reference columns.
     * Returns [scanRangeId, rowId]
     */
    public List<ColumnRefOperator> createFetchRefColumns(Table table, 
                                                         java.util.Map<ColumnRefOperator, Column> existingColumns) {
        ColumnRefOperator rowId = createOrFindRowIdColumn(table, existingColumns);
        ColumnRefOperator scanRangeId = createScanRangeIdColumn(table);
        return Arrays.asList(scanRangeId, rowId);
    }
}
