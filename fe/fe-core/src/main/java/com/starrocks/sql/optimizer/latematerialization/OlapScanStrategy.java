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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Late materialization strategy for OLAP table scans.
 * 
 * <p>This strategy enables late materialization for StarRocks native OLAP tables.
 * OLAP tables support efficient columnar storage and row-level access, making them
 * ideal candidates for late materialization optimization.
 * 
 * <p><b>Supported scenarios:</b>
 * <ul>
 *   <li>Queries with selective column access (e.g., SELECT a, b FROM t WHERE c > 10)</li>
 *   <li>Joins where probe side only needs a subset of columns initially</li>
 *   <li>Aggregations that can be computed before fetching remaining columns</li>
 * </ul>
 * 
 * <p><b>How it works:</b>
 * <ol>
 *   <li>Receives columns to fetch immediately from ColumnCollector analysis</li>
 *   <li>Adds row ID columns for later lookup via primary key or row position</li>
 *   <li>Reduces scan operator output to only immediate columns + row IDs</li>
 * </ol>
 * 
 * <p><b>Performance benefit:</b> Reduces I/O and memory usage by deferring column
 * materialization until after filtering/aggregation reduces row count.
 */
public class OlapScanStrategy implements LateMaterializationScanStrategy {
    
    @Override
    public boolean supportsLateMaterialization(PhysicalScanOperator scanOperator) {
        if (!(scanOperator instanceof PhysicalOlapScanOperator)) {
            return false;
        }
        
        // OLAP tables support late materialization
        return true;
    }
    
    @Override
    public OptExpression rewriteScan(OptExpression optExpression,
                                    ScanRewriteContext rewriteCtx,
                                    OptimizerContext optimizerContext) {
        PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) optExpression.getOp();
        
        // Build new column map with only immediately fetched columns
        Map<ColumnRefOperator, Column> newColumnRefMap = scanOperator.getColRefToColumnMetaMap()
                .entrySet()
                .stream()
                .filter(entry -> rewriteCtx.getImmediateFetchColumns().contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        
        // Add row ID columns
        ColumnRefOperator rowIdColumn = rewriteCtx.getRowIdColumn();
        if (rowIdColumn != null) {
            Column rowIdCol = optimizerContext.getColumnRefFactory().getColumn(rowIdColumn);
            newColumnRefMap.put(rowIdColumn, rowIdCol);
        }
        
        // Add fetch reference columns
        if (rewriteCtx.getFetchRefColumns() != null) {
            for (ColumnRefOperator refCol : rewriteCtx.getFetchRefColumns()) {
                Column col = optimizerContext.getColumnRefFactory().getColumn(refCol);
                newColumnRefMap.put(refCol, col);
            }
        }
        
        // Build new scan operator
        PhysicalOlapScanOperator.Builder builder = PhysicalOlapScanOperator.builder()
                .withOperator(scanOperator);
        builder.setColRefToColumnMetaMap(newColumnRefMap);
        builder.setGlobalDicts(scanOperator.getGlobalDicts());
        builder.setGlobalDictsExpr(scanOperator.getGlobalDictsExpr());
        
        // Build result
        OptExpression result = OptExpression.builder()
                .with(optExpression)
                .setOp(builder.build())
                .build();
        
        // Update logical property
        LogicalProperty newProperty = new LogicalProperty(optExpression.getLogicalProperty());
        newProperty.setOutputColumns(new ColumnRefSet(newColumnRefMap.keySet()));
        result.setLogicalProperty(newProperty);
        
        return result;
    }
}
