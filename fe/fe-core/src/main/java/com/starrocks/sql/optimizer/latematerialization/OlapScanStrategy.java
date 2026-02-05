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
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Late materialization strategy for OLAP table scans.
 * Supports StarRocks native OLAP tables.
 */
public class OlapScanStrategy implements LateMaterializationScanStrategy {
    
    @Override
    public boolean supportsLateMaterialization(PhysicalScanOperator scanOperator) {
        if (!(scanOperator instanceof PhysicalOlapScanOperator)) {
            return false;
        }
        
        // OLAP tables support late materialization
        OlapTable table = (OlapTable) scanOperator.getTable();
        return true;
    }
    
    @Override
    public ScanRewriteContext createRewriteContext(PhysicalScanOperator scanOperator,
                                                   OptimizerContext optimizerContext,
                                                   RowIdColumnManager rowIdManager) {
        PhysicalOlapScanOperator olapScan = (PhysicalOlapScanOperator) scanOperator;
        
        // Create context with all columns initially un-materialized
        Map<ColumnRefOperator, Column> allColumns = olapScan.getColRefToColumnMetaMap();
        Set<ColumnRefOperator> immediateFetchColumns = new HashSet<>();
        
        // Columns used in predicates must be fetched immediately
        List<ColumnRefOperator> predicateColumns = olapScan.getScanOperatorPredicates()
                .getUsedColumns()
                .getColumnRefOperators(optimizerContext.getColumnRefFactory());
        immediateFetchColumns.addAll(predicateColumns);
        
        // Columns used in projection must be fetched immediately
        if (olapScan.getProjection() != null) {
            List<ColumnRefOperator> projectionColumns = olapScan.getProjection()
                    .getUsedColumns()
                    .getColumnRefOperators(optimizerContext.getColumnRefFactory());
            immediateFetchColumns.addAll(projectionColumns);
        }
        
        return new ScanRewriteContext(immediateFetchColumns, allColumns);
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
