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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;

/**
 * Strategy interface for late materialization of different scan types.
 * 
 * <p>Late materialization (also known as lazy column materialization) is an optimization
 * technique that defers reading certain columns from storage until they are actually needed.
 * This can significantly reduce I/O when only a subset of columns is required early in
 * query execution (e.g., for filtering or aggregation).
 * 
 * <p>Implementations of this interface handle scan-specific logic for different table types
 * (e.g., Iceberg, OLAP, Hudi). Each implementation determines:
 * <ul>
 *   <li>Whether late materialization is supported for a given scan operator</li>
 *   <li>Which columns can be deferred and which must be fetched immediately</li>
 *   <li>How to rewrite the scan operator to support deferred column reading</li>
 * </ul>
 * 
 * <p><b>Example usage:</b>
 * <pre>{@code
 * // Register strategy for a new scan type
 * ScanStrategyRegistry registry = new ScanStrategyRegistry();
 * registry.registerStrategy(new MyCustomScanStrategy());
 * 
 * // Find and apply strategy
 * Optional<LateMaterializationScanStrategy> strategy = registry.findStrategy(scanOperator);
 * if (strategy.isPresent() && strategy.get().supportsLateMaterialization(scanOperator)) {
 *     ScanRewriteContext ctx = strategy.get().createRewriteContext(...);
 *     OptExpression rewritten = strategy.get().rewriteScan(...);
 * }
 * }</pre>
 * 
 * @see IcebergScanStrategy
 * @see OlapScanStrategy
 */
public interface LateMaterializationScanStrategy {
    
    /**
     * Checks if late materialization is supported for this scan operator.
     * 
     * <p>Implementations should check both the operator type and any table-specific
     * requirements. For example, IcebergScanStrategy requires Iceberg format version 3+
     * with Parquet format.
     * 
     * @param scanOperator the scan operator to check
     * @return true if late materialization is supported, false otherwise
     */
    boolean supportsLateMaterialization(PhysicalScanOperator scanOperator);
    
    /**
     * Creates a context for rewriting the scan operator.
     * 
     * <p>This method analyzes the scan operator to determine:
     * <ul>
     *   <li>Which columns must be fetched immediately (used in predicates/projections)</li>
     *   <li>Which columns can be deferred until later</li>
     *   <li>What row ID columns are needed for late fetching</li>
     * </ul>
     * 
     * @param scanOperator the scan operator to analyze
     * @param optimizerContext the optimizer context for column factory access
     * @param rowIdManager manager for creating row ID columns
     * @return a scan rewrite context containing column categorization
     */
    ScanRewriteContext createRewriteContext(PhysicalScanOperator scanOperator, 
                                           OptimizerContext optimizerContext,
                                           RowIdColumnManager rowIdManager);
    
    /**
     * Rewrites the scan operator to support late materialization.
     * 
     * <p>The rewritten scan operator will:
     * <ul>
     *   <li>Output only immediately-needed columns (not deferred columns)</li>
     *   <li>Include synthetic row ID columns for later lookup</li>
     *   <li>Have updated logical properties reflecting reduced column set</li>
     * </ul>
     * 
     * @param optExpression the original scan expression to rewrite
     * @param rewriteCtx the context containing column categorization
     * @param optimizerContext the optimizer context
     * @return the rewritten scan expression with reduced column output
     */
    OptExpression rewriteScan(OptExpression optExpression, 
                             ScanRewriteContext rewriteCtx,
                             OptimizerContext optimizerContext);
}
