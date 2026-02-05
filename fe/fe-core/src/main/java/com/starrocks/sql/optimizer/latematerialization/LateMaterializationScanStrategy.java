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
 * Implementations handle scan-specific logic for lazy column materialization.
 */
public interface LateMaterializationScanStrategy {
    
    /**
     * Checks if late materialization is supported for this scan operator.
     * 
     * @param scanOperator the scan operator to check
     * @return true if late materialization is supported, false otherwise
     */
    boolean supportsLateMaterialization(PhysicalScanOperator scanOperator);
    
    /**
     * Creates a context for rewriting the scan operator.
     * 
     * @param scanOperator the scan operator to rewrite
     * @param optimizerContext the optimizer context
     * @param rowIdManager the row ID column manager
     * @return a scan rewrite context
     */
    ScanRewriteContext createRewriteContext(PhysicalScanOperator scanOperator, 
                                           OptimizerContext optimizerContext,
                                           RowIdColumnManager rowIdManager);
    
    /**
     * Rewrites the scan operator to support late materialization.
     * 
     * @param optExpression the scan expression to rewrite
     * @param rewriteCtx the scan rewrite context
     * @param optimizerContext the optimizer context
     * @return the rewritten scan expression
     */
    OptExpression rewriteScan(OptExpression optExpression, 
                             ScanRewriteContext rewriteCtx,
                             OptimizerContext optimizerContext);
}
