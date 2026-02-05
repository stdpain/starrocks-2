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

import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Registry for late materialization scan strategies.
 * Manages different strategies for different scan operator types.
 */
public class ScanStrategyRegistry {
    private final List<LateMaterializationScanStrategy> strategies;
    
    public ScanStrategyRegistry() {
        this.strategies = new ArrayList<>();
        // Register built-in strategies
        registerStrategy(new IcebergScanStrategy());
        registerStrategy(new OlapScanStrategy());
    }
    
    /**
     * Registers a new strategy.
     */
    public void registerStrategy(LateMaterializationScanStrategy strategy) {
        strategies.add(strategy);
    }
    
    /**
     * Finds a suitable strategy for the given scan operator.
     * Returns the first strategy that supports the operator.
     */
    public Optional<LateMaterializationScanStrategy> findStrategy(PhysicalScanOperator scanOperator) {
        return strategies.stream()
                .filter(strategy -> strategy.supportsLateMaterialization(scanOperator))
                .findFirst();
    }
}
