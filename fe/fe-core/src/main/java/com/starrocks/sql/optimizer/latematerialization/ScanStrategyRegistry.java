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
 * 
 * <p>This registry manages different strategies for different scan operator types,
 * allowing the late materialization rewriter to support multiple table formats
 * without tight coupling.
 * 
 * <p><b>Built-in strategies:</b>
 * <ul>
 *   <li>{@link IcebergScanStrategy} - Apache Iceberg tables (v3+ Parquet)</li>
 *   <li>{@link OlapScanStrategy} - StarRocks native OLAP tables</li>
 * </ul>
 * 
 * <p><b>Adding custom strategies:</b>
 * <pre>{@code
 * ScanStrategyRegistry registry = new ScanStrategyRegistry();
 * registry.registerStrategy(new MyCustomScanStrategy());
 * }</pre>
 * 
 * <p>The registry uses a first-match approach: when looking up a strategy for a scan
 * operator, it returns the first registered strategy that supports the operator.
 */
public class ScanStrategyRegistry {
    // List of registered strategies, checked in registration order
    private final List<LateMaterializationScanStrategy> strategies;
    
    /**
     * Creates a new registry with built-in strategies pre-registered.
     * The strategies are tried in this order:
     * 1. IcebergScanStrategy
     * 2. OlapScanStrategy
     */
    public ScanStrategyRegistry() {
        this.strategies = new ArrayList<>();
        // Register built-in strategies - order matters for lookup
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
