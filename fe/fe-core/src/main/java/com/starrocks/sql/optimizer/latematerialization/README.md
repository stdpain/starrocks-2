# Late Materialization Strategy Pattern

## Overview

This package contains the strategy pattern implementation for late materialization optimization in StarRocks. Late materialization (also known as lazy column materialization) defers reading certain columns from storage until they are actually needed, reducing I/O and improving query performance.

## Architecture

### Before Refactoring
The original `LateMaterializationRewriter` had:
- **Tight coupling**: Iceberg-specific logic hardcoded in visitor methods
- **Poor extensibility**: Adding new scan types required modifying core classes
- **Code duplication**: Repeated logic for handling row IDs and column mappings
- **Large methods**: 600+ line methods with nested conditionals

### After Refactoring
The refactored implementation uses:
- **Strategy Pattern**: Pluggable scan handlers for different table types
- **Separation of Concerns**: Distinct classes for row ID management, context, and strategies
- **Extensibility**: New scan types can be added without modifying core logic
- **Maintainability**: Smaller, focused methods with clear responsibilities

## Key Components

### 1. LateMaterializationScanStrategy (Interface)
The core strategy interface with three methods:
- `supportsLateMaterialization()`: Checks if strategy applies to a scan operator
- `createRewriteContext()`: Analyzes columns and categorizes them
- `rewriteScan()`: Rewrites scan to output only immediate columns + row IDs

### 2. IcebergScanStrategy
Strategy for Apache Iceberg tables:
- **Requirements**: Format v3+, Parquet files
- **Row ID**: Uses Iceberg's `_row_id` positional delete file feature
- **Use case**: Queries with selective column access on large Iceberg tables

### 3. OlapScanStrategy
Strategy for StarRocks native OLAP tables:
- **Requirements**: Any OLAP table
- **Row ID**: Uses tablet-local row position
- **Use case**: Queries with filtering/aggregation before final projection

### 4. RowIdColumnManager
Manages synthetic columns for row identification:
- `_row_id`: Row identifier within a scan range (BIGINT)
- `_row_source_id`: Distinguishes scan operators (INT)
- `_scan_range_id`: Identifies scan ranges (INT)

### 5. ScanRewriteContext
Holds state during scan rewriting:
- Immediate fetch columns (used in predicates/projections)
- All available columns
- Row ID columns
- Fetch reference columns

### 6. ScanStrategyRegistry
Manages available strategies:
- Auto-registers built-in strategies (Iceberg, OLAP)
- Supports custom strategy registration
- First-match lookup algorithm

## How It Works

### Phase 1: Column Collection (ColumnCollector)
Bottom-up tree traversal that:
1. Identifies scans that support late materialization (via strategy)
2. Marks all columns as initially un-materialized
3. Identifies columns used in predicates → must fetch immediately
4. Identifies columns used in projections → can defer if used later
5. Records fetch positions (where deferred columns must materialize)

### Phase 2: Plan Rewriting (PlanRewriter)
Top-down tree rewriting that:
1. Rewrites scan operators to output reduced column set
2. Adds row ID columns for later lookup
3. Inserts Fetch operators where columns must materialize
4. Inserts Lookup operators to retrieve deferred columns
5. Updates projections and logical properties

## Example Query Flow

```sql
SELECT a, length(b) + length(c)
FROM iceberg_table
WHERE d > 10
LIMIT 100
```

### Original Plan
```
Limit(100)
  Project(a, length(b) + length(c))
    Filter(d > 10)
      IcebergScan(columns: a, b, c, d)  -- Reads ALL columns
```

### Optimized Plan with Late Materialization
```
Limit(100)
  Project(a, length(b) + length(c))
    Fetch(columns: a, b, c)  -- Fetch deferred columns
      Filter(d > 10)
        IcebergScan(columns: d, _row_id, _scan_range_id, _row_source_id)  -- Only filter column
```

**I/O Savings**: Only column `d` is read during filtering. Columns `a`, `b`, `c` are fetched after filtering reduces row count (potentially 100× less I/O).

## Adding a New Scan Type

To add support for a new table type:

### 1. Create Strategy Implementation
```java
public class HudiScanStrategy implements LateMaterializationScanStrategy {
    @Override
    public boolean supportsLateMaterialization(PhysicalScanOperator scanOperator) {
        if (!(scanOperator instanceof PhysicalHudiScanOperator)) {
            return false;
        }
        HudiTable table = (HudiTable) scanOperator.getTable();
        // Add Hudi-specific checks (e.g., table format, version)
        return table.supportRowPosition();
    }
    
    @Override
    public ScanRewriteContext createRewriteContext(...) {
        // Analyze predicate/projection columns
        // Return context with immediate vs. deferred columns
    }
    
    @Override
    public OptExpression rewriteScan(...) {
        // Create new PhysicalHudiScanOperator with reduced columns
        // Add row ID columns
        // Return rewritten expression
    }
}
```

### 2. Register Strategy
In `ScanStrategyRegistry` constructor:
```java
public ScanStrategyRegistry() {
    this.strategies = new ArrayList<>();
    registerStrategy(new IcebergScanStrategy());
    registerStrategy(new OlapScanStrategy());
    registerStrategy(new HudiScanStrategy());  // Add new strategy
}
```

### 3. Add Visitor Method (if needed)
In `ColumnCollector` and `PlanRewriter`:
```java
@Override
public OptExpression visitPhysicalHudiScan(OptExpression optExpression, Context context) {
    return visitPhysicalScan(optExpression, context);  // Delegates to strategy
}
```

**That's it!** No changes to core late materialization logic required.

## Testing

### Unit Tests
Test each strategy independently:
```java
@Test
public void testIcebergStrategy() {
    IcebergScanStrategy strategy = new IcebergScanStrategy();
    
    // Test support detection
    assertTrue(strategy.supportsLateMaterialization(icebergV3Scan));
    assertFalse(strategy.supportsLateMaterialization(icebergV2Scan));
    
    // Test context creation
    ScanRewriteContext ctx = strategy.createRewriteContext(...);
    assertEquals(expectedImmediateCols, ctx.getImmediateFetchColumns());
    
    // Test scan rewriting
    OptExpression rewritten = strategy.rewriteScan(...);
    // Verify reduced output columns
}
```

### Integration Tests
Use existing `GlobalLateMaterializationTest`:
- Tests Iceberg v3 table join scenarios
- Validates Fetch operator placement
- Checks query plan structure

## Performance Considerations

### When Late Materialization Helps
- Queries with selective predicates (high filter ratio)
- Wide tables with many columns
- Queries accessing only a few columns
- Joins where probe side needs few columns initially

### When to Avoid
- Queries accessing all columns anyway
- Small tables (overhead > benefit)
- Predicates that don't reduce rows significantly
- Streaming queries that must access all data

## Metrics

Track effectiveness with:
- Column fetch reduction ratio
- I/O bytes saved
- Query latency improvement
- Memory usage reduction

## Future Enhancements

Potential improvements:
- [ ] Support for Delta Lake tables
- [ ] Support for Paimon tables
- [ ] Cost-based decision (only apply if estimated benefit > overhead)
- [ ] Push down late materialization to storage engine
- [ ] Support for nested column types (structs, arrays)

## References

- Original implementation: `LateMaterializationRewriter.java` (commit 2e665164)
- Strategy pattern: Gang of Four design patterns
- Iceberg positional deletes: https://iceberg.apache.org/spec/#position-delete-files
