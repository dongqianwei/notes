# presto 基于代价的优化(cost based optimizer)

presto 基于代价的优化核心是计划的代价如何衡量，是由两个接口提供的：StatsCalculator vs CostCalculator

```java
public interface StatsCalculator
{
    /**
     * Calculate stats for the {@code node}.
     *  @param node The node to compute stats for.
     * @param sourceStats The stats provider for any child nodes' stats, if needed to compute stats for the {@code node}
     * @param lookup Lookup to be used when resolving source nodes, allowing stats calculation to work within {@link IterativeOptimizer}
     * @param types
     */
    PlanNodeStatsEstimate calculateStats(
            PlanNode node,
            StatsProvider sourceStats,
            Lookup lookup,
            Session session,
            TypeProvider types);
}

public interface CostCalculator
{
    /**
     * Calculates non-cumulative cost of a node.
     *
     * @param node The node to compute cost for.
     * @param stats The stats provider for node's stats and child nodes' stats, to be used if stats are needed to compute cost for the {@code node}
     * @param lookup Lookup to be used when resolving source nodes, allowing cost calculation to work within {@link IterativeOptimizer}
     */
    PlanNodeCostEstimate calculateCost(
            PlanNode node,
            StatsProvider stats,
            Lookup lookup,
            Session session,
            TypeProvider types);

    @BindingAnnotation
    @Target({PARAMETER})
    @Retention(RUNTIME)
    @interface EstimatedExchanges {}
}

```
