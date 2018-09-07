# presto 基于代价的优化(cost based optimizer)

presto 基于代价的优化核心是计划的代价如何衡量，是由两个接口提供的：StatsCalculator vs CostCalculator

```java
// 统计信息计算器
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

// 代价计算器
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

1. StatsCalculator 统计信息计算器

优化器中使用的StatsCalculator接口的实现类为SelectingStatsCalculator。
该实现类包装了两个具体的实现类，分别为oldStatsCalculator和newStatsCalculator：

```java
public class SelectingStatsCalculator
        implements StatsCalculator
{
    private final StatsCalculator oldStatsCalculator;
    private final StatsCalculator newStatsCalculator;

    @Inject
    public SelectingStatsCalculator(@Old StatsCalculator oldStatsCalculator, @New StatsCalculator newStatsCalculator)
    {
        this.oldStatsCalculator = requireNonNull(oldStatsCalculator, "oldStatsCalculator can not be null");
        this.newStatsCalculator = requireNonNull(newStatsCalculator, "newStatsCalculator can not be null");
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        if (SystemSessionProperties.isEnableNewStatsCalculator(session)) {
            return newStatsCalculator.calculateStats(node, sourceStats, lookup, session, types);
        }
        else {
            return oldStatsCalculator.calculateStats(node, sourceStats, lookup, session, types);
        }
    }
}
```

使用哪一个具体实现取决于系统参数配置enable_new_stats_calculator；

其中oldStatsCalculator为CoefficientBasedStatsCalculator；

newStatsCalculator为ComposableStatsCalculator(组件化统计信息计算器)

其中CoefficientBasedStatsCalculator通过Visitor模式访问PlanNode中的各节点，
ComposableStatsCalculator通过注册包含匹配模式的规则来访问PlanNode中各节点。

1.1 CoefficientBasedStatsCalculator

```java
    private class Visitor
            extends PlanVisitor<PlanNodeStatsEstimate, Void>
    {
        // 用来计算PlanNode的source的统计信息
        private final StatsProvider sourceStats;
        private final Session session;

        public Visitor(StatsProvider sourceStats, Session session)
        {
            this.sourceStats = requireNonNull(sourceStats, "sourceStats is null");
            this.session = requireNonNull(session, "session is null");
        }

        private PlanNodeStatsEstimate getStats(PlanNode sourceNode)
        {
            // 计算source节点
            return sourceStats.getStats(sourceNode);
        }

        @Override
        protected PlanNodeStatsEstimate visitPlan(PlanNode node, Void context)
        {
            // 默认是未知统计信息
            return UNKNOWN_STATS;
        }

        @Override
        public PlanNodeStatsEstimate visitGroupReference(GroupReference node, Void context)
        {
            // StatsCalculator should not be directly called on GroupReference
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanNodeStatsEstimate visitOutput(OutputNode node, Void context)
        {
            return getStats(node.getSource());
        }

        @Override
        public PlanNodeStatsEstimate visitFilter(FilterNode node, Void context)
        {
            // 对于FilterNode，将source的统计信息乘以过滤系数（0.5），即默认过滤后的数据为原数据量的50%
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            return sourceStats.mapOutputRowCount(value -> value * FILTER_COEFFICIENT);
        }

        @Override
        public PlanNodeStatsEstimate visitProject(ProjectNode node, Void context)
        {
            return getStats(node.getSource());
        }

        @Override
        public PlanNodeStatsEstimate visitJoin(JoinNode node, Void context)
        {
            // 对于Join节点，获取左右子表的统计信息，取其中行数多的乘以JOIN匹配系数，默认值为2
            PlanNodeStatsEstimate leftStats = getStats(node.getLeft());
            PlanNodeStatsEstimate rightStats = getStats(node.getRight());

            PlanNodeStatsEstimate.Builder joinStats = PlanNodeStatsEstimate.builder();
            double rowCount = Math.max(leftStats.getOutputRowCount(), rightStats.getOutputRowCount()) * JOIN_MATCHING_COEFFICIENT;
            joinStats.setOutputRowCount(rowCount);
            return joinStats.build();
        }

        @Override
        public PlanNodeStatsEstimate visitExchange(ExchangeNode node, Void context)
        {
        // 对于Exchange节点，取所有源节点统计数据之和
            double rowCount = 0;
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNodeStatsEstimate sourceStat = getStats(node.getSources().get(i));
                rowCount = rowCount + sourceStat.getOutputRowCount();
            }

            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(rowCount)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitTableScan(TableScanNode node, Void context)
        {
        // tablescan节点从元数据中获取统计信息
            Constraint<ColumnHandle> constraint = new Constraint<>(node.getCurrentConstraint());
            TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint);
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(tableStatistics.getRowCount().getValue())
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitValues(ValuesNode node, Void context)
        {
            int valuesCount = node.getRows().size();
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(valuesCount)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(1.0)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
        // semijoin 取源节点统计数据乘以Join匹配系数
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            return sourceStats.mapOutputRowCount(rowCount -> rowCount * JOIN_MATCHING_COEFFICIENT);
        }

        @Override
        public PlanNodeStatsEstimate visitLimit(LimitNode node, Void context)
        {
        // Limit节点取源节点统计数据和limit的最小值作为结果
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            PlanNodeStatsEstimate.Builder limitStats = PlanNodeStatsEstimate.builder();
            if (sourceStats.getOutputRowCount() < node.getCount()) {
                limitStats.setOutputRowCount(sourceStats.getOutputRowCount());
            }
            else {
                limitStats.setOutputRowCount(node.getCount());
            }
            return limitStats.build();
        }
    }
```

1.2 ComposableStatsCalculator

```java
public class ComposableStatsCalculator
        implements StatsCalculator
{
    private final ListMultimap<Class<?>, Rule<?>> rulesByRootType;

// 通过构造函数传入规则列表
    public ComposableStatsCalculator(List<Rule<?>> rules)
    {
    // 将rules转成class => list<Rule> 的 multiMap结构
        this.rulesByRootType = rules.stream()
                .peek(rule -> {
                    checkArgument(rule.getPattern() instanceof TypeOfPattern, "Rule pattern must be TypeOfPattern");
                    Class<?> expectedClass = ((TypeOfPattern<?>) rule.getPattern()).expectedClass();
                    checkArgument(!expectedClass.isInterface() && !Modifier.isAbstract(expectedClass.getModifiers()), "Rule must be registered on a concrete class");
                })
                .collect(toMultimap(
                        rule -> ((TypeOfPattern<?>) rule.getPattern()).expectedClass(),
                        rule -> rule,
                        ArrayListMultimap::create));
    }

// 返回node的rules
    private Stream<Rule<?>> getCandidates(PlanNode node)
    {
        for (Class<?> superclass = node.getClass().getSuperclass(); superclass != null; superclass = superclass.getSuperclass()) {
            // This is important because rule ordering, given in the constructor, is significant.
            // We can't check this fully in the constructor, since abstract class may lack `abstract` modifier.
            checkState(rulesByRootType.get(superclass).isEmpty(), "Cannot maintain rule order because there is rule registered for %s", superclass);
        }
        return rulesByRootType.get(node.getClass()).stream();
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
    // 遍历所有node对应的的rules，计算统计信息，如果成功计算统计信息，返回。
        Iterator<Rule<?>> ruleIterator = getCandidates(node).iterator();
        while (ruleIterator.hasNext()) {
            Rule<?> rule = ruleIterator.next();
            Optional<PlanNodeStatsEstimate> calculatedStats = calculateStats(rule, node, sourceStats, lookup, session, types);
            if (calculatedStats.isPresent()) {
                return calculatedStats.get();
            }
        }
        return PlanNodeStatsEstimate.UNKNOWN_STATS;
    }

    private static <T extends PlanNode> Optional<PlanNodeStatsEstimate> calculateStats(Rule<T> rule, PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        return rule.calculate((T) node, sourceStats, lookup, session, types);
    }

    public interface Rule<T extends PlanNode>
    {
        Pattern<T> getPattern();

        Optional<PlanNodeStatsEstimate> calculate(T node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types);
    }
}
```
