ReorderJoins是优化器中的Rule，Pattern为:

```java
private static final Pattern<JoinNode> PATTERN = join().matching(
            joinNode -> !joinNode.getDistributionType().isPresent()
                    && joinNode.getType() == INNER
                    && isDeterministic(joinNode.getFilter().orElse(TRUE_LITERAL)));
```

匹配的条件为：joinNode节点，分布式算法未确定，过滤条件表达式是确定的（不包含随机函数）。

```java
public Result apply(JoinNode joinNode, Captures captures, Context context)
{
    //(1)
    MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, context.getLookup(), getMaxReorderedJoins(context.getSession()));
    //(2)
    JoinEnumerator joinEnumerator = new JoinEnumerator(
            costComparator,
            multiJoinNode.getFilter(),
            context);
    JoinEnumerationResult result = joinEnumerator.chooseJoinOrder(multiJoinNode.getSources(), multiJoinNode.getOutputSymbols());
    if (!result.getPlanNode().isPresent()) {
        return Result.empty();
    }
    return Result.ofPlanNode(result.getPlanNode().get());
}
```

1. toMultiJoinNode

```java
static MultiJoinNode toMultiJoinNode(JoinNode joinNode, Lookup lookup, int joinLimit)
{
// the number of sources is the number of joins + 1
return new JoinNodeFlattener(joinNode, lookup, joinLimit + 1).toMultiJoinNode();
}

JoinNodeFlattener(JoinNode node, Lookup lookup, int sourceLimit)
{
    requireNonNull(node, "node is null");
    checkState(node.getType() == INNER, "join type must be INNER");
    this.outputSymbols = node.getOutputSymbols();
    this.lookup = requireNonNull(lookup, "lookup is null");
    flattenNode(node, sourceLimit);
}

// JoinNode是一个树状结构，该方法的功能是将所有‘叶子节点’添加到sources中；将所有的Join条件添加到filters中
private void flattenNode(PlanNode node, int limit)
{
    PlanNode resolved = lookup.resolve(node);

    // (limit - 2) because you need to account for adding left and right side
    // 如果当前节点不是JoinNode类型，或者sources中的JoinNode数量超过limit - 2，添加当前节点并返回
    if (!(resolved instanceof JoinNode) || (sources.size() > (limit - 2))) {
        sources.add(node);
        return;
    }

    JoinNode joinNode = (JoinNode) resolved;
    // 如果当前JoinNode类型不是INNER JOIN；或者joinNode的过滤器不是确定；或者JOIN算法已经确定，添加当前节点并返回
    if (joinNode.getType() != INNER || !isDeterministic(joinNode.getFilter().orElse(TRUE_LITERAL)) || joinNode.getDistributionType().isPresent()) {
        sources.add(node);
        return;
    }

    // we set the left limit to limit - 1 to account for the node on the right
    // 第归处理JoinNode左右子节点
    flattenNode(joinNode.getLeft(), limit - 1);
    flattenNode(joinNode.getRight(), limit);
    // 将joinNode的criteria和Filter添加到filters中
    joinNode.getCriteria().stream()
            .map(EquiJoinClause::toExpression)
            .forEach(filters::add);
    joinNode.getFilter().ifPresent(filters::add);
}

MultiJoinNode toMultiJoinNode()
{
    return new MultiJoinNode(sources, and(filters), outputSymbols);
}
```

2. joinEnumerator.chooseJoinOrder

```java
        private JoinEnumerationResult chooseJoinOrder(LinkedHashSet<PlanNode> sources, List<Symbol> outputSymbols)
        {
            context.checkTimeoutNotExhausted();

            Set<PlanNode> multiJoinKey = ImmutableSet.copyOf(sources);
            // 检查缓存中有没有结果
            JoinEnumerationResult bestResult = memo.get(multiJoinKey);
            if (bestResult == null) {
                checkState(sources.size() > 1, "sources size is less than or equal to one");
                ImmutableList.Builder<JoinEnumerationResult> resultBuilder = ImmutableList.builder();
                // 生成sources.size()
                Set<Set<Integer>> partitions = generatePartitions(sources.size());
                for (Set<Integer> partition : partitions) {
                    JoinEnumerationResult result = createJoinAccordingToPartitioning(sources, outputSymbols, partition);
                    if (result.equals(UNKNOWN_COST_RESULT)) {
                        memo.put(multiJoinKey, result);
                        return result;
                    }
                    if (!result.equals(INFINITE_COST_RESULT)) {
                        resultBuilder.add(result);
                    }
                }

                List<JoinEnumerationResult> results = resultBuilder.build();
                if (results.isEmpty()) {
                    memo.put(multiJoinKey, INFINITE_COST_RESULT);
                    return INFINITE_COST_RESULT;
                }

                bestResult = resultComparator.min(results);
                memo.put(multiJoinKey, bestResult);
            }

            bestResult.planNode.ifPresent((planNode) -> log.debug("Least cost join was: %s", planNode));
            return bestResult;
        }
```



