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
    MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, context.getLookup(), getMaxReorderedJoins(context.getSession()));
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

