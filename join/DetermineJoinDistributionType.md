JoinNode的distributionType属性决定了分布式join算法的类型：
```java
public enum DistributionType
{
    PARTITIONED,
    REPLICATED
}
```

PARTITIONED算法是指将两个表根据join使用的的column值计算hash后分布到不同节点上执行；

REPLICATED算法是指将比较小的表发送到所有存在大表分区的节点上执行。

DetermineJoinDistributionType是优化器中的一条规则(Rule)，匹配模式为：

```java
private static final Pattern<JoinNode> PATTERN = join().matching(joinNode -> !joinNode.getDistributionType().isPresent());
```

即匹配joinNode.getDistributionType()为空的JoinNode。

该优化规则会设置具体的join算法：

```java
    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        DistributionType distributionType = determineDistributionType(node, context);
        return Result.ofPlanNode(node.withDistributionType(distributionType));
    }

    private static DistributionType determineDistributionType(JoinNode node, Context context)
    {
        JoinNode.Type type = node.getType();
        if (type == RIGHT || type == FULL) {
            // With REPLICATED, the unmatched rows from right-side would be duplicated.
            // RIGHT JOIN 和 FULL JOIN的情况下，如果将right table复制到所有left table所在节点上，会导致right中不匹配的项重复
            // 所以只能用PARITIONED算法。
            return PARTITIONED;
        }

        if (node.getCriteria().isEmpty() && (type == INNER || type == LEFT)) {
            // 没有criteria，没有可以用来partitioned的列，只能用REPLICATED
            return REPLICATED;
        }

        if (isAtMostScalar(node.getRight(), context.getLookup())) {
        // 右表足够小，可以复制到所有左表所在节点
            return REPLICATED;
        }

        // http://teradata.github.io/presto/docs/0.167-t/optimizer/join-distribution-type.html
        // 系统设置的join分布式算法
        if (getJoinDistributionType(context.getSession()).canRepartition()) {
            return PARTITIONED;
        }

        return REPLICATED;
    }
```
