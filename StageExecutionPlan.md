# 来源:

DistributedExecutionPlanner
public StageExecutionPlan plan(SubPlan root, Session session)

SubPlan转为StageExecutionPlan

```java
private StageExecutionPlan doPlan(SubPlan root, Session session, ImmutableList.Builder<SplitSource> allSplitSources)
{
    PlanFragment currentFragment = root.getFragment();

    // get splits for this fragment, this is lazy so split assignments aren't actually calculated here
    Map<PlanNodeId, SplitSource> splitSources = currentFragment.getRoot().accept(new Visitor(session, currentFragment.getPipelineExecutionStrategy(), allSplitSources), null);

    // create child stages
    ImmutableList.Builder<StageExecutionPlan> dependencies = ImmutableList.builder();
    for (SubPlan childPlan : root.getChildren()) {
        dependencies.add(doPlan(childPlan, session, allSplitSources));
    }

    return new StageExecutionPlan(
            currentFragment,
            splitSources,
            dependencies.build());
}
```

递归遍历SubPlan中的fragement，调用内部类Visitor访问fragment的PlanNode root节点，
核心是获取Map<PlanNodeId, SplitSource> splitSources

在Visitor中，实际添加了Map元素的visitor方法为visitTableScan:
```java
@Override
public Map<PlanNodeId, SplitSource> visitTableScan(TableScanNode node, Void context)
{
    // get dataSource for table
    SplitSource splitSource = splitManager.getSplits(
            session,
            node.getLayout().get(),
            pipelineExecutionStrategy == GROUPED_EXECUTION ? GROUPED_SCHEDULING : UNGROUPED_SCHEDULING);

    splitSources.add(splitSource);

    return ImmutableMap.of(node.getId(), splitSource);
}
```
对于其他类型节点，只是返回所有source节点的合集。

也就是说splitSources的含义是当前PlanNode所有类型为TableScanNode的叶子节点的PlanNodeId和splitSource的Map
