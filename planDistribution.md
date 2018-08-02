# Plan Distribution


## planDistribution

经过[PlanFragmenter](PlanFragmenter)处理得到SubPlan后，将会执行planDistribution将任务分发:
```java
private void planDistribution(PlanRoot plan)
{
...

    // plan the execution on the active nodes
    DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(splitManager);
    StageExecutionPlan outputStageExecutionPlan = distributedPlanner.plan(plan.getRoot(), stateMachine.getSession());
...

    // build the stage execution objects (this doesn't schedule execution)
    SqlQueryScheduler scheduler = new SqlQueryScheduler(
            stateMachine,
            locationFactory,
            outputStageExecutionPlan,
            nodePartitioningManager,
            nodeScheduler,
            remoteTaskFactory,
            stateMachine.getSession(),
            plan.isSummarizeTaskInfos(),
            scheduleSplitBatchSize,
            queryExecutor,
            schedulerExecutor,
            failureDetector,
            rootOutputBuffers,
            nodeTaskMap,
            executionPolicy,
            schedulerStats);

    queryScheduler.set(scheduler);
...
}
```
该方法主要分为两个步骤:
* 调用DistributedExecutionPlanner.plan将SubPlan转为StageExecutionPlan;
* 创建SqlQueryScheduler对象;

## StageExecutionPlan

DistributedExecutionPlanner.plan最终调用到DistributedExecutionPlanner.doPlan:
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
doPlan是一个递归调用的方法，输入参数SubPlan是树状结构，doPlan首先对根节点使用子类Visitor访问者处理，
然后对root的子节点分别调用doPlan，生成对应的StageExecutionPlan列表，然后再构造根节点的StageExecutionPlan.

最终树状结构的SubPlan就被替换成了同样结构的StageExecutionPlan,每一个SubPlan都变成了StageExecutionPlan.

我们观察StageExecutionPlan和SubPlan的区别可以发现，其实每个StageExecutionPlan只是比SubPlan多出来一个成员:splitSources,
splitSources就是Visitor访问后生成的。

我们来看Visitor到底做了什么，对于只有一个source节点的Node，内容如下：
```java
return node.getSource().accept(this, context);
```
只是让Visitor继续访问source节点而已;

如果Node有多个source节点，例如visitJoin方法，有左右两个source节点，则分别处理子source节点再合并:

```java
public Map<PlanNodeId, SplitSource> visitJoin(JoinNode node, Void context)
{
    Map<PlanNodeId, SplitSource> leftSplits = node.getLeft().accept(this, context);
    Map<PlanNodeId, SplitSource> rightSplits = node.getRight().accept(this, context);
    return ImmutableMap.<PlanNodeId, SplitSource>builder()
            .putAll(leftSplits)
            .putAll(rightSplits)
            .build();
}
```

如果Node类型本身就属于数据源，比如visitTableScan，则会构造实际的splitSources:

```java
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

我们知道在之前的Fragmenter步骤中，PlanNode结构被以REMOTE ExchangeNode为分割点切割成若干SubPlan，
而原先的ExchangeNode被替换成了RemoteSourceNode.

而在DistributedExecutionPlanner.Visitor中我们看到，visit方法会一直递归调用当前节点的source节点，直到发现真正的数据源结点，
比如TableScanNode，但是我们也看到上层的SubPlan最终的source节点为RemoteSourceNode，那么对RemoteSourceNode的处理是什么呢?

```java
public Map<PlanNodeId, SplitSource> visitRemoteSource(RemoteSourceNode node, Void context)
{
    // remote source node does not have splits
    return ImmutableMap.of();
}
```
结果是一个空Map，也就是说对于上层的StageExecutionPlan

