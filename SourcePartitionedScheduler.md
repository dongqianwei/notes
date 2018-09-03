# SourcePartitionedScheduler 调度器

SourcePartitionedScheduler通过两个静态方法间接调用私有构造方法：
```java
public static SourcePartitionedScheduler simpleSourcePartitionedScheduler(
        SqlStageExecution stage,
        PlanNodeId partitionedNode,
        SplitSource splitSource,
        SplitPlacementPolicy splitPlacementPolicy,
        int splitBatchSize)
{
    SourcePartitionedScheduler result = new SourcePartitionedScheduler(stage, partitionedNode, splitSource, splitPlacementPolicy, splitBatchSize, true);
    result.startLifespan(Lifespan.taskWide(), NOT_PARTITIONED);
    return result;
}


public static SourcePartitionedScheduler managedSourcePartitionedScheduler(
        SqlStageExecution stage,
        PlanNodeId partitionedNode,
        SplitSource splitSource,
        SplitPlacementPolicy splitPlacementPolicy,
        int splitBatchSize)
{
    return new SourcePartitionedScheduler(stage, partitionedNode, splitSource, splitPlacementPolicy, splitBatchSize, false);
}
```

1. simpleSourcePartitionedScheduler方法在SqlQueryScheduler.createStages中调用。

2. managedSourcePartitionedScheduler方法在FixedSourcePartitionedScheduler调度器构造方法中间接调动。

本文中只考虑SourcePartitionedScheduler直接使用的场景，因此仅分析simpleSourcePartitionedScheduler使用的场景。

```java
PartitioningHandle partitioningHandle = plan.getFragment().getPartitioning();
if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
    // nodes are selected dynamically based on the constraints of the splits and the system load
    Entry<PlanNodeId, SplitSource> entry = Iterables.getOnlyElement(plan.getSplitSources().entrySet());
    PlanNodeId planNodeId = entry.getKey();
    SplitSource splitSource = entry.getValue();
    ConnectorId connectorId = splitSource.getConnectorId();
    if (isInternalSystemConnector(connectorId)) {
        connectorId = null;
    }
    NodeSelector nodeSelector = nodeScheduler.createNodeSelector(connectorId);
    SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stage::getAllTasks);

    checkArgument(plan.getFragment().getPipelineExecutionStrategy() == UNGROUPED_EXECUTION);
    stageSchedulers.put(stageId, simpleSourcePartitionedScheduler(stage, planNodeId, splitSource, placementPolicy, splitBatchSize));
    bucketToPartition = Optional.of(new int[1]);
}
```
可以看到，只有当partitioningHandle为SOURCE_DISTRIBUTION时才会调用simpleSourcePartitionedScheduler，
设置当前StageExecution的调度器。

在PlanFragmenter.Fragmenter.visitTableScan方法中：
```java
public PlanNode visitTableScan(TableScanNode node, RewriteContext<FragmentProperties> context)
{
    PartitioningHandle partitioning = node.getLayout()
            .map(layout -> metadata.getLayout(session, layout))
            .flatMap(TableLayout::getTablePartitioning)
            .map(TablePartitioning::getPartitioningHandle)
            .orElse(SOURCE_DISTRIBUTION);

    context.get().addSourceDistribution(node.getId(), partitioning);
    return context.defaultRewrite(node, context.get());
}
```
如果tableScanNode节点中tableLayout中没有设置partitioningHandle的话，使用默认的handle，也就是SOURCE_DISTRIBUTION。

也就是说只有当PlanNode的叶子节点为TableScanNode且没有默认的paritioningHandle时，
当前Fragment的partitioningHandle为SOURCE_DISTRIBUTION。

下面来看simpleSourcePartitionedScheduler方法的参数：
1. SqlStageExecution stage, // 当前StageExecution
2. PlanNodeId partitionedNode, // 作为stage数据源的TableScanNode的Id
3. SplitSource splitSource, // 下文解释
4. SplitPlacementPolicy splitPlacementPolicy, // 下文解释
5. int splitBatchSize // split batch大小，系统配置

下面着重解释splitSource和splitPlacementPolicy两个参数：

1. splitSource
```java
// splictSource来源于plan.getSplitSources()
Entry<PlanNodeId, SplitSource> entry = Iterables.getOnlyElement(plan.getSplitSources().entrySet());
PlanNodeId planNodeId = entry.getKey();
SplitSource splitSource = entry.getValue();

public Map<PlanNodeId, SplitSource> getSplitSources()
{
    return splitSources;
}

// splitSources是StageExecutionPlan构造函数中指定的
public StageExecutionPlan(
        PlanFragment fragment,
        Map<PlanNodeId, SplitSource> splitSources,
        List<StageExecutionPlan> subStages)
{
    this.fragment = requireNonNull(fragment, "fragment is null");
    this.splitSources = requireNonNull(splitSources, "dataSource is null");
    this.subStages = ImmutableList.copyOf(requireNonNull(subStages, "dependencies is null"));

    fieldNames = (fragment.getRoot() instanceof OutputNode) ?
            Optional.of(ImmutableList.copyOf(((OutputNode) fragment.getRoot()).getColumnNames())) :
            Optional.empty();
}

// DistributedExecutionPlanner
// 构造函数中的splitSources是使用DistributedExecutionPlanner.Visitor访问SubPlan节点生成的
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

// 访问者中的visitTableScan返回了splitSource
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

// splitManager.getSplits
// 调用了插件实现的ConnectorSplitManager.getSplits方法
public SplitSource getSplits(Session session, TableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
{
    ConnectorId connectorId = layout.getConnectorId();
    ConnectorSplitManager splitManager = getConnectorSplitManager(connectorId);

    ConnectorSession connectorSession = session.toConnectorSession(connectorId);

    ConnectorSplitSource source = splitManager.getSplits(
            layout.getTransactionHandle(),
            connectorSession,
            layout.getConnectorHandle(),
            splitSchedulingStrategy);

    SplitSource splitSource = new ConnectorAwareSplitSource(connectorId, layout.getTransactionHandle(), source);
    if (minScheduleSplitBatchSize > 1) {
        splitSource = new BufferingSplitSource(splitSource, minScheduleSplitBatchSize);
    }
    return splitSource;
}
```

