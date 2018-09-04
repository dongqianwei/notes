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

// 其中调用result.startLifespan，向scheduleGroups中添加了一组lifespan（Lifespan.taskWide()）和调度组对象的映射。
public synchronized void startLifespan(Lifespan lifespan, ConnectorPartitionHandle partitionHandle)
{
checkState(state == State.INITIALIZED || state == State.SPLITS_ADDED);
scheduleGroups.put(lifespan, new ScheduleGroup(partitionHandle));
whenFinishedOrNewLifespanAdded.set(null);
whenFinishedOrNewLifespanAdded = SettableFuture.create();
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
// 调用了插件实现的ConnectorSplitManager.getSplits方法返回ConnectorSplitSource，
// 再通过ConnectorAwareSplitSource类转成SplitSource接口再返回。
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

2. splitPlacementPolicy

```java
// 该接口用于通过传入splits参数计算SplitPlacementResult，也就是split和执行节点的映射表
public interface SplitPlacementPolicy
{
    SplitPlacementResult computeAssignments(Set<Split> splits);

    void lockDownNodes();

    List<Node> allNodes();
}

public final class SplitPlacementResult
{
    private final ListenableFuture<?> blocked;
    private final Multimap<Node, Split> assignments;

    public SplitPlacementResult(ListenableFuture<?> blocked, Multimap<Node, Split> assignments)
    {
        this.blocked = requireNonNull(blocked, "blocked is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
    }

    public ListenableFuture<?> getBlocked()
    {
        return blocked;
    }

    public Multimap<Node, Split> getAssignments()
    {
        return assignments;
    }
}
```

下面来看调度器的调度算法：

```java
@Override
public synchronized ScheduleResult schedule()
{
...

// simpleSourcePartitionedScheduler静态方法中只向scheduleGroups中添加了一组对象，因此只会循环一次。
for (Entry<Lifespan, ScheduleGroup> entry : scheduleGroups.entrySet()) {
    Lifespan lifespan = entry.getKey();
    ScheduleGroup scheduleGroup = entry.getValue();
    // 第一次为空
    Set<Split> pendingSplits = scheduleGroup.pendingSplits;

    // scheduleGroup.state初始默认值就是DISCOVERING_SPLITS，如果是其他状态（NO_MORE_SPLIT/DONE），
    // nextSplitBatchFuture必须为null
    if (scheduleGroup.state != ScheduleGroupState.DISCOVERING_SPLITS) {
        verify(scheduleGroup.nextSplitBatchFuture == null);
    }
    // 第一次pendingSplits为空
    else if (pendingSplits.isEmpty()) {
        // try to get the next batch
        if (scheduleGroup.nextSplitBatchFuture == null) {
        // 调用splitSource.getNextBatch得到nextSplitBatchFuture用来获取下一批次的SplitBatch
            scheduleGroup.nextSplitBatchFuture = splitSource.getNextBatch(scheduleGroup.partitionHandle, lifespan, splitBatchSize - pendingSplits.size());
        ...
        }

        // 如果nextSplitBatchFuture.isDone()为true，说明此时已经可以获取SplitBatch
        if (scheduleGroup.nextSplitBatchFuture.isDone()) {
            // 调用getFutureValue获取nextSplits
            SplitBatch nextSplits = getFutureValue(scheduleGroup.nextSplitBatchFuture);
            scheduleGroup.nextSplitBatchFuture = null;
            // 将下一批次的Splits添加到pendingSplits中
            pendingSplits.addAll(nextSplits.getSplits());
            // 如果nextSplits中的splits已经获取完了，且state为DISCOVERING_SPLITS的话，设置state为NO_MORE_SPLITS
            if (nextSplits.isLastBatch() && scheduleGroup.state == ScheduleGroupState.DISCOVERING_SPLITS) {
                scheduleGroup.state = ScheduleGroupState.NO_MORE_SPLITS;
            }
        }
        // nextSplitBatchFuture还不能获取SplitBatch，说明当前阻塞，将其添加到overallBlockedFutures中
        else {
            overallBlockedFutures.add(scheduleGroup.nextSplitBatchFuture);
            // 记录当前存在阻塞的SplitBatchFuture
            anyBlockedOnNextSplitBatch = true;
            // 直接跳出循环了，因为当前只会循环一次，等待下次调度
            continue;
        }
    }

    Multimap<Node, Split> splitAssignment = ImmutableMultimap.of();
    // 如果pendingSplits中有内容
    if (!pendingSplits.isEmpty()) {
        // 检查split和节点的分配关系是否计算完成，如果没有则跳出循环，等待下一次调度
        // scheduleGroup.placementFuture初始值为Futures.immediateFuture(null)，isDone直接返回true。
        // 第一次直接往下执行
        if (!scheduleGroup.placementFuture.isDone()) {
            continue;
        }

        if (state == State.INITIALIZED) {
            state = State.SPLITS_ADDED;
        }

        // 计算split分配到那些节点上
        SplitPlacementResult splitPlacementResult = splitPlacementPolicy.computeAssignments(pendingSplits);
        splitAssignment = splitPlacementResult.getAssignments();

        // remove splits with successful placements
        将已经计算完成的节点从pendingSplits中移除
        splitAssignment.values().forEach(pendingSplits::remove); // AbstractSet.removeAll performs terribly here.
        // 当前一共计算出来的split分配关系的数量
        overallSplitAssignmentCount += splitAssignment.size();

        // pendingSplits不为空，说明仍然有split的分配关系没有计算出来
        if (!pendingSplits.isEmpty()) {
            // 设置placementFuture的值为splitPlacementResult.getBlocked(),并放入overallBlockedFutures中
            scheduleGroup.placementFuture = splitPlacementResult.getBlocked();
            overallBlockedFutures.add(scheduleGroup.placementFuture);
            // 记录当前仍有阻塞的关系计算
            anyBlockedOnPlacements = true;
        }
    }

    // if no new splits will be assigned, update state and attach completion event
    Multimap<Node, Lifespan> noMoreSplitsNotification = ImmutableMultimap.of();
    // 如果所有的splits关系都计算完成了，设置调度组的状态为完成
    if (pendingSplits.isEmpty() && scheduleGroup.state == ScheduleGroupState.NO_MORE_SPLITS) {
        scheduleGroup.state = ScheduleGroupState.DONE;
        if (!lifespan.isTaskWide()) {
            Node node = ((FixedSplitPlacementPolicy) splitPlacementPolicy).getNodeForBucket(lifespan.getId());
            noMoreSplitsNotification = ImmutableMultimap.of(node, lifespan);
        }
    }

    // 将所有的splits根据计算出来的关系发送到相应的节点上创建任务并执行，将新创建的任务添加到overallNewTasks中。
    overallNewTasks.addAll(assignSplits(splitAssignment, noMoreSplitsNotification));

    // Assert that "placement future is not done" implies "pendingSplits is not empty".
    // The other way around is not true. One obvious reason is (un)lucky timing, where the placement is unblocked between `computeAssignments` and this line.
    // However, there are other reasons that could lead to this.
    // Note that `computeAssignments` is quite broken:
    // 1. It always returns a completed future when there are no tasks, regardless of whether all nodes are blocked.
    // 2. The returned future will only be completed when a node with an assigned task becomes unblocked. Other nodes don't trigger future completion.
    // As a result, to avoid busy loops caused by 1, we check pendingSplits.isEmpty() instead of placementFuture.isDone() here.
    if (scheduleGroup.nextSplitBatchFuture == null && scheduleGroup.pendingSplits.isEmpty() && scheduleGroup.state != ScheduleGroupState.DONE) {
        anyNotBlocked = true;
    }
}

if (autoDropCompletedLifespans) {
    drainCompletedLifespans();
}

// * `splitSource.isFinished` invocation may fail after `splitSource.close` has been invoked.
//   If state is NO_MORE_SPLITS/FINISHED, splitSource.isFinished has previously returned true, and splitSource is closed now.
// * Even if `splitSource.isFinished()` return true, it is not necessarily safe to tear down the split source.
//   * If anyBlockedOnNextSplitBatch is true, it means we have not checked out the recently completed nextSplitBatch futures,
//     which may contain recently published splits. We must not ignore those.
//   * If any scheduleGroup is still in DISCOVERING_SPLITS state, it means it hasn't realized that there will be no more splits.
//     Next time it invokes getNextBatch, it will realize that. However, the invocation will fail we tear down splitSource now.
if ((state == State.NO_MORE_SPLITS || state == State.FINISHED) || (scheduleGroups.isEmpty() && splitSource.isFinished())) {
    switch (state) {
        case INITIALIZED:
            // we have not scheduled a single split so far
            state = State.SPLITS_ADDED;
            ScheduleResult emptySplitScheduleResult = scheduleEmptySplit();
            overallNewTasks.addAll(emptySplitScheduleResult.getNewTasks());
            overallSplitAssignmentCount++;
            // fall through
        case SPLITS_ADDED:
            state = State.NO_MORE_SPLITS;
            splitSource.close();
            // fall through
        case NO_MORE_SPLITS:
            state = State.FINISHED;
            whenFinishedOrNewLifespanAdded.set(null);
            // fall through
        case FINISHED:
            return new ScheduleResult(
                    true,
                    overallNewTasks.build(),
                    overallSplitAssignmentCount);
        default:
            throw new IllegalStateException("Unknown state");
    }
}

if (anyNotBlocked) {
    return new ScheduleResult(false, overallNewTasks.build(), overallSplitAssignmentCount);
}

// Only try to finalize task creation when scheduling would block
overallNewTasks.addAll(finalizeTaskCreationIfNecessary());

ScheduleResult.BlockedReason blockedReason;
if (anyBlockedOnNextSplitBatch) {
    blockedReason = anyBlockedOnPlacements ? MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE : WAITING_FOR_SOURCE;
}
else {
    blockedReason = anyBlockedOnPlacements ? SPLIT_QUEUES_FULL : NO_ACTIVE_DRIVER_GROUP;
}

overallBlockedFutures.add(whenFinishedOrNewLifespanAdded);
return new ScheduleResult(
        false,
        overallNewTasks.build(),
        nonCancellationPropagating(whenAnyComplete(overallBlockedFutures)),
        blockedReason,
        overallSplitAssignmentCount);
}
```

根据计算出来的split关系创建任务：

```java
    private Set<RemoteTask> assignSplits(Multimap<Node, Split> splitAssignment, Multimap<Node, Lifespan> noMoreSplitsNotification)
{
ImmutableSet.Builder<RemoteTask> newTasks = ImmutableSet.builder();

ImmutableSet<Node> nodes = ImmutableSet.<Node>builder()
        .addAll(splitAssignment.keySet())
        .addAll(noMoreSplitsNotification.keySet())
        .build();
for (Node node : nodes) {
    // source partitioned tasks can only receive broadcast data; otherwise it would have a different distribution
    ImmutableMultimap<PlanNodeId, Split> splits = ImmutableMultimap.<PlanNodeId, Split>builder()
            .putAll(partitionedNode, splitAssignment.get(node))
            .build();
    ImmutableMultimap.Builder<PlanNodeId, Lifespan> noMoreSplits = ImmutableMultimap.builder();
    if (noMoreSplitsNotification.containsKey(node)) {
        noMoreSplits.putAll(partitionedNode, noMoreSplitsNotification.get(node));
    }
    newTasks.addAll(stage.scheduleSplits(
            node,
            splits,
            noMoreSplits.build()));
}
return newTasks.build();
}
```
