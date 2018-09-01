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
结果是一个空Map，也就是说对于上层的StageExecutionPlan，其splitSources为空，因为其数据是从其他节点来的，而不是split。

## SqlQueryScheduler
StageExecutionPlan生成后，会构造一个SqlQueryScheduler对象:
    
```java
public SqlQueryScheduler(QueryStateMachine queryStateMachine,
        LocationFactory locationFactory,
        StageExecutionPlan plan,
        NodePartitioningManager nodePartitioningManager,
        NodeScheduler nodeScheduler,
        RemoteTaskFactory remoteTaskFactory,
        Session session,
        boolean summarizeTaskInfo,
        int splitBatchSize,
        ExecutorService queryExecutor,
        ScheduledExecutorService schedulerExecutor,
        FailureDetector failureDetector,
        OutputBuffers rootOutputBuffers,
        NodeTaskMap nodeTaskMap,
        ExecutionPolicy executionPolicy,
        SplitSchedulerStats schedulerStats)
{
...
    List<SqlStageExecution> stages = createStages(FixedSourcePartitionedScheduler
            (fragmentId, tasks, noMoreExchangeLocations) -> updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations),
            new AtomicInteger(),
            locationFactory,
            plan.withBucketToPartition(Optional.of(new int[1])),
            nodeScheduler,
            remoteTaskFactory,
            session,
            splitBatchSize,
            partitioningHandle -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle)),
            nodePartitioningManager,
            queryExecutor,
            schedulerExecutor,
            failureDetector,
            nodeTaskMap,
            stageSchedulers,
            stageLinkages);

    SqlStageExecution rootStage = stages.get(0);
    rootStage.setOutputBuffers(rootOutputBuffers);
    this.rootStageId = rootStage.getStageId();

    this.stages = stages.stream()
            .collect(toImmutableMap(SqlStageExecution::getStageId, identity()));

    this.stageSchedulers = stageSchedulers.build();
    this.stageLinkages = stageLinkages.build();
    ...
    }
```
构造函数中调用createStages，主要完成三件事

1. 创建List<SqlStageExecution> stages；

2. 创建stageSchedulers；

3. 创建(stageLinkages)[StageLinkage]；

```java
    private List<SqlStageExecution> createStages(
            ExchangeLocationsConsumer parent,
            AtomicInteger nextStageId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            int splitBatchSize,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            NodePartitioningManager nodePartitioningManager,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers,
            ImmutableMap.Builder<StageId, StageLinkage> stageLinkages)
    {
        // 最终返回的stages列表builder
        ImmutableList.Builder<SqlStageExecution> stages = ImmutableList.builder();

        StageId stageId = new StageId(queryStateMachine.getQueryId(), nextStageId.getAndIncrement());
        // 创建根Fragement的SqlStageExecution对象
        SqlStageExecution stage = new SqlStageExecution(
                stageId,
                locationFactory.createStageLocation(stageId),
                plan.getFragment(),
                remoteTaskFactory,
                session,
                summarizeTaskInfo,
                nodeTaskMap,
                queryExecutor,
                failureDetector,
                schedulerStats);

        stages.add(stage);

        Optional<int[]> bucketToPartition;
        // 根Fragement的分区类型
        PartitioningHandle partitioningHandle = plan.getFragment().getPartitioning();
        // 如果分区类型为SOURCE_DISTRIBUTION，则调用simpleSourcePartitionedScheduler方法创建调度器
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
            // 保存stageId对应的调度器
            stageSchedulers.put(stageId, simpleSourcePartitionedScheduler(stage, planNodeId, splitSource, placementPolicy, splitBatchSize));
            bucketToPartition = Optional.of(new int[1]);
        }
        // 这里的调度器在最后处理
        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            bucketToPartition = Optional.of(new int[1]);
        }
        else {
            // nodes are pre determined by the nodePartitionMap
            NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
            long nodeCount = nodePartitionMap.getPartitionToNode().values().stream().distinct().count();
            OptionalInt concurrentLifespansPerTask = getConcurrentLifespansPerNode(session);

            Map<PlanNodeId, SplitSource> splitSources = plan.getSplitSources();
            // 如果splitSources不为空，说明有TableScanNode，则创建FixedSourcePartitionedScheduler调度器。
            if (!splitSources.isEmpty()) {
                List<PlanNodeId> schedulingOrder = plan.getFragment().getPartitionedSources();
                List<ConnectorPartitionHandle> connectorPartitionHandles;
                switch (plan.getFragment().getPipelineExecutionStrategy()) {
                    case GROUPED_EXECUTION:
                        connectorPartitionHandles = nodePartitioningManager.listPartitionHandles(session, partitioningHandle);
                        checkState(connectorPartitionHandles.size() == nodePartitionMap.getBucketToPartition().length);
                        checkState(!ImmutableList.of(NOT_PARTITIONED).equals(connectorPartitionHandles));
                        break;
                    case UNGROUPED_EXECUTION:
                        connectorPartitionHandles = ImmutableList.of(NOT_PARTITIONED);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
                stageSchedulers.put(stageId, new FixedSourcePartitionedScheduler(
                        stage,
                        splitSources,
                        plan.getFragment().getPipelineExecutionStrategy(),
                        schedulingOrder,
                        nodePartitionMap,
                        splitBatchSize,
                        concurrentLifespansPerTask.isPresent() ? OptionalInt.of(toIntExact(concurrentLifespansPerTask.getAsInt() * nodeCount)) : OptionalInt.empty(),
                        nodeScheduler.createNodeSelector(null),
                        connectorPartitionHandles));
                bucketToPartition = Optional.of(nodePartitionMap.getBucketToPartition());
            }
            else {
                // 否则创建FixedCountScheduler调度器
                Map<Integer, Node> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                stageSchedulers.put(stageId, new FixedCountScheduler(stage, partitionToNode));
                bucketToPartition = Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }
        // 这里递归调用createStages对所有子subPlan处理
        ImmutableSet.Builder<SqlStageExecution> childStagesBuilder = ImmutableSet.builder();
        for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
            List<SqlStageExecution> subTree = createStages(
                    stage::addExchangeLocations,
                    nextStageId,
                    locationFactory,
                    subStagePlan.withBucketToPartition(bucketToPartition),
                    nodeScheduler,
                    remoteTaskFactory,
                    session,
                    splitBatchSize,
                    partitioningCache,
                    nodePartitioningManager,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    stageSchedulers,
                    stageLinkages);
            // 将生层的SqlStageExecution同样加入stages列表
            stages.addAll(subTree);

            SqlStageExecution childStage = subTree.get(0);
            childStagesBuilder.add(childStage);
        }
        Set<SqlStageExecution> childStages = childStagesBuilder.build();
        stage.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                childStages.forEach(SqlStageExecution::cancel);
            }
        });

        // 创建stageLinkages，用于建立父子Stage之间的数据流
        stageLinkages.put(stageId, new StageLinkage(plan.getFragment().getId(), parent, childStages));

        // 这里处理分区类型为SCALED_WRITER_DISTRIBUTION的情况，创建类型为 ScaledWriterScheduler
        if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStages.stream()
                    .map(SqlStageExecution::getAllTasks)
                    .flatMap(Collection::stream)
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            Supplier<Collection<TaskStatus>> writerTasksProvider = () -> stage.getAllTasks().stream()
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                    stage,
                    sourceTasksProvider,
                    writerTasksProvider,
                    nodeScheduler.createNodeSelector(null),
                    schedulerExecutor,
                    getWriterMinSize(session));
            whenAllStages(childStages, StageState::isDone)
                    .addListener(scheduler::finish, directExecutor());
            stageSchedulers.put(stageId, scheduler);
        }

        return stages.build();
    }
```


最终SqlQueryExecution.start执行时，会将SqlQueryScheduler提交到线程池中执行,
最终调用SqlQueryScheduler.schedule方法:

```java
private void schedule()
{
    try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
        Set<StageId> completedStages = new HashSet<>();
        ExecutionSchedule executionSchedule = executionPolicy.createExecutionSchedule(stages.values());
        while (!executionSchedule.isFinished()) {
            List<ListenableFuture<?>> blockedStages = new ArrayList<>();
            for (SqlStageExecution stage : executionSchedule.getStagesToSchedule()) {
                stage.beginScheduling();

                // perform some scheduling work
                ScheduleResult result = stageSchedulers.get(stage.getStageId())
                        .schedule();

                // modify parent and children based on the results of the scheduling
                if (result.isFinished()) {
                    stage.schedulingComplete();
                }
                else if (!result.getBlocked().isDone()) {
                    blockedStages.add(result.getBlocked());
                }
                stageLinkages.get(stage.getStageId())
                        .processScheduleResults(stage.getState(), result.getNewTasks());
                schedulerStats.getSplitsScheduledPerIteration().add(result.getSplitsScheduled());
                if (result.getBlockedReason().isPresent()) {
                    switch (result.getBlockedReason().get()) {
                        case WRITER_SCALING:
                            // no-op
                            break;
                        case WAITING_FOR_SOURCE:
                            schedulerStats.getWaitingForSource().update(1);
                            break;
                        case SPLIT_QUEUES_FULL:
                            schedulerStats.getSplitQueuesFull().update(1);
                            break;
                        case MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE:
                        case NO_ACTIVE_DRIVER_GROUP:
                            break;
                        default:
                            throw new UnsupportedOperationException("Unknown blocked reason: " + result.getBlockedReason().get());
                    }
                }
            }

            // make sure to update stage linkage at least once per loop to catch async state changes (e.g., partial cancel)
            for (SqlStageExecution stage : stages.values()) {
                if (!completedStages.contains(stage.getStageId()) && stage.getState().isDone()) {
                    stageLinkages.get(stage.getStageId())
                            .processScheduleResults(stage.getState(), ImmutableSet.of());
                    completedStages.add(stage.getStageId());
                }
            }

            // wait for a state change and then schedule again
            if (!blockedStages.isEmpty()) {
                try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                    tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                }
                for (ListenableFuture<?> blockedStage : blockedStages) {
                    blockedStage.cancel(true);
                }
            }
        }

        for (SqlStageExecution stage : stages.values()) {
            StageState state = stage.getState();
            if (state != SCHEDULED && state != RUNNING && !state.isDone()) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage %s is in state %s", stage.getStageId(), state));
            }
        }
    }
    catch (Throwable t) {
        queryStateMachine.transitionToFailed(t);
        throw t;
    }
    finally {
        RuntimeException closeError = new RuntimeException();
        for (StageScheduler scheduler : stageSchedulers.values()) {
            try {
                scheduler.close();
            }
            catch (Throwable t) {
                queryStateMachine.transitionToFailed(t);
                // Self-suppression not permitted
                if (closeError != t) {
                    closeError.addSuppressed(t);
                }
            }
        }
        if (closeError.getSuppressed().length > 0) {
            throw closeError;
        }
    }
}
```
这里最关键的一步就是找到stage关联的调度器并开始调度:
`stageSchedulers.get(stage.getStageId()).schedule();`
