# presto StageLinkage

在SqlQueryScheduler.createStages中调用
```java
stageLinkages.put(stageId, new StageLinkage(plan.getFragment().getId(), parent, childStages));
```
创建stageLinkages，也就是Stage之间的数据链接，保证子Stage执行完成后数据流进入哪一个父Stage。

构造函数:
```java
// fragementId: 父Stage的fragementId
// parent 更新父Stage的数据源的回调函数
// children 子Stage集合
public StageLinkage(PlanFragmentId fragmentId, ExchangeLocationsConsumer parent, Set<SqlStageExecution> children)
{
    this.currentStageFragmentId = fragmentId;
    this.parent = parent;
    this.childOutputBufferManagers = children.stream()
            .map(childStage -> {
                PartitioningHandle partitioningHandle = childStage.getFragment().getPartitioningScheme().getPartitioning().getHandle();
                if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
                    return new BroadcastOutputBufferManager(childStage::setOutputBuffers);
                }
                else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                    return new ScaledOutputBufferManager(childStage::setOutputBuffers);
                }
                else {
                    int partitionCount = Ints.max(childStage.getFragment().getPartitioningScheme().getBucketToPartition().get()) + 1;
                    return new PartitionedOutputBufferManager(partitioningHandle, partitionCount, childStage::setOutputBuffers);
                }
            })
            .collect(toImmutableSet());

    this.childStageIds = children.stream()
            .map(SqlStageExecution::getStageId)
            .collect(toImmutableSet());
}
```

在SqlQueryScheduler.schedule中会调用到：

```java
// result.getNewTasks() 返回本次调度新创建的tasks
stageLinkages.get(stage.getStageId()).processScheduleResults(stage.getState(), result.getNewTasks());

public void processScheduleResults(StageState newState, Set<RemoteTask> newTasks)
{
    boolean noMoreTasks = false;
    switch (newState) {
        case PLANNED:
        case SCHEDULING:
            // workers are still being added to the query
            break;
        case SCHEDULING_SPLITS:
        case SCHEDULED:
        case RUNNING:
        case FINISHED:
        case CANCELED:
            // no more workers will be added to the query
            noMoreTasks = true;
        case ABORTED:
        case FAILED:
            // DO NOT complete a FAILED or ABORTED stage.  This will cause the
            // stage above to finish normally, which will result in a query
            // completing successfully when it should fail..
            break;
    }

    // Add an exchange location to the parent stage for each new task
    // 将newTasks的exchange地址添加到parentStage中（回调到task.addSplits触发父Stage执行）
    parent.addExchangeLocations(currentStageFragmentId, newTasks, noMoreTasks);

    if (!childOutputBufferManagers.isEmpty()) {
        // Add an output buffer to the child stages for each new task
        List<OutputBufferId> newOutputBuffers = newTasks.stream()
                .map(task -> new OutputBufferId(task.getTaskId().getId()))
                .collect(toImmutableList());
        for (OutputBufferManager child : childOutputBufferManagers) {
            child.addOutputBuffers(newOutputBuffers, noMoreTasks);
        }
    }
}

```
该方法主要做两件事：
1. 调用 parent.addExchangeLocations将子Stage的Exchange地址添加到父节点中

2. 调用child.addOutputBuffers设置子Stage的输出缓冲区



