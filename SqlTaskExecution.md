# SqlTaskExecution

SqlTaskExecution是SqlTask的核心，下面分析SqlTaskExecution构造方法：

```java
private SqlTaskExecution(
        TaskStateMachine taskStateMachine,
        TaskContext taskContext,
        OutputBuffer outputBuffer,
        LocalExecutionPlan localExecutionPlan,
        TaskExecutor taskExecutor,
        QueryMonitor queryMonitor,
        Executor notificationExecutor)
{
    this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
    this.taskId = taskStateMachine.getTaskId();
    this.taskContext = requireNonNull(taskContext, "taskContext is null");
    this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");

    this.taskExecutor = requireNonNull(taskExecutor, "driverExecutor is null");
    this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");

    this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null")
    
    // 这里将driverFactories按照生命周期分为三类
    // driverRunnerFactoriesWithSplitLifeCycle
    // driverRunnerFactoriesWithTaskLifeCycle
    // driverRunnerFactoriesWithDriverGroupLifeCycle
    try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
        // index driver factories
        Set<PlanNodeId> partitionedSources = ImmutableSet.copyOf(localExecutionPlan.getPartitionedSourceOrder());
        ImmutableMap.Builder<PlanNodeId, DriverSplitRunnerFactory> driverRunnerFactoriesWithSplitLifeCycle = ImmutableMap.builder();
        ImmutableList.Builder<DriverSplitRunnerFactory> driverRunnerFactoriesWithTaskLifeCycle = ImmutableList.builder();
        ImmutableList.Builder<DriverSplitRunnerFactory> driverRunnerFactoriesWithDriverGroupLifeCycle = ImmutableList.builder();
        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            Optional<PlanNodeId> sourceId = driverFactory.getSourceId();
            
            if (sourceId.isPresent() && partitionedSources.contains(sourceId.get())) {
                driverRunnerFactoriesWithSplitLifeCycle.put(sourceId.get(), new DriverSplitRunnerFactory(driverFactory));
            }
            else {
                switch (driverFactory.getPipelineExecutionStrategy()) {
                    case GROUPED_EXECUTION:
                        driverRunnerFactoriesWithDriverGroupLifeCycle.add(new DriverSplitRunnerFactory(driverFactory));
                        break;
                    case UNGROUPED_EXECUTION:
                        driverRunnerFactoriesWithTaskLifeCycle.add(new DriverSplitRunnerFactory(driverFactory));
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
        }
        this.driverRunnerFactoriesWithSplitLifeCycle = driverRunnerFactoriesWithSplitLifeCycle.build();
        this.driverRunnerFactoriesWithDriverGroupLifeCycle = driverRunnerFactoriesWithDriverGroupLifeCycle.build();
        this.driverRunnerFactoriesWithTaskLifeCycle = driverRunnerFactoriesWithTaskLifeCycle.build();

        this.pendingSplitsByPlanNode = this.driverRunnerFactoriesWithSplitLifeCycle.keySet().stream()
                .collect(toImmutableMap(identity(), ignore -> new PendingSplitsForPlanNode()));
        this.status = new Status(
                taskContext,
                localExecutionPlan.getDriverFactories().stream()
                        .collect(toImmutableMap(DriverFactory::getPipelineId, DriverFactory::getPipelineExecutionStrategy)));
        this.schedulingLifespanManager = new SchedulingLifespanManager(localExecutionPlan.getPartitionedSourceOrder(), this.status);

        checkArgument(this.driverRunnerFactoriesWithSplitLifeCycle.keySet().equals(partitionedSources),
                "Fragment is partitioned, but not all partitioned drivers were found");

        // Pre-register Lifespans for ungrouped partitioned drivers in case they end up get no splits.
        for (Entry<PlanNodeId, DriverSplitRunnerFactory> entry : this.driverRunnerFactoriesWithSplitLifeCycle.entrySet()) {
            PlanNodeId planNodeId = entry.getKey();
            DriverSplitRunnerFactory driverSplitRunnerFactory = entry.getValue();
            if (driverSplitRunnerFactory.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION) {
                schedulingLifespanManager.addLifespanIfAbsent(Lifespan.taskWide());
                this.pendingSplitsByPlanNode.get(planNodeId).getLifespan(Lifespan.taskWide());
            }
        }

        // don't register the task if it is already completed (most likely failed during planning above)
        if (!taskStateMachine.getState().isDone()) {
            taskHandle = taskExecutor.addTask(taskId, outputBuffer::getUtilization, getInitialSplitsPerNode(taskContext.getSession()), getSplitConcurrencyAdjustmentInterval(taskContext.getSession()));
            taskStateMachine.addStateChangeListener(state -> {
                if (state.isDone()) {
                    taskExecutor.removeTask(taskHandle);
                    for (DriverFactory factory : localExecutionPlan.getDriverFactories()) {
                        factory.noMoreDrivers();
                    }
                }
            });
        }
        else {
            taskHandle = null;
        }

        outputBuffer.addStateChangeListener(new CheckTaskCompletionOnBufferFinish(SqlTaskExecution.this));
    }
}
```
