# presto worker原理

## 

presto worker 通过 TaskResource上的restful接口接收来自coordinator的请求:
```java
@POST
@Path("{taskId}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public Response createOrUpdateTask(@PathParam("taskId") TaskId taskId, TaskUpdateRequest taskUpdateRequest, @Context UriInfo uriInfo)
{
    requireNonNull(taskUpdateRequest, "taskUpdateRequest is null");

    Session session = taskUpdateRequest.getSession().toSession(sessionPropertyManager);
    TaskInfo taskInfo = taskManager.updateTask(session,
            taskId,
            taskUpdateRequest.getFragment(),
            taskUpdateRequest.getSources(),
            taskUpdateRequest.getOutputIds(),
            taskUpdateRequest.getTotalPartitions());

    if (shouldSummarize(uriInfo)) {
        taskInfo = taskInfo.summarize();
    }

    return Response.ok().entity(taskInfo).build();
}
```
该方法调用taskManager.updateTask:
```java
@Override
public TaskInfo updateTask(Session session, TaskId taskId, Optional<PlanFragment> fragment, List<TaskSource> sources, OutputBuffers outputBuffers, OptionalInt totalPartitions)
{
...
    SqlTask sqlTask = tasks.getUnchecked(taskId);
    sqlTask.recordHeartbeat();
    return sqlTask.updateTask(session, fragment, sources, outputBuffers, totalPartitions);
}
```
首先传入taskId从缓存中获取SqlTask(如果不存在则创建)，然后调用sqlTask.updateTask更新具体的task:
```java
public TaskInfo updateTask(Session session, Optional<PlanFragment> fragment, List<TaskSource> sources, OutputBuffers outputBuffers, OptionalInt totalPartitions)
{
        outputBuffer.setOutputBuffers(outputBuffers);

        // assure the task execution is only created once
        SqlTaskExecution taskExecution;
        synchronized (this) {
            // is task already complete?
            TaskHolder taskHolder = taskHolderReference.get();
            if (taskHolder.isFinished()) {
                return taskHolder.getFinalTaskInfo();
            }
            taskExecution = taskHolder.getTaskExecution();
            if (taskExecution == null) {
                checkState(fragment.isPresent(), "fragment must be present");
                taskExecution = sqlTaskExecutionFactory.create(session, queryContext, taskStateMachine, outputBuffer, fragment.get(), sources, totalPartitions);
                taskHolderReference.compareAndSet(taskHolder, new TaskHolder(taskExecution));
                needsPlan.set(false);
            }
        }

        if (taskExecution != null) {
            taskExecution.addSources(sources);
        }

    return getTaskInfo();
}
```
该方法首先检查taskHolder中有没有SqlTaskExecution，如果没有则创建taskExecution。
然后调用taskExecution.addSources方法

## SqlTaskExecution

sqlTask通过调用SqlTaskExecutionFactory.create创建taskExecution:
```java
    public SqlTaskExecution create(Session session, QueryContext queryContext, TaskStateMachine taskStateMachine, OutputBuffer outputBuffer, PlanFragment fragment, List<TaskSource> sources, OptionalInt totalPartitions)
    {
        boolean verboseStats = getVerboseStats(session);
        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                session,
                verboseStats,
                cpuTimerEnabled,
                totalPartitions);

        LocalExecutionPlan localExecutionPlan;
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskStateMachine.getTaskId())) {
            try {
                localExecutionPlan = planner.plan(
                        taskContext,
                        fragment.getRoot(),
                        TypeProvider.copyOf(fragment.getSymbols()),
                        fragment.getPartitioningScheme(),
                        fragment.getPipelineExecutionStrategy() == GROUPED_EXECUTION,
                        fragment.getPartitionedSources(),
                        outputBuffer);

                for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
                    Optional<PlanNodeId> sourceId = driverFactory.getSourceId();
                    if (sourceId.isPresent() && fragment.isPartitionedSources(sourceId.get())) {
                        checkArgument(fragment.getPipelineExecutionStrategy() == driverFactory.getPipelineExecutionStrategy(),
                                "Partitioned pipelines are expected to have the same execution strategy as the fragment");
                    }
                    else {
                        checkArgument(fragment.getPipelineExecutionStrategy() != UNGROUPED_EXECUTION || driverFactory.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION,
                                "When fragment execution strategy is ungrouped, all pipelines should have ungrouped execution strategy");
                    }
                }
            }
            catch (Throwable e) {
                // planning failed
                taskStateMachine.failed(e);
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
        return createSqlTaskExecution(
                taskStateMachine,
                taskContext,
                outputBuffer,
                sources,
                localExecutionPlan,
                taskExecutor,
                taskNotificationExecutor,
                queryMonitor);
    }
```
该方法分为两个步骤：
1. 调用LocalExecutionPlanner.plan创建localExecutionPlan。
2. 调用createSqlTaskExecution创建SqlTaskExecution

### create LocalExecutionPlan
```java
    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanNode plan,
            TypeProvider types,
            PartitioningScheme partitioningScheme,
            boolean planGrouped,
            List<PlanNodeId> partitionedSourceOrder,
            OutputBuffer outputBuffer)
    {
        List<Symbol> outputLayout = partitioningScheme.getOutputLayout();

        if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SCALED_WRITER_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(COORDINATOR_DISTRIBUTION)) {
            return plan(taskContext, planGrouped, plan, outputLayout, types, partitionedSourceOrder, new TaskOutputFactory(outputBuffer));
        }

        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        List<Integer> partitionChannels;
        List<Optional<NullableValue>> partitionConstants;
        List<Type> partitionChannelTypes;
        if (partitioningScheme.getHashColumn().isPresent()) {
            partitionChannels = ImmutableList.of(outputLayout.indexOf(partitioningScheme.getHashColumn().get()));
            partitionConstants = ImmutableList.of(Optional.empty());
            partitionChannelTypes = ImmutableList.of(BIGINT);
        }
        else {
            partitionChannels = ...
            partitionConstants = ...
            partitionChannelTypes = ...
        }
        PartitionFunction partitionFunction = nodePartitioningManager.getPartitionFunction(taskContext.getSession(), partitioningScheme, partitionChannelTypes);
        OptionalInt nullChannel = OptionalInt.empty();
        Set<Symbol> partitioningColumns = partitioningScheme.getPartitioning().getColumns();

        // partitioningColumns expected to have one column in the normal case, and zero columns when partitioning on a constant
        checkArgument(!partitioningScheme.isReplicateNullsAndAny() || partitioningColumns.size() <= 1);
        if (partitioningScheme.isReplicateNullsAndAny() && partitioningColumns.size() == 1) {
            nullChannel = OptionalInt.of(outputLayout.indexOf(getOnlyElement(partitioningColumns)));
        }

        return plan(
                taskContext,
                planGrouped,
                plan,
                outputLayout,
                types,
                partitionedSourceOrder,
                new PartitionedOutputFactory(
                        partitionFunction,
                        partitionChannels,
                        partitionConstants,
                        partitioningScheme.isReplicateNullsAndAny(),
                        nullChannel,
                        outputBuffer,
                        maxPagePartitioningBufferSize));
    }
```
该方法首先检查partitioningScheme.getPartitioning().getHandle()，如果分区方式是五种固定分区方式的话，
直接调用重载的plan方法创建LocalExecutionPlan，且最后一个参数为TaskOutputFactory。

否则则会构造PartitionedOutputFactory的各项参数，然后再调用plan，最后一个参数为PartitionedOutputFactory。

下面来看重载的plan方法：
```java
    public LocalExecutionPlan plan(
            TaskContext taskContext,
            boolean planGrouped,
            PlanNode plan,
            List<Symbol> outputLayout,
            TypeProvider types,
            List<PlanNodeId> partitionedSourceOrder,
            OutputFactory outputOperatorFactory)
    {
        Session session = taskContext.getSession();
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(taskContext, types);

        PhysicalOperation physicalOperation = plan.accept(new Visitor(session, planGrouped), context);

        Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(outputLayout, physicalOperation.getLayout());

        List<Type> outputTypes = outputLayout.stream()
                .map(types::get)
                .collect(toImmutableList());

        // 添加physicalOperation构造的DriverFactory
        context.addDriverFactory(
                context.isInputDriver(),
                true,
                ImmutableList.<OperatorFactory>builder()
                        .addAll(physicalOperation.getOperatorFactories())
                        .add(outputOperatorFactory.createOutputOperator(
                                context.getNextOperatorId(),
                                plan.getId(),
                                outputTypes,
                                pagePreprocessor,
                                new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session))))
                        .build(),
                context.getDriverInstanceCount(),
                physicalOperation.getPipelineExecutionStrategy());

        addLookupOuterDrivers(context);

        // notify operator factories that planning has completed
        context.getDriverFactories().stream()
                .map(DriverFactory::getOperatorFactories)
                .flatMap(List::stream)
                .filter(LocalPlannerAware.class::isInstance)
                .map(LocalPlannerAware.class::cast)
                .forEach(LocalPlannerAware::localPlannerComplete);

        return new LocalExecutionPlan(context.getDriverFactories(), partitionedSourceOrder);
    }
```
该方法首先使用内部类Visitor访问PlanNode，输出两个参数，一个是返回值physicalOperation，一个通过参数context传出。

1. physicalOperation

这两个参数分别传出什么副作用呢？首先看返回值physicalOperation:
```java
context.addDriverFactory(
        context.isInputDriver(),
        true,
        ImmutableList.<OperatorFactory>builder()
                .addAll(physicalOperation.getOperatorFactories())
                .add(outputOperatorFactory.createOutputOperator(
                        context.getNextOperatorId(),
                        plan.getId(),
                        outputTypes,
                        pagePreprocessor,
                        new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session))))
                .build(),
        context.getDriverInstanceCount(),
        physicalOperation.getPipelineExecutionStrategy());
```
可以看到后面会调用context.addDriverFactory将physicalOperation中的operatorFactories也包装进一个DriverFactory添加进context中，
physicalOperation中的operatorFactoreis是怎么生成的呢？以visitLimit为例:
```java
@Override
public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
{
    PhysicalOperation source = node.getSource().accept(this, context);

    OperatorFactory operatorFactory = new LimitOperatorFactory(context.getNextOperatorId(), node.getId(), node.getCount());
    return new PhysicalOperation(operatorFactory, source.getLayout(), context, source);
}
```
该方法先用同样的Visitor访问LimitNode的数据源结点，获取sourceNode的physicalOperation，然后创建LimitNode的operatorFacotry，
最后调用PhysicalOperation构造方法返回。

再看PhysicalOperation构造方法中是怎么构造operatorFactories的：
```java
public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, LocalExecutionPlanContext context, PhysicalOperation source)
{
    this(operatorFactory, layout, context, Optional.of(requireNonNull(source, "source is null")), source.getPipelineExecutionStrategy());
}

private PhysicalOperation(
        OperatorFactory operatorFactory,
        Map<Symbol, Integer> layout,
        LocalExecutionPlanContext context,
        Optional<PhysicalOperation> source,
        PipelineExecutionStrategy pipelineExecutionStrategy)
{
...
    this.operatorFactories = ImmutableList.<OperatorFactory>builder()
            .addAll(source.map(PhysicalOperation::getOperatorFactories).orElse(ImmutableList.of()))
            .add(operatorFactory)
            .build();
    this.layout = ImmutableMap.copyOf(layout);
    this.types = toTypes(layout, context);
    this.pipelineExecutionStrategy = pipelineExecutionStrategy;
}
```
该方法首先将source节点中的所有operatorFactor添加到列表中，再将当前节点的operatorFactory添加到列表中。
那么最终的operatorFactories就是从最源头节点对应的operatorFactory一直到当前节点对应的operatorFactory

2. context

context的主要作用是在Visitor内部调用addDriverFactory。在访问某些类型的节点时，可能存在节点的数据源不能通过operator获取，


然后调用context.addDriverFactory添加新的DriverFactory。

最终调用LocalExecutionPlan的构造方法，传入context.getDriverFactories()，也就是所有的DriverFactory。

我们可以看到，最终的LocalExecutionPlan里面传入的是一个DriverFactory的链表，
每个DriverFactory中又有一个OperatorFactory的链表，最终我们执行的是OperatorFactory创建的Operator。

我们再看官方文档中对于Driver的介绍：

Driver
Tasks contain one or more parallel drivers. Drivers act upon data and combine operators to produce output that is then aggregated by a task and then delivered to another task in a another stage. A driver is a sequence of operator instances, or you can think of a driver as a physical set of operators in memory. It is the lowest level of parallelism in the Presto architecture. A driver has one input and one output.


可以看到Task中包括多个并行的Driver，向Driver输入数据，经过其中operators的处理，最终输出数据被task整合然后输入到其他Stage的task中。
