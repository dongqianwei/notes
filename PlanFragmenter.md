# Plan Fragment

## Plan Fragment introduction

在SqlQueryExecution的doAnalyzeQuery方法中：
```java
private PlanRoot doAnalyzeQuery()
{
...
    LogicalPlanner logicalPlanner = new LogicalPlanner(stateMachine.getSession(), planOptimizers, idAllocator, metadata, sqlParser);
    Plan plan = logicalPlanner.plan(analysis);
...
    // fragment the plan
    SubPlan fragmentedPlan = PlanFragmenter.createSubPlans(stateMachine.getSession(), metadata, nodePartitioningManager, plan, false);
...
    return new PlanRoot(fragmentedPlan, !explainAnalyze, extractConnectors(analysis));
}
```
实际上包含两个步骤：

1. logicalPlanner.plan方法将Statement解析为初步的PlanNode(Plan{PlanNode})

相关代码在[presto_SqlQueryExecution](presto_sqlqueryexecution)中解读

2. PlanFragmenter.createSubPlans将PlanNode分片，输出为SubPlan

SubPlan类是一个树状结构：
```java
public class SubPlan
{
    private final PlanFragment fragment;
    private final List<SubPlan> children;
}
```
其中PlanFragment中保存了当前Fragement的PlanNode,SubPlan和其成员children形成了树状结构，
并且当前subplan和其children为依赖关系，即只有children的subplan执行完成后才能执行当前subplan。

## Plan Fragment implementation
PlanNode Fragment 实现代码位于 PlanFragmenter.createSubPlans 中：
```java
public static SubPlan createSubPlans(Session session, Metadata metadata, NodePartitioningManager nodePartitioningManager, Plan plan, boolean forceSingleNode)
{
    Fragmenter fragmenter = new Fragmenter(session, metadata, plan.getTypes());

    FragmentProperties properties = new FragmentProperties(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getRoot().getOutputSymbols()));
    if (forceSingleNode || isForceSingleNodeOutput(session)) {
        properties = properties.setSingleNodeDistribution();
    }
    // step 1
    PlanNode root = SimplePlanRewriter.rewriteWith(fragmenter, plan.getRoot(), properties);

    // step 2
    SubPlan subPlan = fragmenter.buildRootFragment(root, properties);
    subPlan = analyzeGroupedExecution(session, metadata, nodePartitioningManager, subPlan);

    checkState(!isForceSingleNodeOutput(session) || subPlan.getFragment().getPartitioning().isSingleNode(), "Root of PlanFragment is not single node");
    subPlan.sanityCheck();

    return subPlan;
}
```
大致上分两个步骤：
1. 调用SimplePlanRewriter.rewriteWith，使用Fragmenter这个Visitor将PlanNode重写;

Fragmenter Visitor中实际对PlanNode重写的方法为visitExchange:
```java
public PlanNode visitExchange(ExchangeNode exchange, RewriteContext<FragmentProperties> context)
{
    if (exchange.getScope() != REMOTE) {
        return context.defaultRewrite(exchange, context.get());
    }
    ...
    ImmutableList.Builder<SubPlan> builder = ImmutableList.builder();
    for (int sourceIndex = 0; sourceIndex < exchange.getSources().size(); sourceIndex++) {
        FragmentProperties childProperties = new FragmentProperties(partitioningScheme.translateOutputLayout(exchange.getInputs().get(sourceIndex)));
        builder.add(buildSubPlan(exchange.getSources().get(sourceIndex), childProperties, context));
    }

    List<SubPlan> children = builder.build();
    context.get().addChildren(children);

    List<PlanFragmentId> childrenIds = children.stream()
            .map(SubPlan::getFragment)
            .map(PlanFragment::getId)
            .collect(toImmutableList());

    return new RemoteSourceNode(exchange.getId(), childrenIds, exchange.getOutputSymbols(), exchange.getOrderingScheme());
}
```
该方法只处理scope为REMOTE的exchange，即需要从其他节点收集数据的exchange。

该方法主要分为两个步骤:
* 遍历exchangeNode的所有source节点，对每个节点创建新的SubPlan，然后将生成的SubPlan列表保存到当前节点对应的FragmentProperties中.

创建subPlan代码如下：
```java
private SubPlan buildSubPlan(PlanNode node, FragmentProperties properties, RewriteContext<FragmentProperties> context)
{
    PlanFragmentId planFragmentId = nextFragmentId();
    PlanNode child = context.rewrite(node, properties);
    return buildFragment(child, properties, planFragmentId);
}
```


* 创建并返回RemoteSourceNode，也就是将当前的ExchangeNode替换为了RemoteSourceNode.

2. 调用fragmenter.buildRootFragment构建SubPlan;

该方法的具体实现:
```java
private SubPlan buildFragment(PlanNode root, FragmentProperties properties, PlanFragmentId fragmentId)
{
    Set<Symbol> dependencies = SymbolsExtractor.extractOutputSymbols(root);

    List<PlanNodeId> schedulingOrder = scheduleOrder(root);
    boolean equals = properties.getPartitionedSources().equals(ImmutableSet.copyOf(schedulingOrder));
    checkArgument(equals, "Expected scheduling order (%s) to contain an entry for all partitioned sources (%s)", schedulingOrder, properties.getPartitionedSources());

    PlanFragment fragment = new PlanFragment(
            fragmentId,
            root,
            Maps.filterKeys(types.allTypes(), in(dependencies)),
            properties.getPartitioningHandle(),
            schedulingOrder,
            properties.getPartitioningScheme(),
            UNGROUPED_EXECUTION);

    return new SubPlan(fragment, properties.getChildren());
}
```
用当前的root节点创建fragment，然后创建SubPlan，传入root节点的fragment以及properties.getChildren().
properties.getChildren()返回的是之前visitExchange时所有source创建的SubPlan，这样就构建出了一个树状结构的SubPlan.

这个算法最核心的地方在于，原先的PlanNode节点也是一个树状结构，在经过Fragement步骤后，所有的REMOTE Exchange节点都被切割开了，
用一个RemoteSourceNode代替，同时被包装进一个PlanFragement结构中，被分割开的Exchange的source节点都被包装进各自的SubPlan节点最终形成一个SunPlan树状结构，该结构实际上表示一个SubPlan在分布式的环境中PlanNode之间的跨机器的依赖关系。
