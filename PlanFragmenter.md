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
1. 调用SimplePlanRewriter.rewriteWith，使用Fragmenter这个Visitor将PlanNode重写，修改其中一些内容；
2. 调用fragmenter.buildRootFragment构建SubPlan；

