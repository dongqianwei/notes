# when does queryExecution start?
in `SqlQueryManager.createQueryinternal`, after queryExecution is created, at the last line, queryExecution is submited:
```java
resourceGroupManager.submit(statement, queryExecution, selectionContext, queryExecutor);
```
and submit implementation:
```java
public void submit(Statement statement, QueryExecution queryExecution, SelectionContext<C> selectionContext, Executor executor)
{
    checkState(configurationManager.get() != null, "configurationManager not set");
    createGroupIfNecessary(selectionContext, executor);
    groups.get(selectionContext.getResourceGroupId()).run(queryExecution);
}
```
groups's type is `ConcurrentMap<ResourceGroupId, InternalResourceGroup>`, so it calls `internalResourceGroup.run`:
```java
public void run(QueryExecution query) {
    ...
    startInBackground(query);
    ...
}
```
```java
private void startInBackground(QueryExecution query)
{
    ...
    executor.execute(query::start);
    ...
}
```
so at last, `queryExecution` is submitted to a `ThreadPool` and start and do real business.

# How `SqlQueryExecution` works
```java
...
PlanRoot plan = analyzeQuery();
metadata.beginQuery(getSession(), plan.getConnectors());
// plan distribution of query
planDistribution(plan);
// transition to starting
if (!stateMachine.transitionToStarting()) {
    // query already started or finished
    return;
}
// if query is not finished, start the scheduler, otherwise cancel it
SqlQueryScheduler scheduler = queryScheduler.get();
if (!stateMachine.isDone()) {
    scheduler.start();
}
...
```
`analyzeQuery` returns `PlanRoot`
`PlanRoot` is the root to reference the whole Plan, the structure is as following:

```
PlanRoot
-->SubPlan
-->-->PlanNode
```
and PlanNode is the base class of all actural XXXPlanNode(`TableScanNode`,`AggregationNode`...)
`SubPlan` has children subPlans and the whole structure will be a Plan Tree with all leafs of type PlanNode.

analyzeQuery calls doAnalyzeQuery and to actual business:
```java
private PlanRoot doAnalyzeQuery()
{
    ...
    // plan query
    PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    LogicalPlanner logicalPlanner = new LogicalPlanner(stateMachine.getSession(), planOptimizers, idAllocator, metadata, sqlParser);
    Plan plan = logicalPlanner.plan(analysis);
    ...

    // fragment the plan
    SubPlan fragmentedPlan = PlanFragmenter.createSubPlans(stateMachine.getSession(), metadata, nodePartitioningManager, plan, false);
    ...

    boolean explainAnalyze = analysis.getStatement() instanceof Explain && ((Explain) analysis.getStatement()).isAnalyze();
    return new PlanRoot(fragmentedPlan, !explainAnalyze, extractConnectors(analysis));
}
```
the function does two things, first calls `LogicalPlanner.plan` to generate the plan,
then calls lanFragmenter.createSubPlans to calculate fragmentedPlan.

> fragement determines which groupe of plans are executed on a same worker

1. `LogicalPlanner.plan`
```java
public Plan plan(Analysis analysis, Stage stage)
{
    PlanNode root = planStatement(analysis, analysis.getStatement());

    planSanityChecker.validateIntermediatePlan(root, session, metadata, sqlParser, symbolAllocator.getTypes());

    if (stage.ordinal() >= Stage.OPTIMIZED.ordinal()) {
        for (PlanOptimizer optimizer : planOptimizers) {
            root = optimizer.optimize(root, session, symbolAllocator.getTypes(), symbolAllocator, idAllocator);
            requireNonNull(root, format("%s returned a null plan", optimizer.getClass().getName()));
        }
    }

    if (stage.ordinal() >= Stage.OPTIMIZED_AND_VALIDATED.ordinal()) {
        // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
        planSanityChecker.validateFinalPlan(root, session, metadata, sqlParser, symbolAllocator.getTypes());
    }

    return new Plan(root, symbolAllocator.getTypes());
}
```
this function first calls planStatement to generate the original PlanNode,
and then use optimizers to optimizes the PlanNode if needed,
and then validates the final plan if needed.

1.1 `planStatement`
```java
public PlanNode planStatement(Analysis analysis, Statement statement)
{
    if (statement instanceof CreateTableAsSelect && analysis.isCreateTableAsSelectNoOp()) {
        checkState(analysis.getCreateTableDestination().isPresent(), "Table destination is missing");
        Symbol symbol = symbolAllocator.newSymbol("rows", BIGINT);
        PlanNode source = new ValuesNode(idAllocator.getNextId(), ImmutableList.of(symbol), ImmutableList.of(ImmutableList.of(new LongLiteral("0"))));
        return new OutputNode(idAllocator.getNextId(), source, ImmutableList.of("rows"), ImmutableList.of(symbol));
    }
    return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
}
```
first, the function detect if statement is instance of CreateTableAsSelect and if the statement contains "if not exists"
and the table already exists(this is detected in the analysis stage).

if condition is true, then nothing needs to be done and an empty PlanNode is returned.
otherwise, it calls planStatementWithoutOutput to generate the PlanNode.
```java
private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement)
{
    if (statement instanceof CreateTableAsSelect) {
        if (analysis.isCreateTableAsSelectNoOp()) {
            throw new PrestoException(NOT_SUPPORTED, "CREATE TABLE IF NOT EXISTS is not supported in this context " + statement.getClass().getSimpleName());
        }
        return createTableCreationPlan(analysis, ((CreateTableAsSelect) statement).getQuery());
    }
    else if (statement instanceof Insert) {
        checkState(analysis.getInsert().isPresent(), "Insert handle is missing");
        return createInsertPlan(analysis, (Insert) statement);
    }
    else if (statement instanceof Delete) {
        return createDeletePlan(analysis, (Delete) statement);
    }
    else if (statement instanceof Query) {
        return createRelationPlan(analysis, (Query) statement);
    }
    else if (statement instanceof Explain && ((Explain) statement).isAnalyze()) {
        return createExplainAnalyzePlan(analysis, (Explain) statement);
    }
    else {
        throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type " + statement.getClass().getSimpleName());
    }
}
```
the function checks statement type and use different function to generate PlanNode,
1.1.1 `createTableCreationPlan`
the first branch detects if statement type is `CreateTableAsSelect`,
it calls `createTableCreationPlan` and passes the Query Part of the statement.
