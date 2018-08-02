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
```java
private RelationPlan createTableCreationPlan(Analysis analysis, Query query)
{
    QualifiedObjectName destination = analysis.getCreateTableDestination().get();

    RelationPlan plan = createRelationPlan(analysis, query);

    ConnectorTableMetadata tableMetadata = createTableMetadata(
            destination,
            getOutputTableColumns(plan, analysis.getColumnAliases()),
            analysis.getCreateTableProperties(),
            analysis.getParameters(),
            analysis.getCreateTableComment());
    Optional<NewTableLayout> newTableLayout = metadata.getNewTableLayout(session, destination.getCatalogName(), tableMetadata);

    List<String> columnNames = tableMetadata.getColumns().stream()
            .filter(column -> !column.isHidden())
            .map(ColumnMetadata::getName)
            .collect(toImmutableList());

    return createTableWriterPlan(
            analysis,
            plan,
            new CreateName(destination.getCatalogName(), tableMetadata, newTableLayout),
            columnNames,
            newTableLayout);
}
```
this function first calls createRelationPlan to generate the plan of the query part,
then it calls `createTableWriterPlan` to generate the `TableFinishNode` wrapped in `RelationPlan`.

1.1.2 `createInsertPlan`
if the statement's type is Insert, then createInsertPlan is called:
```
private RelationPlan createInsertPlan(Analysis analysis, Insert insertStatement)
{
    Analysis.Insert insert = analysis.getInsert().get();

    TableMetadata tableMetadata = metadata.getTableMetadata(session, insert.getTarget());

    List<ColumnMetadata> visibleTableColumns = tableMetadata.getColumns().stream()
            .filter(column -> !column.isHidden())
            .collect(toImmutableList());
    List<String> visibleTableColumnNames = visibleTableColumns.stream()
            .map(ColumnMetadata::getName)
            .collect(toImmutableList());

    RelationPlan plan = createRelationPlan(analysis, insertStatement.getQuery());

    Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, insert.getTarget());
    Assignments.Builder assignments = Assignments.builder();
    for (ColumnMetadata column : tableMetadata.getColumns()) {
        if (column.isHidden()) {
            continue;
        }
        Symbol output = symbolAllocator.newSymbol(column.getName(), column.getType());
        int index = insert.getColumns().indexOf(columns.get(column.getName()));
        if (index < 0) {
            Expression cast = new Cast(new NullLiteral(), column.getType().getTypeSignature().toString());
            assignments.put(output, cast);
        }
        else {
            Symbol input = plan.getSymbol(index);
            Type tableType = column.getType();
            Type queryType = symbolAllocator.getTypes().get(input);

            if (queryType.equals(tableType) || metadata.getTypeManager().isTypeOnlyCoercion(queryType, tableType)) {
                assignments.put(output, input.toSymbolReference());
            }
            else {
                Expression cast = new Cast(input.toSymbolReference(), tableType.getTypeSignature().toString());
                assignments.put(output, cast);
            }
        }
    }
    ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());

    List<Field> fields = visibleTableColumns.stream()
            .map(column -> Field.newUnqualified(column.getName(), column.getType()))
            .collect(toImmutableList());
    Scope scope = Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields)).build();

    plan = new RelationPlan(projectNode, scope, projectNode.getOutputSymbols());

    Optional<NewTableLayout> newTableLayout = metadata.getInsertLayout(session, insert.getTarget());

    return createTableWriterPlan(
            analysis,
            plan,
            new InsertReference(insert.getTarget()),
            visibleTableColumnNames,
            newTableLayout);
}
```
this is a long function which can be devided into three parts:
* calls `createRelationPlan` to create Plan of the insert query part;
* create a ProjectNode with the query part Plan and the assignments
* calls `createTableWriterPlan` to generate the `TableFinishNode`

1.1.2 `createDeletePlan`
if statement's type is Delete, then it calls `createDeletePlan`:
```java
private RelationPlan createDeletePlan(Analysis analysis, Delete node)
{
    DeleteNode deleteNode = new QueryPlanner(analysis, symbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, symbolAllocator), metadata, session)
            .plan(node);

    List<Symbol> outputs = ImmutableList.of(symbolAllocator.newSymbol("rows", BIGINT));
    TableFinishNode commitNode = new TableFinishNode(idAllocator.getNextId(), deleteNode, deleteNode.getTarget(), outputs);

    return new RelationPlan(commitNode, analysis.getScope(node), commitNode.getOutputSymbols());
}
```
the function first calls QueryPlanner.plan(Delete) to generate a DeleteNode,
and then create a TableFinishNode with the DeleteNode as arguments.

1.1.3 `createRelationPlan`
if statement's type is Query, then it just calls createRelationPlan to generate a RelationPlan,
this function is used before when other kinds of statements contains a query part,
so we analyze the implementation here:

```java
private RelationPlan createRelationPlan(Analysis analysis, Query query)
{
    return new RelationPlanner(analysis, symbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, symbolAllocator), metadata, session)
            .process(query, null);
}
```
the function use RelationPlanner.process to process the query, RelationPlanner is a AstVisitor
,it can process all kinds of Query Type Nodes in the AST.

1.1.4 `createExplainAnalyzePlan`
at last, if statement's type is Explain, it calls `createExplainAnalyzePlan`:
```java
private RelationPlan createExplainAnalyzePlan(Analysis analysis, Explain statement)
{
    RelationPlan underlyingPlan = planStatementWithoutOutput(analysis, statement.getStatement());
    PlanNode root = underlyingPlan.getRoot();
    Scope scope = analysis.getScope(statement);
    Symbol outputSymbol = symbolAllocator.newSymbol(scope.getRelationType().getFieldByIndex(0));
    root = new ExplainAnalyzeNode(idAllocator.getNextId(), root, outputSymbol, statement.isVerbose());
    return new RelationPlan(root, scope, ImmutableList.of(outputSymbol));
}
```
this function just calls `planStatementWithoutOutput` to get the planNode and wraps it in `ExplainAnalyzeNode`
