in SqlQueryManger.createQueryInternal(Line 433)
```java
QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(statement.getClass());
```
this statement get `QueryExecutionFactory` from `executionFactories` by statement's class,
QueryExecutionFactory interface has two implementations: `DataDefinationExecutionFactory` and `SqlQueryExecutionFactory.
`executionFactories` is a Map and maps statement Class to `QueryExecutionFactory`,
the map is injected by Guice and initilized in `CoordinatorModule`,
```java
getAllQueryTypes().entrySet().stream()
        .filter(entry -> entry.getValue() != QueryType.DATA_DEFINITION)
        .forEach(entry -> executionBinder.addBinding(entry.getKey()).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON));

```
`getAllQueryTypes()`get all query's type build in `StatementUtils`,
```java
builder.put(Query.class, QueryType.SELECT);

builder.put(Explain.class, QueryType.EXPLAIN);

builder.put(CreateTableAsSelect.class, QueryType.INSERT);
builder.put(Insert.class, QueryType.INSERT);

builder.put(Delete.class, QueryType.DELETE);

builder.put(ShowCatalogs.class, QueryType.DESCRIBE);
builder.put(ShowCreate.class, QueryType.DESCRIBE);
...

builder.put(CreateSchema.class, QueryType.DATA_DEFINITION);
builder.put(DropSchema.class, QueryType.DATA_DEFINITION);
builder.put(RenameSchema.class, QueryType.DATA_DEFINITION);
```
and bind all Statement with types not equals to `QueryType.DATA_DEFINITION`
to `SqlQueryExecutionFactory`.
other kinds of Statements is bind to `DataDefinitionExecuationFactory` by following calls:
```java
bindDataDefinitionTask(binder, executionBinder, CreateSchema.class, CreateSchemaTask.class);
bindDataDefinitionTask(binder, executionBinder, DropSchema.class, DropSchemaTask.class);
bindDataDefinitionTask(binder, executionBinder, RenameSchema.class, RenameSchemaTask.class);
bindDataDefinitionTask(binder, executionBinder, AddColumn.class, AddColumnTask.class);
bindDataDefinitionTask(binder, executionBinder, CreateTable.class, CreateTableTask.class);
bindDataDefinitionTask(binder, executionBinder, RenameTable.class, RenameTableTask.class);
bindDataDefinitionTask(binder, executionBinder, RenameColumn.class, RenameColumnTask.class);
...
```
in `bindDataDefinitionTask` 
```java
MapBinder<Class<? extends Statement>, DataDefinitionTask<?>> taskBinder = newMapBinder(binder,
        new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<DataDefinitionTask<?>>() {});

taskBinder.addBinding(statement).to(task).in(Scopes.SINGLETON);
executionBinder.addBinding(statement).to(DataDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
```
every DataDefinitionStatement is maped to its Task through a MapBinder,
and the map will be injected into `DataDefinitionExecutionFactory`'s `tasks` field,

So in
```java
QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(statement.getClass());
```
if statement's type is DataDefination, then `DataDefinationExecuationFactory` is returned.
then when it calls
```java
queryExecution = queryExecutionFactory.createQueryExecution(queryId, query, session, statement, parameters);
```
in `DataDefinationExecuationFactory`, the implementation is
```java
URI self = locationFactory.createQueryLocation(queryId);

DataDefinitionTask<Statement> task = getTask(statement);
checkArgument(task != null, "no task for statement: %s", statement.getClass().getSimpleName());

QueryStateMachine stateMachine = QueryStateMachine.begin(queryId, query, session, self, task.isTransactionControl(), transactionManager, accessControl, executor, metadata);
stateMachine.setUpdateType(task.getName());
return new DataDefinitionExecution<>(task, statement, transactionManager, metadata, accessControl, stateMachine, parameters);
```
and the `DataDefinationTask` is fetched and wrapped in `DataDefinitionExecution` and returned, and the task will run when `DataDefinitionExecution` is started.

and if Statement is of other types, `SqlQueryExecutionFactory` is returned and it will create `SqlQueryExecution`,
when this Execution start, the Query is analysed and create Plan and so on. It will be analysed in other articles.
