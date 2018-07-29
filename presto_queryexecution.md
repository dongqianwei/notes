in SqlQueryManger.createQueryInternal(433),get QueryExecutionFactory,
```java
QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(statement.getClass());
```
QueryExecutionFactory interface has two implementations: `DataDefinationExecutionFactory` and `SqlQueryExecutionFactory`,
`executionFactories`is a map initialized in `CoordinatorModule`,`SqlQueryExecutionFactory`is binded as following:
```java
getAllQueryTypes().entrySet().stream()
        .filter(entry -> entry.getValue() != QueryType.DATA_DEFINITION)
        .forEach(entry -> executionBinder.addBinding(entry.getKey()).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON));

```
`getAllQueryTypes()`get query types defined in StatementUtils and bind all Statement with types not equals with `QueryType.DATA_DEFINITION`
to `SqlQueryExecutionFactory`.
other Statement is binded to `DataDefinitionExecuationFactory` by following calls:
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
every Data Definition Statement is bind to its Task in a MapBinder,
and the map will be injected into `DataDefinitionExecuationFactory`
by its constructor.

So when in SqlQueryManger.createQueryInternal(433)
```java
QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(statement.getClass());
```
if statement is kind of DataDefinationStatement, then `DataDefinationExecuationFactory` is returned.
then it will call
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
and the DataDefinationTask is fetched and wrapped in DataDefinitionExecution and returned, and the task will run when Execution is started.

and If Statement is of other types, `SqlQueryExecutionFactory` is returned and it will create `SqlQueryExecution`,
when this Execution start, the Query is analysed and create Plan and so on.
