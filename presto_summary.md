# presto全流程索引

1. coodrinator入口:


StatementResource
getQueryResults
-> asyncQueryResults

Query
--> waitForResults
---> getFutureStateChange
----> QuerySubmissionFuture.submitQuery

SqlQueryManager
-----> createQuery
------> createQueryInternal
{
statement = sqlParser.createStatement()
QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(statement.getClass());
queryExecution = queryExecutionFactory.createQueryExecution(queryId, query, session, statement, parameters);
  /**
    two types:
    DataDefinationExecutionFactory
    SqlQueryExecutionFactory
  */
resourceGroupManager.submit(statement, queryExecution, selectionContext, queryExecutor);
}

SqlQueryExecution.start
{
PlanRoot plan = analyzeQuery();
->doAnalyzeQuery
    {
        LogicalPlanner logicalPlanner = new LogicalPlanner(stateMachine.getSession(), planOptimizers, idAllocator, metadata, sqlParser);
        Plan plan = logicalPlanner.plan(analysis);
        SubPlan fragmentedPlan = PlanFragmenter.createSubPlans(stateMachine.getSession(), metadata, nodePartitioningManager, plan, false);
        return new PlanRoot(fragmentedPlan, !explainAnalyze, extractConnectors(analysis));
    }
planDistribution(plan);
  {
    DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(splitManager);
    StageExecutionPlan outputStageExecutionPlan = distributedPlanner.plan(plan.getRoot(), stateMachine.getSession());
    SqlQueryScheduler scheduler = new SqlQueryScheduler()
      {
      
      }
    queryScheduler.set(scheduler);
  }
SqlQueryScheduler scheduler = queryScheduler.get();
scheduler.start();
}

