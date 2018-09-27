## MY NOTES

### presto 源码学习

* [QueryExecutionFactory](QueryExecutionFactory)
* [SqlQueryExecution](SqlQueryExecution)
* [Plan Fragmenter](PlanFragmenter)
* [ExchangeNode](ExchangeNode)
* [Plan Distribution](PlanDistribution)
* [presto worker原理](presto_worker.md)
* [SqlTaskExecution](SqlTaskExecution)
* [StageExecutionPlan](StageExecutionPlan)
* presto spi
  * [ConnectorPageSourceProvider vs ConnectorRecordSetProvider](presto_spi/ConnectorPageSourceProvider)
  * spi components
* [StageLinkage](StageLinkage)
* [SourcePartitionedScheduler](SourcePartitionedScheduler)
* [presto jdbc并行优化方案](presto_jdbc_Parallelism)
* [presto类型演变](presto_types)
* [presto summary](presto_summary)
* 优化器
  * [presto 模式匹配](presto_pattern_match)
  * [presto基于代价的优化](optimize/presto_cost)
* join
  * [DetermineJoinDistributionType](join/DetermineJoinDistributionType)
  * [ReorderJoins](join/ReorderJoins)
  * [PARTITIONED JOIN](join/presto_partitioned_join)
  * [REPLICATED JOIN](join/presto_replicated_join)
* plugins
  * jdbc(TODO)
  * redis
   * [Redis手册](plugins/redis_manual)
   * [Redis实现](plugins/redis_implementation)

### 容器技术

* [docker](container/docker)
* [kubernetes](container/kubernetes)
