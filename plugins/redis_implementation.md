## redis插件实现

1. 表描述文件解析

redis插件通过表描述文件定义了redis数据和preso表之间的映射关系，在presto框架中RedisTableDescriptionSupplier类提供了表描述文件的解析方法：

RedisTableDescriptionSupplier.get方法返回了表明和RedisTableDescription的映射关系：

```java
public Map<SchemaTableName, RedisTableDescription> get()
{
    ImmutableMap.Builder<SchemaTableName, RedisTableDescription> builder = ImmutableMap.builder();

    try {
    // 在redis.table-description-dir配置项指定的目录中遍历所有的json格式文件（表描述文件）
        for (File file : listFiles(redisConnectorConfig.getTableDescriptionDir())) {
            if (file.isFile() && file.getName().endsWith(".json")) {
            // 解析json文件内容，构造RedisTableDescription实例
                RedisTableDescription table = tableDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                String schemaName = firstNonNull(table.getSchemaName(), redisConnectorConfig.getDefaultSchema());
                log.debug("Redis table %s.%s: %s", schemaName, table.getTableName(), table);
                builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
            }
        }

        // 根据表描述文件解析得到的表名和RedisTableDescription的Map
        Map<SchemaTableName, RedisTableDescription> tableDefinitions = builder.build();

        log.debug("Loaded table definitions: %s", tableDefinitions.keySet());

        builder = ImmutableMap.builder();
        // 遍历配置文件中配置的表
        for (String definedTable : redisConnectorConfig.getTableNames()) {
            SchemaTableName tableName;
            try {
                tableName = parseTableName(definedTable);
            }
            catch (IllegalArgumentException iae) {
                tableName = new SchemaTableName(redisConnectorConfig.getDefaultSchema(), definedTable);
            }

            // 如果这个表有对应的表描述文件，添加到返回结果中
            if (tableDefinitions.containsKey(tableName)) {
                RedisTableDescription redisTable = tableDefinitions.get(tableName);
                log.debug("Found Table definition for %s: %s", tableName, redisTable);
                builder.put(tableName, redisTable);
            }
            // 没有表描述文件，构造一个虚拟表，虚拟表只包含内部列
            else {
                // A dummy table definition only supports the internal columns.
                log.debug("Created dummy Table definition for %s", tableName);
                builder.put(tableName, new RedisTableDescription(tableName.getTableName(),
                        tableName.getSchemaName(),
                        new RedisTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.of()),
                        new RedisTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.of())));
            }
        }

        return builder.build();
    }
    catch (IOException e) {
        log.warn(e, "Error: ");
        throw new UncheckedIOException(e);
    }
}
```

2. 表数据读取过程

presto表的数据的读取过程基本经过几个步骤：

* tableHandle -> Splits
* split -> RecordSet
* RecordSet -> RecordCursor

下面看redis插件中几个步骤的实现：

### tableHandle -> Splits

RedisSplitManager.getSplits

```java
public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
{
    RedisTableHandle redisTableHandle = convertLayout(layout).getTable();

    List<HostAddress> nodes = new ArrayList<>(redisConnectorConfig.getNodes());
    Collections.shuffle(nodes);

    checkState(!nodes.isEmpty(), "No Redis nodes available");
    ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

    long numberOfKeys = 1;
    // 插件支持两种读取key的方式，一是遍历整个redis库，读取所有的key；二是从指定的一个zset类型中读取key
    // 如果表描述文件中key的dataFormat为zset的话，则采用第二种方式，并且将zset中的key分成若干个区间，划分为不同的split读取。
    // 否则只生成一个split
    // when Redis keys are provides in a zset, create multiple
    // splits by splitting zset in chunks
    if (redisTableHandle.getKeyDataFormat().equals("zset")) {
        try (Jedis jedis = jedisManager.getJedisPool(nodes.get(0)).getResource()) {
            numberOfKeys = jedis.zcount(redisTableHandle.getKeyName(), "-inf", "+inf");
        }
    }

    long stride = REDIS_STRIDE_SPLITS;

    if (numberOfKeys / stride > REDIS_MAX_SPLITS) {
        stride = numberOfKeys / REDIS_MAX_SPLITS;
    }

    for (long startIndex = 0; startIndex < numberOfKeys; startIndex += stride) {
        long endIndex = startIndex + stride - 1;
        if (endIndex >= numberOfKeys) {
            endIndex = -1;
        }

        RedisSplit split = new RedisSplit(connectorId,
                redisTableHandle.getSchemaName(),
                redisTableHandle.getTableName(),
                redisTableHandle.getKeyDataFormat(),
                redisTableHandle.getValueDataFormat(),
                redisTableHandle.getKeyName(),
                startIndex,
                endIndex,
                nodes);

        builder.add(split);
    }
    return new FixedSplitSource(builder.build());
}
```

### split -> RecordSet

```java
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
{
    RedisSplit redisSplit = convertSplit(split);

    List<RedisColumnHandle> redisColumns = columns.stream()
            .map(RedisHandleResolver::convertColumnHandle)
            .collect(ImmutableList.toImmutableList());

    // 解码器负责将输入数据转化为各列的值
    // key解码器
    RowDecoder keyDecoder = decoderFactory.create(
            redisSplit.getKeyDataFormat(),
            emptyMap(),
            redisColumns.stream()
                    .filter(col -> !col.isInternal())
                    .filter(RedisColumnHandle::isKeyDecoder)
                    .collect(toImmutableSet()));

    // value解码器
    RowDecoder valueDecoder = decoderFactory.create(
            redisSplit.getValueDataFormat(),
            emptyMap(),
            redisColumns.stream()
                    .filter(col -> !col.isInternal())
                    .filter(col -> !col.isKeyDecoder())
                    .collect(toImmutableSet()));

    return new RedisRecordSet(redisSplit, jedisManager, redisColumns, keyDecoder, valueDecoder);
}
```

### RecordSet -> RecordCursor

