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

```java
public RecordCursor cursor()
{
    return new RedisRecordCursor(keyDecoder, valueDecoder, split, columnHandles, jedisManager);
}

// 构造函数
RedisRecordCursor(
        RowDecoder keyDecoder,
        RowDecoder valueDecoder,
        RedisSplit split,
        List<RedisColumnHandle> columnHandles,
        RedisJedisManager redisJedisManager)
{
    this.keyDecoder = keyDecoder;
    this.valueDecoder = valueDecoder;
    this.split = split;
    this.columnHandles = columnHandles;
    this.redisJedisManager = redisJedisManager;
    this.jedisPool = redisJedisManager.getJedisPool(split.getNodes().get(0));
    this.scanParms = setScanParms();
    this.currentRowValues = new FieldValueProvider[columnHandles.size()];

    fetchKeys();
}

// 构造函数最后一步调用fetchKeys()，将会初始化后面遍历key用的迭代器:keyIterator。

// 可以看到keys要么从用户制定的zset中获取，要么通过redis scan获取
    // Redis keys can be contained in the user-provided ZSET
    // Otherwise they need to be found by scanning Redis
    private boolean fetchKeys()
    {
        try (Jedis jedis = jedisPool.getResource()) {
            // 如果key的类型为string，调用scan方法
            // 因为redis scan是利用cursor分批返回数据，所以需要保存一个redisCursor
            switch (split.getKeyDataType()) {
                case STRING: {
                    String cursor = SCAN_POINTER_START;
                    if (redisCursor != null) {
                        cursor = redisCursor.getStringCursor();
                    }

                    log.debug("Scanning new Redis keys from cursor %s . %d values read so far", cursor, totalValues);
                    // 调用jedis.scan获取keys
                    redisCursor = jedis.scan(cursor, scanParms);
                    List<String> keys = redisCursor.getResult();
                    // 设置keyIterator
                    keysIterator = keys.iterator();
                }
                break;
                // 如果key类型为zset
                case ZSET:
                // split.getKeyName为zset的key，根据上一布分配的zset范围获取keys
                    Set<String> keys = jedis.zrange(split.getKeyName(), split.getStart(), split.getEnd());
                    // 设置keyIterator
                    keysIterator = keys.iterator();
                    break;
                default:
                    log.debug("Redis type of key %s is unsupported", split.getKeyDataFormat());
                    return false;
            }
        }
        return true;
    }

// advanceNextPosition在遍历cursor时，移动到下一行的数据，并返回是否还有数据
    @Override
    public boolean advanceNextPosition()
    {
        // 如果keysIterator遍历完了
        while (!keysIterator.hasNext()) {
            if (!hasUnscannedData()) {
                return endOfData();
            }
            // 如果还有没有处理的数据（当key类型为STRING，且scan还没有返回最后的数据时），
            // 再次调用fetchKeys()更新redisCursor和keysIterator
            fetchKeys();
        }

        //根据key获取到value并解析为行，下文分析
        return nextRow(keysIterator.next());
    }


// RedisRecordCursor.nextRow
// 在advanceNextPosition中调用，用于初始化读取下一行所需要的数据
    private boolean nextRow(String keyString)
    {
        // 根据当前key获取并解析redis数据，保存在valueString或valueMap中，下文分析
        fetchData(keyString);

        // key字符串的byte array
        byte[] keyData = keyString.getBytes(StandardCharsets.UTF_8);

        byte[] valueData = EMPTY_BYTE_ARRAY;
        // 如果valueString中有值，说明value为String类型
        if (valueString != null) {
        // 将valueString转为byte array，保存在valueData中
            valueData = valueString.getBytes(StandardCharsets.UTF_8);
        }

        totalBytes += valueData.length;
        totalValues++;

        // 用keyDecoder对key解码，生成对应列
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey = keyDecoder.decodeRow(
                keyData,
                null);
        // 用valueDecoder对value解码，生成对应列
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = valueDecoder.decodeRow(
                valueData,
                valueMap);

        Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();

        // 保存内部列各项数据
        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                RedisInternalFieldDescription fieldDescription = RedisInternalFieldDescription.forColumnName(columnHandle.getName());
                switch (fieldDescription) {
                    case KEY_FIELD:
                        currentRowValuesMap.put(columnHandle, bytesValueProvider(keyData));
                        break;
                    case VALUE_FIELD:
                        currentRowValuesMap.put(columnHandle, bytesValueProvider(valueData));
                        break;
                    case KEY_LENGTH_FIELD:
                        currentRowValuesMap.put(columnHandle, longValueProvider(keyData.length));
                        break;
                    case VALUE_LENGTH_FIELD:
                        currentRowValuesMap.put(columnHandle, longValueProvider(valueData.length));
                        break;
                    case KEY_CORRUPT_FIELD:
                        currentRowValuesMap.put(columnHandle, booleanValueProvider(!decodedKey.isPresent()));
                        break;
                    case VALUE_CORRUPT_FIELD:
                        currentRowValuesMap.put(columnHandle, booleanValueProvider(!decodedValue.isPresent()));
                        break;
                    default:
                        throw new IllegalArgumentException("unknown internal field " + fieldDescription);
                }
            }
        }

        // 保存key/value解码的到的各列数据
        decodedKey.ifPresent(currentRowValuesMap::putAll);
        decodedValue.ifPresent(currentRowValuesMap::putAll);

        // 将各列数据保存到currentRowValues中
        for (int i = 0; i < columnHandles.size(); i++) {
            ColumnHandle columnHandle = columnHandles.get(i);
            currentRowValues[i] = currentRowValuesMap.get(columnHandle);
        }

        return true;
    }


// fetchData，根据key获取读取行所需要的各项数据
// 根据value类型保存在valueString或者valueMap中
    private boolean fetchData(String keyString)
    {
        valueString = null;
        valueMap = null;
        // redis插件目前支持两种value类型，String和hash
        // 其中hash类型需要HashRowDecoder解码器解析为各列数据
        // Redis connector supports two types of Redis
        // values: STRING and HASH
        // HASH types requires hash row decoder to
        // fill in the columns
        // whereas for the STRING type decoders are optional
        try (Jedis jedis = jedisPool.getResource()) {
            switch (split.getValueDataType()) {
                case STRING:
                // 如果value类型为String
                // 调用jedis.get获取value，保存到valueString中
                    valueString = jedis.get(keyString);
                    if (valueString == null) {
                        log.warn("Redis data modified while query was running, string value at key %s deleted", keyString);
                        return false;
                    }
                    break;
                    // 如果value类型为hash
                case HASH:
                // 调用jedis.hgetAll获取hash表，保存到valueMap中
                    valueMap = jedis.hgetAll(keyString);
                    if (valueMap == null) {
                        log.warn("Redis data modified while query was running, hash value at key %s deleted", keyString);
                        return false;
                    }
                    break;
                default:
                    log.debug("Redis type for key %s is unsupported", keyString);
                    return false;
            }
        }
        return true;
    }
```


