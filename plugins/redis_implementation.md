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


