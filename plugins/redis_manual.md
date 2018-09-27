## redis插件手册

[redis 插件官方手册](https://prestodb.io/docs/current/connector/redis.html)

每一个redis键值对都会在presto中转换为一行数据。行可以进一步根据表描述文件分解为多个cell。

当前只支持String和Hash两种value类型，set和zset类型的数据目前无法查询。

1. 配置项：

名称 | 说明
---|---
redis.table-names | 所有的表名称（如果schema不存在，使用下面定义的默认schema）
redis.default-schema |	默认的schema名称（默认值为default）
redis.nodes |	Redis服务器地址
redis.scan-count |	redis SCAN指令的COUNT参数，默认值为100
redis.key-prefix-schema-table |	如果为true，则只扫描schema-name:table-name为前缀的key，否则扫描所有key。默认为false
redis.key-delimiter |	如果redis.key-prefix-schema-table为true，指定schema_name和table_name的分隔符，默认为':'
redis.table-description-dir | 表描述文件所在目录，默认值为etc/redis。包含json格式的表描述文件
redis.hide-internal-columns | 控制是否隐藏`内部列`。（除了表描述文件中描述的列，还有一些内部列保存connector维护的一些额外信息，下文说明）
redis.database-index |	指向Redis数据库的索引，默认为0。
redis.password |	Redis服务器密码


2. 内部列：


列名 | 类型 | 描述
--|--|--
_key | VARCHAR |	Redis key.
_value |	VARCHAR |	Redis value corresponding to the key.
_key_length |	BIGINT |	Number of bytes in the key.
_value_length |	BIGINT |	Number of bytes in the value.
_key_corrupt |	BOOLEAN |	True if the decoder could not decode the key for this row. When true, data columns mapped from the key should be treated as invalid.
_value_corrupt |	BOOLEAN |	True if the decoder could not decode the message for this row. When true, data columns mapped from the value should be treated as invalid.

如果没有表
文件，_key_corrupt 和 _value_corrupt 列应该都为false。

3. 表定义文件

通过表定义文件可以将key/value 字符串进一步解析为新的列。
每一个定义文件包含了一个表的定义，文件名是任意的，但必须以json结尾。

```json
{
    "tableName": ...,
    "schemaName": ...,
    "key": {
        "dataFormat": ...,
        "fields": [
            ...
        ]
    },
    "value": {
        "dataFormat": ...,
        "fields": [
            ...
       ]
    }
}
```

字段 |是否必须 |	类型 |	描述
--|--|--|--
tableName |	required |	string | 该文件定义的表名。
schemaName |	optional |	string | 表所属的schema，如果为空，使用默认值。
key |	optional |	JSON object |	映射到键的相关列的字段定义。
value |	optional |	JSON object |	映射到值的相关列的字段定义。

其中value的dataFormat支持hash类型，以支持redis中的hash类型。
