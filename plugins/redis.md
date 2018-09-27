## 手册

[redis 插件官方手册](https://prestodb.io/docs/current/connector/redis.html)

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

如果没有表描述文件，_key_corrupt 和 _value_corrupt 列应该都为false。

3. 表描述文件

