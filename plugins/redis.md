1. 手册

[redis 插件官方文档](https://prestodb.io/docs/current/connector/redis.html)

配置项：


名称 | 说明
---|---
redis.table-names | 所有的表名称
redis.default-schema |	默认的schema名称
redis.nodes |	Redis服务器地址
redis.scan-count |	redis扫描keys所用的参数
redis.key-prefix-schema-table |	Redis keys 有 schema-name:table-name 形式的前缀
redis.key-delimiter |	如果使用了redis.key-prefix-schema-table，schema_name和table_name的分隔符
redis.table-description-dir | 表描述文件所在目录
redis.hide-internal-columns |	控制内部列是否为表schema的一部分
redis.database-index |	Redis数据库索引
redis.password |	Redis服务器密码

