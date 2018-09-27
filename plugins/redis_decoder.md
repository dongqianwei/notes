### redis解码器

目前redis插件支持STRING和HASH两种类型，分别通过对应的解码器转换为列，其中STRING类型支持RAW，CSV和JSON三种具体类型。

1. RawRowDecoder

2. CsvRowDecoder

3. JsonRowDecoder

4. HashRedisRowDecoder
