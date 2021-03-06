# presto插件API TIPS

1. ConnectorPageSourceProvider vs ConnectorRecordSetProvider

在MaterializedConnector构造函数中：
```java
ConnectorPageSourceProvider connectorPageSourceProvider = null;
try {
    connectorPageSourceProvider = connector.getPageSourceProvider();
    requireNonNull(connectorPageSourceProvider, format("Connector %s returned a null page source provider", connectorId));
}
catch (UnsupportedOperationException ignored) {
}

// 如果connectorPageSourceProvider为空且connectorRecordSetProvider也为空则报错
if (connectorPageSourceProvider == null) {
    ConnectorRecordSetProvider connectorRecordSetProvider = null;
    try {
        connectorRecordSetProvider = connector.getRecordSetProvider();
        // 报错
        requireNonNull(connectorRecordSetProvider, format("Connector %s returned a null record set provider", connectorId));
    }
    catch (UnsupportedOperationException ignored) {
    }
    checkState(connectorRecordSetProvider != null, "Connector %s has neither a PageSource or RecordSet provider", connectorId);
    connectorPageSourceProvider = new RecordPageSourceProvider(connectorRecordSetProvider);
}
this.pageSourceProvider = connectorPageSourceProvider;
```
根据代码逻辑可以看出Connector接口的实现类必须至少实现以上两种接口之一。
并且如果实现了connectorRecordSetProvider，则会调用
```java
new RecordPageSourceProvider(connectorRecordSetProvider)
```
将其封装为connectorPageSourceProvider。

下面来看ConnectorPageSourceProvider和ConnectorRecordSetProvider的区别：

```java
public interface ConnectorPageSourceProvider
{
    ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns);
}

public interface ConnectorPageSource
        extends Closeable
{
    CompletableFuture<?> NOT_BLOCKED = CompletableFuture.completedFuture(null);
    long getCompletedBytes();
    long getReadTimeNanos();
    boolean isFinished();
    Page getNextPage();
    long getSystemMemoryUsage();
    @Override
    void close()
            throws IOException;
    default CompletableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }
}

```

ConnectorPageSourceProvider接口中createPageSource方法接受一个split返回ConnectorPageSource，
ConnectorPageSource接口可以以Page的方式读取split中的数据。

```java
public interface ConnectorRecordSetProvider
{
    RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns);
}

public interface RecordSet
{
    List<Type> getColumnTypes();

    RecordCursor cursor();
}

public interface RecordCursor
        extends Closeable
{
    long getCompletedBytes();
    long getReadTimeNanos();
    Type getType(int field);
    boolean advanceNextPosition();
    boolean getBoolean(int field);
    long getLong(int field);
    double getDouble(int field);
    Slice getSlice(int field);
    Object getObject(int field);
    boolean isNull(int field);
    default long getSystemMemoryUsage()
    {
        // TODO: implement this method in subclasses and remove this default implementation
        return 0;
    }
    @Override
    void close();
}
```

ConnectorRecordSetProvider提供了传入split返回RecordSet的方法，RecordSet中可以获取RecordCursor接口对象，
通过该对象可以直接按照行列从split中读取表的信息。

上文提到presto通过RecordPageSourceProvider类将connectorRecordSetProvider转成connectorPageSourceProvider接口，
下面分析接口转换过程:

```java
public class RecordPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private ConnectorRecordSetProvider recordSetProvider;

    public RecordPageSourceProvider(ConnectorRecordSetProvider recordSetProvider)
    {
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        return new RecordPageSource(recordSetProvider.getRecordSet(transactionHandle, session, split, columns));
    }
}
```
可以看到该类通过RecordPageSource类将RecordSet转为ConnectorPageSource接口：
```java
public RecordPageSource(RecordSet recordSet)
{
    this(requireNonNull(recordSet, "recordSet is null").getColumnTypes(), recordSet.cursor());
}

public RecordPageSource(List<Type> types, RecordCursor cursor)
{
    this.cursor = requireNonNull(cursor, "cursor is null");
    this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));
    this.pageBuilder = new PageBuilder(this.types);
}
```
构造函数中初始化了cursor和pageBuilder，下面看关键性的方法getNextPage:
```java
@Override
public Page getNextPage()
{
    if (!closed) {
        int i;
        for (i = 0; i < ROWS_PER_REQUEST; i++) {
            if (pageBuilder.isFull()) {
                break;
            }

            if (!cursor.advanceNextPosition()) {
                closed = true;
                break;
            }

            pageBuilder.declarePosition();
            for (int column = 0; column < types.size(); column++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                if (cursor.isNull(column)) {
                    output.appendNull();
                }
                else {
                    Type type = types.get(column);
                    Class<?> javaType = type.getJavaType();
                    if (javaType == boolean.class) {
                        type.writeBoolean(output, cursor.getBoolean(column));
                    }
                    else if (javaType == long.class) {
                        type.writeLong(output, cursor.getLong(column));
                    }
                    else if (javaType == double.class) {
                        type.writeDouble(output, cursor.getDouble(column));
                    }
                    else if (javaType == Slice.class) {
                        Slice slice = cursor.getSlice(column);
                        type.writeSlice(output, slice, 0, slice.length());
                    }
                    else {
                        type.writeObject(output, cursor.getObject(column));
                    }
                }
            }
        }
    }

    // only return a page if the buffer is full or we are finishing
    if (pageBuilder.isEmpty() || (!closed && !pageBuilder.isFull())) {
        return null;
    }

    Page page = pageBuilder.build();
    pageBuilder.reset();

    return page;
}
```

