ExchangeNode是一种特殊的PlanNode，该节点是在优化步骤中添加的，主要有AddExchanges与AddLocalExchanges等等。

该节点的作用是作为不同计算过程中的数据转发节点，其中AddExchanges添加的是scope为REMOTE的节点，用作不同worker中的数据转发；
AddLocalExchanges添加的是scope为LOCAL的节点，用作单个worker内部的数据转发，提高算法的并行度。

可以看到，当ExchangeNode的scope属性为REMOTE的时候，最终被重写为RemoteSourceNode。

那么什么场景下会添加scope为REMOTE的ExchangeNode呢？以AddExchanges.visitLimit方法为例：

```java
@Override
public PlanWithProperties visitLimit(LimitNode node, PreferredProperties preferredProperties)
{
    PlanWithProperties child = planChild(node, PreferredProperties.any());

    if (!child.getProperties().isSingleNode()) {
        child = withDerivedProperties(
                new LimitNode(idAllocator.getNextId(), child.getNode(), node.getCount(), true),
                child.getProperties());

        child = withDerivedProperties(
                gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                child.getProperties());
    }

    return rebaseAndDeriveProperties(node, child);
}
```
当LimitNode的source PlanNode不是SingleNode时，才会构造一个中间的garther类型ExchangeNode节点，
因为source分布在多个节点，所以必须要有一个exchange负责收集各节点数据。
