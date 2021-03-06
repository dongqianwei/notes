## ReorderJoins是优化器中的Rule，优化目标为找到代价最小的Join排列方式和算法

匹配规则为

```java
private static final Pattern<JoinNode> PATTERN = join().matching(
            joinNode -> !joinNode.getDistributionType().isPresent()
                    && joinNode.getType() == INNER
                    && isDeterministic(joinNode.getFilter().orElse(TRUE_LITERAL)));
```

匹配的条件为：joinNode节点，分布式算法未确定，过滤条件表达式是确定的（不包含随机函数）。

```java
public Result apply(JoinNode joinNode, Captures captures, Context context)
{
    //(1)
    MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, context.getLookup(), getMaxReorderedJoins(context.getSession()));
    //(2)
    JoinEnumerator joinEnumerator = new JoinEnumerator(
            costComparator,
            multiJoinNode.getFilter(),
            context);
    JoinEnumerationResult result = joinEnumerator.chooseJoinOrder(multiJoinNode.getSources(), multiJoinNode.getOutputSymbols());
    if (!result.getPlanNode().isPresent()) {
        return Result.empty();
    }
    return Result.ofPlanNode(result.getPlanNode().get());
}
```

1. toMultiJoinNode

```java
static MultiJoinNode toMultiJoinNode(JoinNode joinNode, Lookup lookup, int joinLimit)
{
// the number of sources is the number of joins + 1
return new JoinNodeFlattener(joinNode, lookup, joinLimit + 1).toMultiJoinNode();
}

JoinNodeFlattener(JoinNode node, Lookup lookup, int sourceLimit)
{
    requireNonNull(node, "node is null");
    checkState(node.getType() == INNER, "join type must be INNER");
    this.outputSymbols = node.getOutputSymbols();
    this.lookup = requireNonNull(lookup, "lookup is null");
    flattenNode(node, sourceLimit);
}

// JoinNode是一个树状结构，该方法的功能是将所有‘叶子节点’添加到sources中；将所有的Join条件添加到filters中
private void flattenNode(PlanNode node, int limit)
{
    PlanNode resolved = lookup.resolve(node);

    // (limit - 2) because you need to account for adding left and right side
    // 如果当前节点不是JoinNode类型，或者sources中的JoinNode数量超过limit - 2，添加当前节点并返回
    if (!(resolved instanceof JoinNode) || (sources.size() > (limit - 2))) {
        sources.add(node);
        return;
    }

    JoinNode joinNode = (JoinNode) resolved;
    // 如果当前JoinNode类型不是INNER JOIN；或者joinNode的过滤器不是确定；或者JOIN算法已经确定，添加当前节点并返回
    if (joinNode.getType() != INNER || !isDeterministic(joinNode.getFilter().orElse(TRUE_LITERAL)) || joinNode.getDistributionType().isPresent()) {
        sources.add(node);
        return;
    }

    // we set the left limit to limit - 1 to account for the node on the right
    // 第归处理JoinNode左右子节点
    flattenNode(joinNode.getLeft(), limit - 1);
    flattenNode(joinNode.getRight(), limit);
    // 将joinNode的criteria和Filter添加到filters中
    joinNode.getCriteria().stream()
            .map(EquiJoinClause::toExpression)
            .forEach(filters::add);
    joinNode.getFilter().ifPresent(filters::add);
}

MultiJoinNode toMultiJoinNode()
{
    return new MultiJoinNode(sources, and(filters), outputSymbols);
}
```

2. joinEnumerator.chooseJoinOrder

```java
        private JoinEnumerationResult chooseJoinOrder(LinkedHashSet<PlanNode> sources, List<Symbol> outputSymbols)
        {
            context.checkTimeoutNotExhausted();

            Set<PlanNode> multiJoinKey = ImmutableSet.copyOf(sources);
            // 检查缓存中有没有结果
            JoinEnumerationResult bestResult = memo.get(multiJoinKey);
            if (bestResult == null) {
                checkState(sources.size() > 1, "sources size is less than or equal to one");
                ImmutableList.Builder<JoinEnumerationResult> resultBuilder = ImmutableList.builder();
                // 生成所有可能的组合
                Set<Set<Integer>> partitions = generatePartitions(sources.size());
                for (Set<Integer> partition : partitions) {
                    // 遍历所有的组合
                    // （2.1）
                    JoinEnumerationResult result = createJoinAccordingToPartitioning(sources, outputSymbols, partition);
                    if (result.equals(UNKNOWN_COST_RESULT)) {
                        memo.put(multiJoinKey, result);
                        return result;
                    }
                    if (!result.equals(INFINITE_COST_RESULT)) {
                        resultBuilder.add(result);
                    }
                }

                List<JoinEnumerationResult> results = resultBuilder.build();
                if (results.isEmpty()) {
                    memo.put(multiJoinKey, INFINITE_COST_RESULT);
                    return INFINITE_COST_RESULT;
                }

                bestResult = resultComparator.min(results);
                memo.put(multiJoinKey, bestResult);
            }

            bestResult.planNode.ifPresent((planNode) -> log.debug("Least cost join was: %s", planNode));
            return bestResult;
        }
```

    2.1 createJoinAccordingToPartitioning

```java
JoinEnumerationResult createJoinAccordingToPartitioning(LinkedHashSet<PlanNode> sources, List<Symbol> outputSymbols, Set<Integer> partitioning)
{
List<PlanNode> sourceList = ImmutableList.copyOf(sources);
// 根据partitioning得到leftSources和rightSources
LinkedHashSet<PlanNode> leftSources = partitioning.stream()
        .map(sourceList::get)
        .collect(toCollection(LinkedHashSet::new));
LinkedHashSet<PlanNode> rightSources = sources.stream()
        .filter(source -> !leftSources.contains(source))
        .collect(toCollection(LinkedHashSet::new));
// 调用createJoin
return createJoin(leftSources, rightSources, outputSymbols);
}

private JoinEnumerationResult createJoin(LinkedHashSet<PlanNode> leftSources, LinkedHashSet<PlanNode> rightSources, List<Symbol> outputSymbols)
{
// leftSources中的所有输出符号
Set<Symbol> leftSymbols = leftSources.stream()
        .flatMap(node -> node.getOutputSymbols().stream())
        .collect(toImmutableSet());
// rightSources中的所有输出符号
Set<Symbol> rightSymbols = rightSources.stream()
        .flatMap(node -> node.getOutputSymbols().stream())
        .collect(toImmutableSet());
// 获取所有join predicates
// (2.1.1)
List<Expression> joinPredicates = getJoinPredicates(leftSymbols, rightSymbols);
// 过滤出所有Equal类型的条件
List<EquiJoinClause> joinConditions = joinPredicates.stream()
        .filter(JoinEnumerator::isJoinEqualityCondition)
        .map(predicate -> toEquiJoinClause((ComparisonExpression) predicate, leftSymbols))
        .collect(toImmutableList());
// 如果相等类型的条件为空，说明所有join都是cross join，返回INFINITE_COST_RESULT，即cost无限大，无法估计
if (joinConditions.isEmpty()) {
    return INFINITE_COST_RESULT;
}
// 过滤出所有Join Filters
List<Expression> joinFilters = joinPredicates.stream()
        .filter(predicate -> !isJoinEqualityCondition(predicate))
        .collect(toImmutableList());

// 所有join涉及到的列，包括结果中的和join predicates中的
Set<Symbol> requiredJoinSymbols = ImmutableSet.<Symbol>builder()
        .addAll(outputSymbols)
        .addAll(SymbolsExtractor.extractUnique(joinPredicates))
        .build();

// 对leftSources第归调用chooseJoinOrder，以及处理leftSources.size() == 1时的终止条件
// (2.1.2)
JoinEnumerationResult leftResult = getJoinSource(
        leftSources,
        requiredJoinSymbols.stream()
                .filter(leftSymbols::contains)
                .collect(toImmutableList()));
// cost无法估计或者无限大，直接返回
if (leftResult.equals(UNKNOWN_COST_RESULT)) {
    return UNKNOWN_COST_RESULT;
}
if (leftResult.equals(INFINITE_COST_RESULT)) {
    return INFINITE_COST_RESULT;
}

PlanNode left = leftResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));
// 处理rightResult
JoinEnumerationResult rightResult = getJoinSource(
        rightSources,
        requiredJoinSymbols.stream()
                .filter(rightSymbols::contains)
                .collect(toImmutableList()));
if (rightResult.equals(UNKNOWN_COST_RESULT)) {
    return UNKNOWN_COST_RESULT;
}
if (rightResult.equals(INFINITE_COST_RESULT)) {
    return INFINITE_COST_RESULT;
}

PlanNode right = rightResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

// sort output symbols so that the left input symbols are first
List<Symbol> sortedOutputSymbols = Stream.concat(left.getOutputSymbols().stream(), right.getOutputSymbols().stream())
        .filter(outputSymbols::contains)
        .collect(toImmutableList());
// 使用最优的leftSource和rightSource组合新的JoinNode
// 调用setJoinNodeProperties对当前joinNode计算所有的组合，选择最优组合
// (2.1.3)
return setJoinNodeProperties(new JoinNode(
        idAllocator.getNextId(),
        INNER,
        left,
        right,
        joinConditions,
        sortedOutputSymbols,
        joinFilters.isEmpty() ? Optional.empty() : Optional.of(and(joinFilters)),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()));
}
```

        2.1.1 getJoinPredicates
        
```java
private List<Expression> getJoinPredicates(Set<Symbol> leftSymbols, Set<Symbol> rightSymbols)
{
ImmutableList.Builder<Expression> joinPredicatesBuilder = ImmutableList.builder();

// This takes all conjuncts that were part of allFilters that
// could not be used for equality inference.
// If they use both the left and right symbols, we add them to the list of joinPredicates
stream(nonInferrableConjuncts(allFilter))
.map(conjunct -> allFilterInference.rewriteExpression(conjunct, symbol -> leftSymbols.contains(symbol) || rightSymbols.contains(symbol)))
.filter(Objects::nonNull)
// filter expressions that contain only left or right symbols
.filter(conjunct -> allFilterInference.rewriteExpression(conjunct, leftSymbols::contains) == null)
.filter(conjunct -> allFilterInference.rewriteExpression(conjunct, rightSymbols::contains) == null)
.forEach(joinPredicatesBuilder::add);

// create equality inference on available symbols
// TODO: make generateEqualitiesPartitionedBy take left and right scope
List<Expression> joinEqualities = allFilterInference.generateEqualitiesPartitionedBy(symbol -> leftSymbols.contains(symbol) || rightSymbols.contains(symbol)).getScopeEqualities();
EqualityInference joinInference = createEqualityInference(joinEqualities.toArray(new Expression[0]));
joinPredicatesBuilder.addAll(joinInference.generateEqualitiesPartitionedBy(in(leftSymbols)).getScopeStraddlingEqualities());

return joinPredicatesBuilder.build();
}
```
        2.1.2 getJoinSource
 
```java
private JoinEnumerationResult getJoinSource(LinkedHashSet<PlanNode> nodes, List<Symbol> outputSymbols)
{
// 如果nodes.size() == 1，进入第归调用的终止条件
if (nodes.size() == 1) {
    PlanNode planNode = getOnlyElement(nodes);
    ImmutableList.Builder<Expression> predicates = ImmutableList.builder();
    predicates.addAll(allFilterInference.generateEqualitiesPartitionedBy(outputSymbols::contains).getScopeEqualities());
    stream(nonInferrableConjuncts(allFilter))
            .map(conjunct -> allFilterInference.rewriteExpression(conjunct, outputSymbols::contains))
            .filter(Objects::nonNull)
            .forEach(predicates::add);
    Expression filter = combineConjuncts(predicates.build());
    if (!TRUE_LITERAL.equals(filter)) {
        planNode = new FilterNode(idAllocator.getNextId(), planNode, filter);
    }
    // 调用代价计算器计算planNode代价并返回结果
    return createJoinEnumerationResult(planNode);
}
// 第归调用chooseJoinOrder
return chooseJoinOrder(nodes, outputSymbols);
}

// 调用代价计算器
private JoinEnumerationResult createJoinEnumerationResult(PlanNode planNode)
{
return JoinEnumerationResult.createJoinEnumerationResult(Optional.of(planNode), costProvider.getCumulativeCost(planNode));
}
```
 
        2.1.3 setJoinNodeProperties
 
```java
private JoinEnumerationResult setJoinNodeProperties(JoinNode joinNode)
{
// TODO avoid stat (but not cost) recalculation for all considered (distribution,flip) pairs, since resulting relation is the same in all case
// 如果left或者right的表足够小，将小表设置为右表，将算法设置为REPLICATED，调用代价计算器计算代价返回。
if (isAtMostScalar(joinNode.getRight(), lookup)) {
    return createJoinEnumerationResult(joinNode.withDistributionType(REPLICATED));
}
if (isAtMostScalar(joinNode.getLeft(), lookup)) {
    return createJoinEnumerationResult(joinNode.flipChildren().withDistributionType(REPLICATED));
}

List<JoinEnumerationResult> possibleJoinNodes = new ArrayList<>();
// 获取系统配置的分布式JOIN算法
JoinDistributionType joinDistributionType = getJoinDistributionType(session);
// 计算各种排列和算法的可能代价
if (joinDistributionType.canRepartition() && !joinNode.isCrossJoin()) {
    possibleJoinNodes.add(createJoinEnumerationResult(joinNode.withDistributionType(PARTITIONED)));
    possibleJoinNodes.add(createJoinEnumerationResult(joinNode.flipChildren().withDistributionType(PARTITIONED)));
}
if (joinDistributionType.canReplicate()) {
    possibleJoinNodes.add(createJoinEnumerationResult(joinNode.withDistributionType(REPLICATED)));
    possibleJoinNodes.add(createJoinEnumerationResult(joinNode.flipChildren().withDistributionType(REPLICATED)));
}
if (possibleJoinNodes.stream().anyMatch(UNKNOWN_COST_RESULT::equals)) {
    return UNKNOWN_COST_RESULT;
}
// 返回最小代价的组合结果
return resultComparator.min(possibleJoinNodes);
}

```

