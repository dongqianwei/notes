#presto pattern match

presto-matching包实现了优化器中Rule系统中对于PlanTree结构匹配的功能，Rule接口中的getPattern方法会返回当前Rule对应的Pattern，
如果PlanTree中存在匹配Pattern的结构，则应用该Rule。

presto优化器中的Rule都被包装在IterativeOptimizer这种优化器中，下面看IterativeOptimizer的构造函数：
```java
public IterativeOptimizer(RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, List<PlanOptimizer> legacyRules, Set<Rule<?>> newRules)
{
    this.stats = requireNonNull(stats, "stats is null");
    this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    this.legacyRules = ImmutableList.copyOf(legacyRules);
    this.ruleIndex = RuleIndex.builder()
            .register(newRules)
            .build();

    stats.registerAll(newRules);
}
```
可以看到rules会被RuleIndex的builder调用register注册：
```java
public Builder register(Set<Rule<?>> rules)
{
    rules.forEach(this::register);
    return this;
}

public Builder register(Rule<?> rule)
{
    Pattern pattern = getFirstPattern(rule.getPattern());
    if (pattern instanceof TypeOfPattern<?>) {
        rulesByRootType.put(((TypeOfPattern<?>) pattern).expectedClass(), rule);
    }
    else {
        throw new IllegalArgumentException("Unexpected Pattern: " + pattern);
    }
    return this;
}
```
注册过程首先会检查当前pattern链中的第一个pattern是不是TypeOfPattern类型，如果不是则抛出异常。
说明所有合法的pattern链中的第一个pattern都必须是TypeOfPattern类型，即限制匹配的PlanNode类型的Pattern（在后面介绍）。

然后以TypeOfPattern中期望的类(expectedClass)为key将rule保存到rulesByRootType中。

下面看具体的优化过程中是怎么使用pattern match的：
首先看优化器的optimize方法：
```java
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
...

        Memo memo = new Memo(idAllocator, plan);
        Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));
        Matcher matcher = new PlanNodeMatcher(lookup);

        Duration timeout = SystemSessionProperties.getOptimizerTimeout(session);
        Context context = new Context(memo, lookup, idAllocator, symbolAllocator, System.nanoTime(), timeout.toMillis(), session);
        exploreGroup(memo.getRootGroup(), context, matcher);

        return memo.extract();
    }
```
首先将planNode转为Memo结构，这样做的原因是planNode是immutable的，如果直接修改planNode中的子节点时需要将整个planTree重新构造一遍，
资源消耗太大，因此设计了Memo结构将PlanNode的结构拆分开保存，具体实现看Memo代码。

构造LoopUp方便从Memo中查找节点。

构造PlanNodeMatcher，这个类就是presto-matching包中的核心之一，在后面解释。

调用exploreGroup进行实际的优化步骤，第一个参数是memo.getRootGroup()，这是planNode转为Memo结构后根planNode对应的Int类型Id：

```java
private boolean exploreGroup(int group, Context context, Matcher matcher)
{
    // tracks whether this group or any children groups change as
    // this method executes
    boolean progress = exploreNode(group, context, matcher);

    while (exploreChildren(group, context, matcher)) {
        progress = true;

        // if children changed, try current group again
        // in case we can match additional rules
        if (!exploreNode(group, context, matcher)) {
            // no additional matches, so bail out
            break;
        }
    }

    return progress;
}
```
可以看到exploreGroup中首先调用了exploreNode，然后在while循环中调用exploreChildren:

```java
private boolean exploreNode(int group, Context context, Matcher matcher)
{
//首先从memo中根据Id找到对应的PlanNode
    PlanNode node = context.memo.getNode(group);

    boolean done = false;
    boolean progress = false;

    while (!done) {
        context.checkTimeoutNotExhausted();

        done = true;
        // 找到当前node可能匹配的Rules
        Iterator<Rule<?>> possiblyMatchingRules = ruleIndex.getCandidates(node).iterator();
        // 遍历所有可能匹配的Rule
        while (possiblyMatchingRules.hasNext()) {
            Rule<?> rule = possiblyMatchingRules.next();

            if (!rule.isEnabled(context.session)) {
                continue;
            }

            // 将rule应用与当前节点
            Rule.Result result = transform(node, rule, matcher, context);

            // 检查是否转换成功
            if (result.getTransformedPlan().isPresent()) {
            // 将转换后的子节点替换到memo中
                node = context.memo.replace(group, result.getTransformedPlan().get(), rule.getClass().getName());

                done = false;
                progress = true;
            }
        }
    }

    return progress;
}
```
从代码中的注释可以看到该函数分为以下几个步骤：

1. 找到当前node可能匹配的Rules

这是调用ruleIndex.getCandidates(node)方法：
```java
public Stream<Rule<?>> getCandidates(Object object)
{
    return supertypes(object.getClass())
            .flatMap(clazz -> rulesByRootType.get(clazz).stream());
}
```
首先获取到当前PlanNode类以及所有父类构成的列表，然后将rulesByRootType中所有以列表中的class为key的rule都取出来并返回。

2. 遍历所有返回的rule，调用transform将rule应用与节点：
```java
    private <T> Rule.Result transform(PlanNode node, Rule<T> rule, Matcher matcher, Context context)
    {
        Rule.Result result;

        Match<T> match = matcher.match(rule.getPattern(), node);

        if (match.isEmpty()) {
            return Rule.Result.empty();
        }

        long duration;
        try {
            long start = System.nanoTime();
            result = rule.apply(match.value(), match.captures(), ruleContext(context));
            duration = System.nanoTime() - start;
        }
        catch (RuntimeException e) {
            stats.recordFailure(rule);
            throw e;
        }
        stats.record(rule, duration, !result.isEmpty());

        return result;
    }
```
该方法首先调用matcher.match对node进行模式匹配，匹配结果保存在match中，如果匹配失败，则返回空。
下面看matcher.match的实现:
```java
default <T> Match<T> match(Pattern<T> pattern, Object object)
{
    return match(pattern, object, Captures.empty());
}

@Override
public <T> Match<T> match(Pattern<T> pattern, Object object, Captures captures)
{
    if (pattern.previous() != null) {
        Match<?> match = match(pattern.previous(), object, captures);
        return match.flatMap((value) -> pattern.accept(this, value, match.captures()));
    }
    else {
        return pattern.accept(this, object, captures);
    }
}
```
首先检查当前的pattern.previous是否为空。如果为空，说明当前pattern链中只有一个pattern，则调用pattern.accept方法。
否则先对pattern.previous()第归调用match方法，再调用当前pattern.accept方法。
Patten类有5个具体的实现：

1. CapturePattern
2. EqualsPattern
3. FilterPattern
4. TypeOfPattern
5. WithPattern

上文提到过，任何pattern链的第一个pattern必须是TypeOfPattern，首先看这个类型的accept方法：
```java
@Override
public Match<T> accept(Matcher matcher, Object object, Captures captures)
{
    return matcher.matchTypeOf(this, object, captures);
}

@Override
public <T> Match<T> matchTypeOf(TypeOfPattern<T> typeOfPattern, Object object, Captures captures)
{
    Class<T> expectedClass = typeOfPattern.expectedClass();
    if (expectedClass.isInstance(object)) {
        return Match.of(expectedClass.cast(object), captures);
    }
    else {
        return Match.empty();
    }
}
```
该方法随后调用了matcher的matchTypeOf方法，其检查object是否为typeOfPattern.expectedClass()类型。
如果是，则返回Match.of(expectedClass，s.cast(object), captures)，否则返回Match.empty()。

可以看出来这种Pattern是为了限制PlanNode的类型，如果类型匹配失败，返回Match.empty()。

同样，EqualsPattern的accept方法也是调用了matcher.matchEquals方法：
```java
@Override
public <T> Match<T> matchEquals(EqualsPattern<T> equalsPattern, Object object, Captures captures)
{
    return Match.of((T) object, captures).filter(equalsPattern.expectedValue()::equals);
}

@Override
public Match<T> filter(Predicate<? super T> predicate)
{
    return predicate.test(value) ? this : empty();
}

```
该方法首先构造了一个包含object的match，然后调用filter方法，以equalsPattern.expectedValue()::equals为predicate来filter。
如果object和equalsPattern.expectedValue()相等，则返回构造的match，否则返回empty。

FilterPattern.accept同样调用了matcher.matchFilter方法：
```java
@Override
public <T> Match<T> matchFilter(FilterPattern<T> filterPattern, Object object, Captures captures)
{
    return Match.of((T) object, captures).filter(filterPattern.predicate());
}
```
该方法直接以filterPattern.predicate()作为predicate过滤，满足条件则返回构造的match对象，否则返回empty()

WithPattern.accept方法调用matcher.matchWith，该方法实现比较复杂，首先来看WithPattern的用法。
WithPattern的构造函数传入两个参数，分别为PropertyPattern<? super T, ?> propertyPattern
和Pattern<T> previous。

代码中唯一调用该构造函数的地方为Pattern.with方法：
```java
public Pattern<T> with(PropertyPattern<? super T, ?> pattern)
{
    return new WithPattern<>(pattern, this);
}
```

看一个具体的使用场景MergeLimits.Pattern:
```java
private static final Pattern<LimitNode> PATTERN = limit()
            .with(source().matching(limit().capturedAs(CHILD)));
```
首先调用limit():
```java
public static Pattern<LimitNode> limit()
{
    return typeOf(LimitNode.class);
}

public static <T> Pattern<T> typeOf(Class<T> expectedClass)
{
    return new TypeOfPattern<>(expectedClass);
}
```
创建一个TypeOfPattern作为pattern链第一个节点（符合前文提到的pattern链第一个节点类型的限制）。
然后调用with，传入参数为source().matching(limit().capturedAs(CHILD))。

上面提到过with里面调用了WithPattern的构造函数，传入参数分别为一个PropertyPattern和当前Pattern作为新创建Pattern的previous Pattern。
PropertyPattern参数，也就是source().matching(limit().capturedAs(CHILD))，究竟是个什么呢？

先看source():
```java
public static Property<PlanNode, PlanNode> source()
{
    return optionalProperty("source", node -> node.getSources().size() == 1 ?
            Optional.of(node.getSources().get(0)) :
            empty());
}

public static <F, T> Property<F, T> optionalProperty(String name, Function<F, Optional<T>> function)
{
    return new Property<>(name, function);
}

    public Property(String name, Function<F, Optional<T>> function)
{
    this.name = name;
    this.function = function;
}
```
source方法返回的是一个Property对象，其中包含一个String类型的name，这里是source，
和一个Function对象，Function对象这里是一个会调函数，作用是从给定的PlanNode中获取节点的第一个source子节点，如果没有返回空。
可以推断出来Property的作用是表示PlanNode的一个属性，可以通过Property里的function回调函数将具体的属性提取出来。

然后在source()上调用matching，参数又是一个Pattern：limit().capturedAs(CHILD)。
```java
public <R> PropertyPattern<F, R> matching(Pattern<R> pattern)
{
    return PropertyPattern.of(this, pattern);
}
```
matching函数传入了一个新的pattern，然后构造了一个PropertyPattern对象，这个节点包含了一个Property对象和一个Pattern。
最终这个PropertyPattern对象作为with的参数，构造出最终的PropertyPattern对象。

那么现在PropertyPattern对象是怎么构造出来的请出来，下面看PropertyPattern.accept的实现：
```java
    @Override
    public <T> Match<T> matchWith(WithPattern<T> withPattern, Object object, Captures captures)
    {
        Function<? super T, Optional<?>> property = withPattern.getProperty().getFunction();
        Optional<?> propertyValue = property.apply((T) object);

        Optional<?> resolvedValue = propertyValue
                .map(value -> value instanceof GroupReference ? lookup.resolve(((GroupReference) value)) : value);

        Match<?> propertyMatch = resolvedValue
                .map(value -> match(withPattern.getPattern(), value, captures))
                .orElse(Match.empty());
        return propertyMatch.map(ignored -> (T) object);
    }
```
该方法首先调用withPattern.getProperty().getFunction()获取到用来提取property的回调函数，然后应用于object将具体的property提取出来。
如果propertyValue属于GroupReference的话说明该property是Memo结构中的索引，则通过lookup.resolve将具体引用的作为property的PlanNode取出来。
然后将提取出来的PlanNode用withPattern.getPattern()获取的Pattern匹配并返回结果。

根据上述分析可知，WithPattern的功能是对当前PlanNode的属性作进一步的匹配。

