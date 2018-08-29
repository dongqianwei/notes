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



