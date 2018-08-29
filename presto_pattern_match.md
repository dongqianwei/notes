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
