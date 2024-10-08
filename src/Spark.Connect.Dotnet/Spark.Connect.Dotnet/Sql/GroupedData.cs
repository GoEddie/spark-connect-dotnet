namespace Spark.Connect.Dotnet.Sql;

public class GroupedData
{
    private readonly IEnumerable<Expression> _groupingExpressions;
    private readonly Aggregate.Types.GroupType _groupType;
    private readonly Aggregate.Types.Pivot? _pivot;
    public readonly SparkSession _session;
    protected internal Relation Relation;

    internal GroupedData(SparkSession session, Relation relation, IEnumerable<Expression> groupingExpressions,
        Aggregate.Types.GroupType groupType)
    {
        _session = session;
        Relation = relation;
        _groupingExpressions = groupingExpressions;
        _groupType = groupType;
    }

    internal GroupedData(SparkSession session, Relation relation, IEnumerable<Expression> groupingExpressions,
        Aggregate.Types.Pivot pivot, Aggregate.Types.GroupType groupType)
    {
        _session = session;
        Relation = relation;
        _groupingExpressions = groupingExpressions;
        _pivot = pivot;
        _groupType = groupType;
    }

    public DataFrame Agg(params Column[] exprs)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Aggregate = new Aggregate
                {
                    Input = Relation, AggregateExpressions = { exprs.Select(p => p.Expression) }, GroupingExpressions = { _groupingExpressions }, GroupType = _groupType
                }
            }
        };

        return new DataFrame(_session, plan.Root);
    }

    public DataFrame Agg(Dictionary<string, string> exprs)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// </summary>
    /// <param name="pivotCol"></param>
    /// <param name="values">MUST be created using Lit() i.e. Lit(123) or Lit("str")</param>
    /// <returns></returns>
    public GroupedData Pivot(string pivotCol, params Column[] values)
    {
        var pivot = new Aggregate.Types.Pivot
        {
            Values = { values.Select(p => p.Expression.Literal) }, Col = new Expression
            {
                UnresolvedAttribute = new Expression.Types.UnresolvedAttribute
                {
                    UnparsedIdentifier = pivotCol
                }
            }
        };

        return new GroupedData(_session, Relation, _groupingExpressions, pivot, Aggregate.Types.GroupType.Pivot);
    }

    public DataFrame NumericAgg(string function, params string[] cols)
    {
        var expressions = new List<Expression>();
        foreach (var col in cols)
        {
            expressions.Add(new Expression
            {
                UnresolvedFunction = new Expression.Types.UnresolvedFunction
                {
                    FunctionName = function, Arguments =
                    {
                        new Expression
                        {
                            UnresolvedAttribute = new Expression.Types.UnresolvedAttribute { UnparsedIdentifier = col }
                        }
                    }
                }
            });
        }

        if (!expressions.Any() && function == "count")
        {
            expressions.Add(new Expression
            {
                UnresolvedFunction = new Expression.Types.UnresolvedFunction
                {
                    FunctionName = function, Arguments =
                    {
                        new Expression
                        {
                            Literal = new Expression.Types.Literal
                            {
                                Integer = 1
                            }
                        }
                    }
                }
            });
        }


        if (_groupType == Aggregate.Types.GroupType.Groupby)
        {
            var plan = new Plan
            {
                Root = new Relation
                {
                    Aggregate = new Aggregate
                    {
                        Input = Relation, AggregateExpressions = { expressions }, GroupType = _groupType, GroupingExpressions = { _groupingExpressions }
                    }
                }
            };

            return new DataFrame(_session, plan.Root);
        }

        if (_groupType == Aggregate.Types.GroupType.Pivot)
        {
            var plan = new Plan
            {
                Root = new Relation
                {
                    Aggregate = new Aggregate
                    {
                        Input = Relation, Pivot = _pivot, GroupType = _groupType, GroupingExpressions = { _groupingExpressions }, AggregateExpressions = { expressions }
                    }
                }
            };

            return new DataFrame(_session, plan.Root);
        }

        if (_groupType == Aggregate.Types.GroupType.Rollup)
        {
            var plan = new Plan
            {
                Root = new Relation
                {
                    Aggregate = new Aggregate
                    {
                        Input = Relation, AggregateExpressions = { expressions }, GroupType = _groupType, GroupingExpressions = { _groupingExpressions }
                    }
                }
            };

            return new DataFrame(_session, plan.Root);
        }

        throw new NotImplementedException();
    }

    public DataFrame Sum(params string[] cols)
    {
        return NumericAgg("sum", cols);
    }

    public DataFrame Min(params string[] cols)
    {
        return NumericAgg("min", cols);
    }

    public DataFrame Avg(params string[] cols)
    {
        return NumericAgg("avg", cols);
    }

    public DataFrame Count(params string[] cols)
    {
        return NumericAgg("count", cols);
    }

    public DataFrame Max(params string[] cols)
    {
        return NumericAgg("max", cols);
    }

    public DataFrame Mean(params string[] cols)
    {
        return NumericAgg("mean", cols);
    }
}