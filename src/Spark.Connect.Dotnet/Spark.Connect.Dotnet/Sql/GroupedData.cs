using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql;


public class GroupedData
{
    public readonly SparkSession _session;
    public readonly Relation _relation;
    private readonly IEnumerable<Expression> _groupingExpressions;
    private readonly Aggregate.Types.GroupType _groupType;
    protected internal readonly Relation Relation;
    
    public GroupedData(SparkSession session, Relation relation, IEnumerable<Expression> groupingExpressions, Aggregate.Types.GroupType groupType)
    {
        _session = session;
        _relation = relation;
        _groupingExpressions = groupingExpressions;
        _groupType = groupType;
    }

    public DataFrame Agg(params Column[] exprs)
    {
        var plan = new Plan()
        {
            Root = new Relation()
            {
                Aggregate = new Aggregate()
                {
                    Input = _relation,
                    AggregateExpressions = { exprs.Select(p => p.Expression) },
                    GroupingExpressions = { _groupingExpressions },
                    GroupType = _groupType
                    
                }
            }
        };

        return new DataFrame(_session, GrpcInternal.Exec(_session, plan));
    }

    public DataFrame Agg(Dictionary<string, string> exprs)
    {
        throw new NotImplementedException();
    }


}