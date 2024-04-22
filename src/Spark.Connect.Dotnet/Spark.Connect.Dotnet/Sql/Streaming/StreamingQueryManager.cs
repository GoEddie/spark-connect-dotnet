namespace Spark.Connect.Dotnet.Sql.Streaming;

public class StreamingQueryManager
{
    private readonly List<StreamingQuery> _active = new List<StreamingQuery>();

    public IEnumerable<StreamingQuery> Active => _active;

    public StreamingQuery Get(string id) => _active.FirstOrDefault(p => p.Id == id);

    public void Add(StreamingQuery sq)
    {
        _active.Add(sq);
    }

    public void Remove(StreamingQuery sq) => _active.Remove(sq);

    public bool AwaitAnyTermination(int? timeout = null)
    {
        var tasks = _active.Select(p => p.AwaitTerminationAsync(timeout)).ToArray();
        var completed = Task.WaitAny(tasks);
        return tasks[completed].Result;
    }
}