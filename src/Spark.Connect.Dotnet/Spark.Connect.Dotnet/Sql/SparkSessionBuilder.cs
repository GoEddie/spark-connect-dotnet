using Grpc.Core;
using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql;

public class SparkSessionBuilder
{
    private readonly Dictionary<string, string> _conf = new();

    private readonly Dictionary<string, string> _sparkConnectDotnetConf = new();
    private string _bearerToken = string.Empty;

    private string _clientType = "goeddie/spark-dotnet";
    private string _clusterId = string.Empty;
    private TimeSpan _databricksConnectionMaxVerificationTime = TimeSpan.FromMinutes(10);

    private DatabricksConnectionVerification _databricksConnectionVerification = DatabricksConnectionVerification.WaitForCluster;

    private string _remote = string.Empty;
    private SparkSession? _session;
    private string _userId = string.Empty;
    private string _userName = string.Empty;

    public SparkSessionBuilder Remote(string address)
    {
        _remote = address;
        return this;
    }

    public SparkSessionBuilder Token(string bearerToken)
    {
        _bearerToken = bearerToken;
        return this;
    }

    /// <summary>
    ///     If we detect the host is at databricks *databricks* then by default when the connection is created we loop waiting
    ///     for the cluster to start. If you don't want this behaviour then set wait to false.
    /// </summary>
    /// <param name="wait"></param>
    /// <returns></returns>
    public SparkSessionBuilder DatabricksWaitForClusterOnSessionCreate(bool wait)
    {
        _databricksConnectionVerification =
            wait ? DatabricksConnectionVerification.WaitForCluster : DatabricksConnectionVerification.None;
        return this;
    }

    /// <summary>
    ///     If we are waiting for databricks clusters, what is the maximum wait time in seconds? The default is ten minutes.
    /// </summary>
    /// <param name="minutes"></param>
    /// <returns></returns>
    public SparkSessionBuilder DatabricksWaitForClusterMaxTime(int minutes)
    {
        _databricksConnectionMaxVerificationTime = TimeSpan.FromMinutes(minutes);
        return this;
    }

    /// <summary>
    ///     Sets ClusterID - if ClusterId is in the profile then whichever you call last wins
    /// </summary>
    /// <param name="clusterId"></param>
    /// <returns></returns>
    public SparkSessionBuilder ClusterId(string clusterId)
    {
        _clusterId = clusterId;
        return this;
    }

    public SparkSessionBuilder ClientType(string clientType)
    {
        _clientType = clientType;
        return this;
    }

    public SparkSessionBuilder UserName(string userName)
    {
        _userName = userName;
        return this;
    }

    public SparkSessionBuilder UserId(string userId)
    {
        _userId = userId;
        return this;
    }

    public SparkSessionBuilder Config(string key, string value)
    {
        if (key.StartsWith("spark.connect.dotnet.", StringComparison.OrdinalIgnoreCase))
        {
            _sparkConnectDotnetConf[key.ToLowerInvariant()] = value.ToLowerInvariant();
            return this;
        }

        _conf[key] = value;
        return this;
    }

    public SparkSessionBuilder Profile(string profileName)
    {
        var profileData = new DatabricksCfgReader().GetProfile(profileName);

        foreach (var item in profileData)
        {
            switch (item.Key)
            {
                case "host":
                    _remote = item.Value;
                    break;
                case "cluster_id":
                    _clusterId = item.Value;
                    break;
                case "token":
                    _bearerToken = item.Value;
                    break;
            }
        }

        return this;
    }

    public SparkSession GetOrCreate()
    {
        if (_session != null)
        {
            return _session;
        }

        _session = new SparkSession(Guid.NewGuid().ToString(), _remote, BuildHeaders(), BuildUserContext(), _clientType,
            _databricksConnectionVerification, _databricksConnectionMaxVerificationTime, _sparkConnectDotnetConf);
        if (_conf.Any())
        {
            Task.Run(() => GrpcInternal.ExecSetConfigCommandResponse(_session, _conf)).Wait();
        }

        return _session;
    }
    
    public SparkSession Create()
    {
        _session = new SparkSession(Guid.NewGuid().ToString(), _remote, BuildHeaders(), BuildUserContext(), _clientType, _databricksConnectionVerification, _databricksConnectionMaxVerificationTime, _sparkConnectDotnetConf);
        if (_conf.Any())
        {
            Task.Run(() => GrpcInternal.ExecSetConfigCommandResponse(_session, _conf)).Wait();
        }

        return _session;
    }

    private UserContext BuildUserContext()
    {
        if (string.IsNullOrEmpty(_userId) && string.IsNullOrEmpty(_userName))
        {
            return new UserContext();
        }

        return new UserContext
        {
            UserId = _userId, UserName = _userName
        };
    }

    private Metadata BuildHeaders()
    {
        var headers = new Metadata();
        if (!string.IsNullOrEmpty(_bearerToken))
        {
            headers.Add("Authorization", $"Bearer {_bearerToken}");
        }

        if (!string.IsNullOrEmpty(_clusterId))
        {
            headers.Add("x-databricks-cluster-id", $"{_clusterId}");
        }

        return headers;
    }
}


public static class SparkDotnetKnownConfigKeys
{
    public const string GrpcLogging = RuntimeConf.SparkDotnetConfigKey + "grpclogging";
    public const string PrintMetrics = RuntimeConf.SparkDotnetConfigKey + "showmetrics";
    public const string DontDecodeArrow = RuntimeConf.SparkDotnetConfigKey + "dontdecodearrow";
    public const string RequestExecutorCancelTimeout = RuntimeConf.SparkDotnetConfigKey + "requestretrytimelimit";
}

public class RuntimeConf
{
    public const string SparkDotnetConfigKey = "spark.connect.dotnet.";
    
    
    private readonly SparkSession _session;


    public RuntimeConf(SparkSession session, IDictionary<string, string> sparkDotnetConnectOptions)
    {
        SparkDotnetConnectOptions = sparkDotnetConnectOptions;
        _session = session;
    }

    public IDictionary<string, string> SparkDotnetConnectOptions { get; }

    public IDictionary<string, string> GetAll(string? prefix = null)
    {
        var task = Task.Run(() => GrpcInternal.ExecGetAllConfigCommandResponse(_session, prefix));
        task.Wait();
        return task.Result;
    }

    public void Set(string key, string value)
    {
        if (key.ToLowerInvariant().StartsWith(SparkDotnetConfigKey))
        {
            SparkDotnetConnectOptions[key] = value;
            return;
        }

        var dict = new Dictionary<string, string>
        {
            { key, value }
        };

        var task = Task.Run(() => GrpcInternal.ExecSetConfigCommandResponse(_session, dict));
        task.Wait();
    }

    public void Unset(string key)
    {
        if (key.ToLowerInvariant().StartsWith(SparkDotnetConfigKey))
        {
            SparkDotnetConnectOptions.Remove(key);
            return;
        }


        var task = Task.Run(() => GrpcInternal.ExecUnSetConfigCommandResponse(_session, key));
        task.Wait();
    }

    public string GetOrDefault(string key, string defaultValue)
    {
        var value = Get(key);
        if (string.IsNullOrEmpty(value))
        {
            return defaultValue;
        }

        return value;
    }
    public string Get(string key)
    {
        if (key.ToLowerInvariant().StartsWith(SparkDotnetConfigKey))
        {
            if (!SparkDotnetConnectOptions.ContainsKey(key))
            {
                return string.Empty;
            }

            return SparkDotnetConnectOptions[key];
        }

        var task = Task.Run(() => GrpcInternal.ExecGetAllConfigCommandResponse(_session));
        task.Wait();

        var config = task.Result;
        return
            config.TryGetValue(key, out var value)
                ? value
                : string.Empty;
    }

    public bool IsTrue(string key)
    {
        var value = Get(key).ToLowerInvariant();
        if (string.IsNullOrEmpty(value))
        {
            return false;
        }

        if (value == "true")
        {
            return true;
        }

        if (value == "1")
        {
            return true;
        }

        return false;
    }
}

public enum DatabricksConnectionVerification
{
    None
    , WaitForCluster
}