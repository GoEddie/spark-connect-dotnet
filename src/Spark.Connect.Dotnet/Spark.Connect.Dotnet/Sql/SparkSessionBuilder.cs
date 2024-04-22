using Grpc.Core;

namespace Spark.Connect.Dotnet.Sql;

public class SparkSessionBuilder
{
    private SparkSession? _session;
    
    private string _remote = String.Empty;
    private string _bearerToken = String.Empty;
    private string _clusterId = String.Empty;
    
    private string _clientType = "goeddie-spark-dotnet";
    private string _userName = String.Empty;
    private string _userId = String.Empty;

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

        _session = new SparkSession(Guid.NewGuid().ToString(), _remote, BuildHeaders(), BuildUserContext(), _clientType);
        return _session;
    }

    private UserContext BuildUserContext()
    {
        if (string.IsNullOrEmpty(_userId) && string.IsNullOrEmpty(_userName))
        {
            return new UserContext();
        }
        
        return new UserContext()
        {
            UserId = _userId,
            UserName = _userName
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