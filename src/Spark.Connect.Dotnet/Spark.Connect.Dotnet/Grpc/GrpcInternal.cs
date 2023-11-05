using System.Text;
using Apache.Arrow.Ipc;
using Grpc.Core;
using Spark.Connect;

namespace Spark.Connect.Dotnet.Grpc;

static class GrpcInternal
{
    static async Task  DumpArrowBatch(ExecutePlanResponse.Types.ArrowBatch batch)
    {
        var reader = new ArrowStreamReader(new ReadOnlyMemory<byte>(batch.Data.ToByteArray()));
        Logger.WriteLine($"arrow schema: {reader.Schema}");
    
        var recordBatch = await reader.ReadNextRecordBatchAsync();
        Logger.WriteLine("arrow: Read record batch with {0} column(s)", recordBatch.ColumnCount);
        foreach (var array in recordBatch.Arrays)
        {
            foreach (var buffer in array.Data.Buffers)
            {
                Logger.WriteLine("arrow buffer: " + buffer.Length);
                Logger.WriteLine($"arrow buffer toString:");
                Console.WriteLine(ASCIIEncoding.ASCII.GetString(buffer.Span));
            }
        }
    }
    
    public static async Task<Relation> Exec(SparkConnectService.SparkConnectServiceClient client, string sessionId, Plan plan)
    {
        var executeRequest = new ExecutePlanRequest
        {
            Plan = plan, SessionId = sessionId
        };
        var execResponse = client.ExecutePlan(executeRequest, new Metadata());
        await execResponse.ResponseStream.MoveNext(new CancellationToken());
        var current = execResponse.ResponseStream.Current;

        Relation? dataframe = null;

        while (current != null)
        {
            if (current?.Extension != null)
            {
                Logger.WriteLine("Extension");
            }

            if (current?.SqlCommandResult != null)
            {
                Logger.WriteLine($"SqlCommandResult: {current.SqlCommandResult.Relation}");
            
                dataframe = current.SqlCommandResult.Relation;
            }
            if (current?.Metrics != null)
            {
                Logger.WriteLine($"metrics: {current.Metrics}");
            }

            if (current?.Schema != null)
            {
                Logger.WriteLine($"schema: {current.Schema}");
            }

            if (!string.IsNullOrEmpty(current?.ResponseId))
            {
                Logger.WriteLine($"response id: {current.ResponseId}");
            }

            if (current?.ResultComplete != null)
            {
                Logger.WriteLine("result complete");
            }

            if (current?.GetResourcesCommandResult != null)
            {
                Logger.WriteLine("get resources");
            }

            if (current?.StreamingQueryCommandResult != null)
            {
                Logger.WriteLine("streaming query result");
            }

            if (current?.ArrowBatch != null)
            {
                await DumpArrowBatch(current.ArrowBatch);
            }

            if (!String.IsNullOrEmpty(current?.SessionId))
            {
                Logger.WriteLine($"Session ID: {current.SessionId}");
            }

            if (current?.ResultComplete != null)
            {
                Logger.WriteLine($"Response Complete: {current.ResultComplete}");
            }

            if (!string.IsNullOrEmpty(current?.OperationId))
            {
                Logger.WriteLine($"Operation ID: {current.OperationId}");
            }

            if (current?.ResponseTypeCase != null)
            {
                Logger.WriteLine($"ResponsetypeCase: {current.ResponseTypeCase}");
            }

            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        if (dataframe == null)
        {
            Logger.WriteLine("EXEC Relation is NULL");
        }
        return dataframe ?? plan.Root;
    }
    
    public static async Task<ExecutePlanResponse.Types.ArrowBatch> ExecArrowResponse(SparkConnectService.SparkConnectServiceClient client, string sessionId, Plan plan)
    {
        var executeRequest = new ExecutePlanRequest
        {
            Plan = plan, SessionId = sessionId
        };
        var execResponse = client.ExecutePlan(executeRequest, new Metadata());
        await execResponse.ResponseStream.MoveNext(new CancellationToken());
        var current = execResponse.ResponseStream.Current;

        Relation? dataframe = null;

        while (current != null)
        {
            if (current?.Extension != null)
            {
                Logger.WriteLine("Extension");
            }

            if (current?.SqlCommandResult != null)
            {
                Logger.WriteLine($"SqlCommandResult: {current.SqlCommandResult.Relation}");
            
                dataframe = current.SqlCommandResult.Relation;
            }
            if (current?.Metrics != null)
            {
                Logger.WriteLine($"metrics: {current.Metrics}");
            }

            if (current?.Schema != null)
            {
                Logger.WriteLine($"schema: {current.Schema}");
            }

            if (!string.IsNullOrEmpty(current?.ResponseId))
            {
                Logger.WriteLine($"response id: {current.ResponseId}");
            }

            if (current?.ResultComplete != null)
            {
                Logger.WriteLine("result complete");
            }

            if (current?.GetResourcesCommandResult != null)
            {
                Logger.WriteLine("get resources");
            }

            if (current?.StreamingQueryCommandResult != null)
            {
                Logger.WriteLine("streaming query result");
            }

            if (current?.ArrowBatch != null)
            {
                return current.ArrowBatch;
            }

            if (!String.IsNullOrEmpty(current?.SessionId))
            {
                Logger.WriteLine($"Session ID: {current.SessionId}");
            }

            if (current?.ResultComplete != null)
            {
                Logger.WriteLine($"Response Complete: {current.ResultComplete}");
            }

            if (!string.IsNullOrEmpty(current?.OperationId))
            {
                Logger.WriteLine($"Operation ID: {current.OperationId}");
            }

            if (current?.ResponseTypeCase != null)
            {
                Logger.WriteLine($"ResponsetypeCase: {current.ResponseTypeCase}");
            }

            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        if (dataframe == null)
        {
            Logger.WriteLine("EXEC Relation is NULL");
        }
        
        //TODO TIDY THIS WHOLE METHOD UP!!!
        throw new InvalidOperationException("no arrow batch?");
    }
}