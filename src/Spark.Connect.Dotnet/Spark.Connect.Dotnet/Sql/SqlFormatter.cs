namespace Spark.Connect.Dotnet.Sql;

#pragma warning disable CS0472


public static class SqlFormatter
{
    /// <summary>
    ///     Formats the SQL including the new token parsing options in SparkSession.Sql - note you can pass a DataFrame and
    ///     this will create a temp name, call df.CreateOrReplaceTempView with the
    ///     unique name and use the name in the query
    /// </summary>
    /// <param name="source">
    ///     Ensure if the input is from an external source that you validate it, we pass the SQL on with the
    ///     tokens removed, there is no validation done here
    /// </param>
    /// <param name="args">
    ///     Encure if args are from an external source that you validat them, we replace tokens in the SQL,
    ///     there is no validaton done here
    /// </param>
    /// <returns></returns>
    /// Example:
    /// "SELECT {identifier} from {dataframe} where something='{something}' and i{three}={three}", identifier=Col("id"), dataFrame=spark.Range(100).withColumn("something", Lit("something")), something="something" and i3=3)
    /// = spark.Range(100).withColumn("something", Lit("something")).CreateOrReplaceTempView(uniqueName);
    /// SELECT id from uniqueName WHERE something='something' and i3=3
    public static string Format(string source, IDictionary<string, object> args)
    {
        var outputSql = source;

        foreach (var item in args)
        {
            if (item.Value is DataFrame dataFrame)
            {
                var dfName = "__dotnet_spark_" + Guid.NewGuid().ToString().Replace("-", "");
                dataFrame.CreateOrReplaceTempView(dfName);
                outputSql = outputSql.Replace($"{{{item.Key}}}", dfName);
            }

            if (item.Value is string str)
            {
                outputSql = outputSql.Replace($"{{{item.Key}}}", $"\"{str}\"");
            }

            if (item.Value is int i)
            {
                outputSql = outputSql.Replace($"{{{item.Key}}}", i.ToString());
            }

            if (item.Value is double d)
            {
                outputSql = outputSql.Replace($"{{{item.Key}}}", d.ToString());
            }

            if (item.Value is float f)
            {
                outputSql = outputSql.Replace($"{{{item.Key}}}", f.ToString());
            }

            if (item.Value is bool b)
            {
                outputSql = outputSql.Replace($"{{{item.Key}}}", b.ToString());
            }

            if (item.Value is Column col)
            {
                if (col.Expression.UnresolvedAttribute != null)
                {
                    outputSql = outputSql.Replace($"{{{item.Key}}}",
                        col.Expression.UnresolvedAttribute.UnparsedIdentifier);
                }

                if (col.Expression.Literal != null)
                {
                    if (col.Expression.Literal.String != null)
                    {
                        outputSql = outputSql.Replace($"{{{item.Key}}}", $"\"{col.Expression.Literal.String}\"");
                    }

                    if (col.Expression.Literal.Integer != null)
                    {
                        outputSql = outputSql.Replace($"{{{item.Key}}}", $"{col.Expression.Literal.Integer}");
                    }

                    if (col.Expression.Literal.Double != null)
                    {
                        outputSql = outputSql.Replace($"{{{item.Key}}}", $"{col.Expression.Literal.Double}");
                    }

                    if (col.Expression.Literal.Float != null)
                    {
                        outputSql = outputSql.Replace($"{{{item.Key}}}", $"{col.Expression.Literal.Float}");
                    }

                    if (col.Expression.Literal.Long != null)
                    {
                        outputSql = outputSql.Replace($"{{{item.Key}}}", $"{col.Expression.Literal.Long}");
                    }
                }
            }
        }


        return outputSql;
    }
}

#pragma warning restore CS0472