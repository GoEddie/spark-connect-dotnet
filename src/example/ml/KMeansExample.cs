using Spark.Connect.Dotnet.ML.Clustering;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace ml;

/// <summary>
/// K-Means Clustering Example
/// Demonstrates unsupervised learning to group similar data points
/// </summary>
public static class KMeansExample
{
    public static void Run(SparkSession spark)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("K-Means Clustering Example");
        Console.WriteLine("========================================\n");

        // Create sample data: customers with spending patterns
        // Features: annual_income (thousands), spending_score (1-100)
        var customerData = new List<(int id, double income, double spendingScore)>
        {
            // Low income, low spending (cluster 1)
            (1, 15, 39), (2, 16, 6), (3, 17, 40), (4, 18, 8), (5, 19, 31),

            // Low income, high spending (cluster 2)
            (6, 15, 77), (7, 16, 76), (8, 17, 94), (9, 18, 72), (10, 19, 99),

            // Medium income, medium spending (cluster 3)
            (11, 39, 36), (12, 40, 42), (13, 42, 52), (14, 43, 47), (15, 44, 50),

            // High income, low spending (cluster 4)
            (16, 70, 29), (17, 72, 14), (18, 75, 32), (19, 77, 17), (20, 78, 25),

            // High income, high spending (cluster 5)
            (21, 69, 73), (22, 71, 88), (23, 73, 93), (24, 75, 81), (25, 78, 95),
        };

        var df = spark.CreateDataFrame(
            customerData.Select(d => new object[] { d.id, d.income, d.spendingScore }),
            new[] { "id", "income", "spending_score" }
        );

        Console.WriteLine("=== Customer Data ===");
        df.Show();

        // Step 1: Assemble features into a vector
        var assembler = new VectorAssembler(spark);
        assembler.SetInputCols(new[] { "income", "spending_score" });
        assembler.SetOutputCol("features");

        var assembledData = assembler.Transform(df);
        Console.WriteLine("\n=== After VectorAssembler ===");
        assembledData.Select("id", "income", "spending_score", "features").Show();

        // Step 2: Scale features using StandardScaler for better clustering
        var scaler = new StandardScaler();
        scaler.SetInputCol("features");
        scaler.SetOutputCol("scaledFeatures");
        scaler.SetWithMean(true);
        scaler.SetWithStd(true);

        var scalerModel = scaler.Fit(assembledData);
        var scaledData = scalerModel.Transform(assembledData);

        Console.WriteLine("\n=== After StandardScaler ===");
        scaledData.Select("id", "features", "scaledFeatures").Show(truncate: 40);

        // Step 3: Train K-Means model with 5 clusters
        var kmeans = new KMeans();
        kmeans.SetK(5);
        kmeans.SetFeaturesCol("scaledFeatures");
        kmeans.SetPredictionCol("cluster");
        kmeans.SetMaxIter(20);
        kmeans.SetSeed(42L);

        Console.WriteLine("\n=== Training K-Means with 5 clusters ===");
        var model = kmeans.Fit(scaledData);

        // Step 4: Make predictions
        var predictions = model.Transform(scaledData);

        Console.WriteLine("\n=== Cluster Assignments ===");
        predictions
            .Select("id", "income", "spending_score", "cluster")
            .OrderBy("cluster", "id")
            .Show(25);

        // Step 5: Analyze clusters
        Console.WriteLine("\n=== Cluster Statistics ===");
        predictions
            .GroupBy("cluster")
            .Agg(
                Count("*").Alias("count"),
                Round(Avg("income"), Lit(1)).Alias("avg_income"),
                Round(Avg("spending_score"), Lit(1)).Alias("avg_spending")
            )
            .OrderBy("cluster")
            .Show();

        // Describe each cluster
        Console.WriteLine("\n=== Cluster Interpretation ===");
        var clusterStats = predictions
            .GroupBy("cluster")
            .Agg(
                Avg("income").Alias("avg_income"),
                Avg("spending_score").Alias("avg_spending")
            )
            .Collect();

        foreach (var row in clusterStats)
        {
            var cluster = row[0];
            var avgIncome = Convert.ToDouble(row[1]);
            var avgSpending = Convert.ToDouble(row[2]);

            var incomeLevel = avgIncome < 30 ? "Low" : avgIncome < 60 ? "Medium" : "High";
            var spendingLevel = avgSpending < 40 ? "Low" : avgSpending < 60 ? "Medium" : "High";

            Console.WriteLine($"Cluster {cluster}: {incomeLevel} Income, {spendingLevel} Spending (avg income: ${avgIncome:F0}k, avg spending: {avgSpending:F0})");
        }

        Console.WriteLine("\nK-Means Clustering Example completed!");
    }
}
