using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Clustering;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.ML.Regression;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;

namespace ml;

/// <summary>
/// ML Model Properties Example
/// Demonstrates accessing model properties for various ML models
/// Shows coefficients, intercepts, feature importances, and other model attributes
/// </summary>
public static class MLModelPropertiesExample
{
    public static void Run(SparkSession spark)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("ML Model Properties Example");
        Console.WriteLine("========================================\n");

        // Run each model example
        LogisticRegressionProperties(spark);
        Console.WriteLine();

        LinearRegressionProperties(spark);
        Console.WriteLine();

        KMeansProperties(spark);
        Console.WriteLine();

        DecisionTreeProperties(spark);
        Console.WriteLine();

        NaiveBayesProperties(spark);
        Console.WriteLine();

        StringIndexerLabels(spark);

        Console.WriteLine("\nML Model Properties Example completed!");
    }

    private static void LogisticRegressionProperties(SparkSession spark)
    {
        Console.WriteLine("=== Logistic Regression Model Properties ===\n");

        var data = new List<(double label, DenseVector features)>
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1])),
            (0.0, new DenseVector([2.0, 1.0, -1.0])),
            (0.0, new DenseVector([2.0, 1.3, 1.0])),
            (1.0, new DenseVector([0.0, 1.2, -0.5]))
        };

        var schema = new StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false)
        });

        var training = spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var lr = new LogisticRegression();
        lr.SetMaxIter(10);
        lr.SetRegParam(0.01);

        var model = lr.Fit(training);

        // Access model properties
        Console.WriteLine($"Coefficients: [{string.Join(", ", model.Coefficients.Select(c => c.ToString("F4")))}]");
        Console.WriteLine($"Intercept: {model.Intercept:F4}");
        Console.WriteLine($"NumClasses: {model.NumClasses}");
        Console.WriteLine($"NumFeatures: {model.NumFeatures}");
    }

    private static void LinearRegressionProperties(SparkSession spark)
    {
        Console.WriteLine("=== Linear Regression Model Properties ===\n");

        var data = new List<(double label, DenseVector features)>
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1])),
            (0.0, new DenseVector([2.0, 1.0, -1.0])),
            (0.0, new DenseVector([2.0, 1.3, 1.0])),
            (1.0, new DenseVector([0.0, 1.2, -0.5]))
        };

        var schema = new StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false)
        });

        var training = spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var lr = new LinearRegression();
        lr.SetMaxIter(10);

        var model = lr.Fit(training);

        // Access model properties
        Console.WriteLine($"Coefficients: [{string.Join(", ", model.Coefficients.Select(c => c.ToString("F4")))}]");
        Console.WriteLine($"Intercept: {model.Intercept:F4}");
        Console.WriteLine($"NumFeatures: {model.NumFeatures}");
    }

    private static void KMeansProperties(SparkSession spark)
    {
        Console.WriteLine("=== KMeans Model Properties ===\n");

        var data = new List<(DenseVector features, int dummy)>
        {
            (new DenseVector([0.0, 0.0]), 0),
            (new DenseVector([1.0, 1.0]), 0),
            (new DenseVector([9.0, 8.0]), 0),
            (new DenseVector([8.0, 9.0]), 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("features", new VectorUDT(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var kmeans = new KMeans();
        kmeans.SetK(2);
        kmeans.SetMaxIter(20);
        kmeans.SetSeed(1L);

        var model = kmeans.Fit(df);

        // Access model properties
        Console.WriteLine($"K (number of clusters): {model.K}");
        Console.WriteLine("Cluster Centers:");
        for (int i = 0; i < model.ClusterCenters.Count; i++)
        {
            var center = model.ClusterCenters[i];
            Console.WriteLine($"  Cluster {i}: [{string.Join(", ", center.Select(c => c.ToString("F2")))}]");
        }

        // Test single vector prediction
        var testVector = new DenseVector([0.5, 0.5]);
        var prediction = model.Predict(testVector);
        Console.WriteLine($"\nPredict([0.5, 0.5]): Cluster {prediction}");
    }

    private static void DecisionTreeProperties(SparkSession spark)
    {
        Console.WriteLine("=== Decision Tree Classifier Model Properties ===\n");

        var data = new List<(double label, DenseVector features)>
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1])),
            (0.0, new DenseVector([2.0, 1.0, -1.0])),
            (0.0, new DenseVector([2.0, 1.3, 1.0])),
            (1.0, new DenseVector([0.0, 1.2, -0.5]))
        };

        var schema = new StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false)
        });

        var training = spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var dt = new DecisionTreeClassifier();
        dt.SetMaxDepth(5);

        var model = dt.Fit(training);

        // Access model properties
        Console.WriteLine($"Depth: {model.Depth}");
        Console.WriteLine($"NumNodes: {model.NumNodes}");
        Console.WriteLine($"NumFeatures: {model.NumFeatures}");
        Console.WriteLine($"NumClasses: {model.NumClasses}");
        Console.WriteLine($"FeatureImportances: [{string.Join(", ", model.FeatureImportances.Select(f => f.ToString("F4")))}]");
        Console.WriteLine($"\nTree Debug String:\n{model.ToDebugString}");
    }

    private static void NaiveBayesProperties(SparkSession spark)
    {
        Console.WriteLine("=== Naive Bayes Model Properties ===\n");

        var data = new List<(double label, DenseVector features, float weight)>
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1]), 0.1F),
            (0.0, new DenseVector([2.0, 1.0, 1.0]), 0.5F),
            (0.0, new DenseVector([2.0, 1.3, 1.0]), 1.0F),
            (1.0, new DenseVector([0.0, 1.2, 0.5]), 1.0F)
        };

        var schema = new StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false),
            new StructField("weight", new FloatType(), false)
        });

        var training = spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var nb = new NaiveBayes();
        nb.SetFeaturesCol("features");

        var model = nb.Fit(training);

        // Access model properties
        Console.WriteLine($"NumClasses: {model.NumClasses}");
        Console.WriteLine($"NumFeatures: {model.NumFeatures}");
        Console.WriteLine($"Pi (log class priors): [{string.Join(", ", model.Pi.Select(p => p.ToString("F4")))}]");
        Console.WriteLine("Theta (log class conditionals):");
        for (int i = 0; i < model.Theta.Count; i++)
        {
            var row = model.Theta[i];
            Console.WriteLine($"  Class {i}: [{string.Join(", ", row.Select(t => t.ToString("F4")))}]");
        }
    }

    private static void StringIndexerLabels(SparkSession spark)
    {
        Console.WriteLine("=== StringIndexer Model Properties ===\n");

        var data = new List<(double label, string fruit)>
        {
            (1.0, "apple"),
            (0.0, "banana"),
            (1.0, "apple"),
            (0.0, "cherry")
        };

        var schema = new StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("fruit", new StringType(), false)
        });

        var training = spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var indexer = new StringIndexer();
        indexer.SetInputCol("fruit");
        indexer.SetOutputCol("indexed");

        var model = indexer.Fit(training);

        // Access model properties
        var labels = model.Labels;
        Console.WriteLine($"Labels (ordered by frequency): [{string.Join(", ", labels)}]");
        Console.WriteLine("\nLabel to Index mapping:");
        for (int i = 0; i < labels.Length; i++)
        {
            Console.WriteLine($"  '{labels[i]}' -> {i}");
        }
    }
}
