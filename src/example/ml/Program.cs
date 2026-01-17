using ml;
using Spark.Connect.Dotnet.Sql;

// Spark .NET ML Examples
// Demonstrates various machine learning capabilities

var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();

Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
Console.WriteLine("║       Spark .NET Machine Learning Examples               ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════╝\n");

// 1. TF-IDF example (text similarity using NLP)
TfidfExample.Run(spark);
Console.WriteLine("\n" + new string('─', 60) + "\n");

// 2. Logistic Regression example (binary classification)
LogisticRegressionExample.Run(spark);
Console.WriteLine("\n" + new string('─', 60) + "\n");

// 3. K-Means example (unsupervised clustering)
KMeansExample.Run(spark);
Console.WriteLine("\n" + new string('─', 60) + "\n");

// 4. Linear Regression example (predicting continuous values)
LinearRegressionExample.Run(spark);
Console.WriteLine("\n" + new string('─', 60) + "\n");

// 5. Random Forest example (ensemble classification)
RandomForestExample.Run(spark);

Console.WriteLine("\n╔══════════════════════════════════════════════════════════╗");
Console.WriteLine("║            All ML Examples Completed!                    ║");
Console.WriteLine("╠══════════════════════════════════════════════════════════╣");
Console.WriteLine("║  Examples demonstrated:                                  ║");
Console.WriteLine("║  1. TF-IDF + Cosine Similarity (NLP)                     ║");
Console.WriteLine("║  2. Logistic Regression (Binary Classification)          ║");
Console.WriteLine("║  3. K-Means (Clustering)                                 ║");
Console.WriteLine("║  4. Linear Regression (Regression)                       ║");
Console.WriteLine("║  5. Random Forest (Ensemble Classification)              ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
