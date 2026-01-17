using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace ml;

/// <summary>
/// Random Forest Classifier Example
/// Demonstrates ensemble learning for classification
/// Predicts wine quality based on chemical properties
/// </summary>
public static class RandomForestExample
{
    public static void Run(SparkSession spark)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("Random Forest Classifier Example");
        Console.WriteLine("========================================\n");

        // Create sample wine quality data
        // Features: alcohol, acidity, sugar, pH
        // Label: quality (0 = poor, 1 = average, 2 = good)
        var wineData = new List<(int id, double alcohol, double acidity, double sugar, double pH, int quality)>
        {
            // Poor quality wines (0)
            (1, 9.0, 0.9, 3.0, 3.5, 0),
            (2, 8.5, 0.95, 3.5, 3.6, 0),
            (3, 9.2, 0.88, 2.8, 3.55, 0),
            (4, 8.8, 0.92, 3.2, 3.52, 0),
            (5, 9.1, 0.87, 2.9, 3.48, 0),
            (6, 8.7, 0.91, 3.3, 3.58, 0),

            // Average quality wines (1)
            (7, 10.5, 0.65, 2.2, 3.3, 1),
            (8, 10.8, 0.62, 2.0, 3.25, 1),
            (9, 11.0, 0.58, 1.8, 3.2, 1),
            (10, 10.2, 0.68, 2.4, 3.35, 1),
            (11, 10.6, 0.64, 2.1, 3.28, 1),
            (12, 10.9, 0.60, 1.9, 3.22, 1),

            // Good quality wines (2)
            (13, 12.5, 0.45, 1.2, 3.1, 2),
            (14, 13.0, 0.42, 1.0, 3.05, 2),
            (15, 12.8, 0.40, 1.1, 3.08, 2),
            (16, 13.2, 0.38, 0.9, 3.0, 2),
            (17, 12.6, 0.44, 1.15, 3.12, 2),
            (18, 13.1, 0.41, 0.95, 3.02, 2),
        };

        var df = spark.CreateDataFrame(
            wineData.Select(d => new object[] { d.id, d.alcohol, d.acidity, d.sugar, d.pH, d.quality }),
            new[] { "id", "alcohol", "acidity", "sugar", "ph", "quality" }
        );

        Console.WriteLine("=== Wine Quality Dataset ===");
        df.Show();

        // Show class distribution
        Console.WriteLine("\n=== Quality Distribution ===");
        df.GroupBy("quality")
            .Agg(
                Count("*").Alias("count"),
                Round(Avg("alcohol"), Lit(2)).Alias("avg_alcohol"),
                Round(Avg("acidity"), Lit(2)).Alias("avg_acidity")
            )
            .OrderBy("quality")
            .Show();

        // Step 1: Assemble features
        var assembler = new VectorAssembler(spark);
        assembler.SetInputCols(new[] { "alcohol", "acidity", "sugar", "ph" });
        assembler.SetOutputCol("features");

        var assembledData = assembler.Transform(df);
        var labeledData = assembledData.WithColumn("label", Col("quality").Cast("double"));

        Console.WriteLine("\n=== After VectorAssembler ===");
        labeledData.Select("id", "features", "label").Show(truncate: 40);

        // Step 2: Split data
        var trainingData = labeledData.Where(Col("id") <= 15);
        var testData = labeledData.Where(Col("id") > 15);

        Console.WriteLine($"\n=== Data Split ===");
        Console.WriteLine($"Training samples: {trainingData.Count()}");
        Console.WriteLine($"Test samples: {testData.Count()}");

        // Step 3: Train Random Forest model
        var rf = new RandomForestClassifier();
        rf.SetNumTrees(20);
        rf.SetMaxDepth(5);
        rf.SetFeaturesCol("features");
        rf.SetLabelCol("label");
        rf.SetSeed(42L);

        Console.WriteLine("\n=== Training Random Forest (20 trees, max depth 5) ===");
        var model = rf.Fit(trainingData);

        // Step 4: Make predictions
        var predictions = model.Transform(testData);

        Console.WriteLine("\n=== Predictions on Test Data ===");
        predictions
            .Select("id", "alcohol", "acidity", "sugar", "label", "prediction", "probability")
            .Show(truncate: 40);

        // Calculate accuracy
        var correct = predictions.Where(Col("label") == Col("prediction")).Count();
        var total = predictions.Count();
        var accuracy = (double)correct / total;

        Console.WriteLine("\n=== Model Evaluation ===");
        Console.WriteLine($"Correct: {correct}/{total}");
        Console.WriteLine($"Accuracy: {accuracy:P2}");

        // Step 5: Classify new wines
        Console.WriteLine("\n=== Classifying New Wines ===");

        var newWines = new List<(int id, double alcohol, double acidity, double sugar, double pH)>
        {
            (101, 8.9, 0.89, 3.1, 3.54),   // Should be poor (0)
            (102, 10.7, 0.63, 2.1, 3.27),  // Should be average (1)
            (103, 12.9, 0.41, 1.05, 3.06), // Should be good (2)
        };

        var newDf = spark.CreateDataFrame(
            newWines.Select(d => new object[] { d.id, d.alcohol, d.acidity, d.sugar, d.pH }),
            new[] { "id", "alcohol", "acidity", "sugar", "ph" }
        );

        var newAssembled = assembler.Transform(newDf);
        var newPredictions = model.Transform(newAssembled);

        newPredictions
            .Select(
                Col("id"),
                Col("alcohol"),
                Col("acidity"),
                Col("prediction").Alias("predicted_quality"),
                Col("probability")
            )
            .Show(truncate: 50);

        // Map quality to labels
        Console.WriteLine("Quality mapping: 0=Poor, 1=Average, 2=Good");

        Console.WriteLine("\nRandom Forest Example completed!");
    }
}
