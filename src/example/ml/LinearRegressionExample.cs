using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.Regression;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace ml;

/// <summary>
/// Linear Regression Example
/// Demonstrates predicting continuous values (house prices)
/// </summary>
public static class LinearRegressionExample
{
    public static void Run(SparkSession spark)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("Linear Regression Example");
        Console.WriteLine("========================================\n");

        // Create sample data: house prices based on features
        // Features: sqft, bedrooms, bathrooms, age (years)
        // Label: price (thousands)
        var houseData = new List<(int id, double sqft, int bedrooms, double bathrooms, int age, double price)>
        {
            (1, 1400, 3, 2.0, 10, 245),
            (2, 1600, 3, 2.0, 15, 265),
            (3, 1700, 3, 2.5, 8, 295),
            (4, 1875, 4, 2.0, 20, 280),
            (5, 1100, 2, 1.5, 25, 195),
            (6, 1550, 3, 2.0, 12, 255),
            (7, 2350, 4, 3.0, 5, 385),
            (8, 2450, 4, 3.0, 3, 425),
            (9, 1425, 3, 2.0, 18, 235),
            (10, 1700, 3, 2.5, 6, 310),
            (11, 2200, 4, 2.5, 8, 355),
            (12, 1150, 2, 1.5, 30, 175),
            (13, 1800, 3, 2.5, 10, 305),
            (14, 1300, 2, 2.0, 22, 215),
            (15, 2100, 4, 2.5, 12, 340),
            (16, 950, 2, 1.0, 35, 145),
            (17, 1950, 4, 2.5, 7, 325),
            (18, 1650, 3, 2.0, 14, 275),
        };

        var df = spark.CreateDataFrame(
            houseData.Select(d => new object[] { d.id, d.sqft, d.bedrooms, d.bathrooms, d.age, d.price }),
            new[] { "id", "sqft", "bedrooms", "bathrooms", "age", "price" }
        );

        Console.WriteLine("=== House Price Dataset ===");
        df.Show();

        // Show correlations with price
        Console.WriteLine("\n=== Feature Statistics ===");
        df.Describe("sqft", "bedrooms", "bathrooms", "age", "price").Show();

        // Step 1: Assemble features into a vector
        var assembler = new VectorAssembler(spark);
        assembler.SetInputCols(new[] { "sqft", "bedrooms", "bathrooms", "age" });
        assembler.SetOutputCol("features");

        var assembledData = assembler.Transform(df);

        // Rename price to label
        var labeledData = assembledData.WithColumn("label", Col("price"));

        Console.WriteLine("\n=== After VectorAssembler ===");
        labeledData.Select("id", "features", "label").Show(truncate: 40);

        // Step 2: Split data
        var trainingData = labeledData.Where(Col("id") <= 14);
        var testData = labeledData.Where(Col("id") > 14);

        Console.WriteLine($"\n=== Data Split ===");
        Console.WriteLine($"Training samples: {trainingData.Count()}");
        Console.WriteLine($"Test samples: {testData.Count()}");

        // Step 3: Train Linear Regression model
        var lr = new LinearRegression();
        lr.SetMaxIter(100);
        lr.SetRegParam(0.1);
        lr.SetElasticNetParam(0.8);
        lr.SetFeaturesCol("features");
        lr.SetLabelCol("label");

        Console.WriteLine("\n=== Training Linear Regression Model ===");
        var model = lr.Fit(trainingData);

        // Step 4: Make predictions
        var predictions = model.Transform(testData);

        Console.WriteLine("\n=== Predictions on Test Data ===");
        predictions
            .Select("id", "sqft", "bedrooms", "bathrooms", "age", "label", "prediction")
            .WithColumn("error", Round(Col("prediction") - Col("label"), Lit(1)))
            .Show();

        // Calculate metrics manually
        var errorStats = predictions
            .Select(
                Avg(Abs(Col("prediction") - Col("label"))).Alias("mae"),
                Sqrt(Avg(Pow(Col("prediction") - Col("label"), Lit(2)))).Alias("rmse")
            )
            .Collect();

        Console.WriteLine("\n=== Model Evaluation ===");
        Console.WriteLine($"Mean Absolute Error: ${Convert.ToDouble(errorStats[0][0]):F2}k");
        Console.WriteLine($"Root Mean Square Error: ${Convert.ToDouble(errorStats[0][1]):F2}k");

        // Step 5: Predict price for new houses
        Console.WriteLine("\n=== Predicting Prices for New Houses ===");

        var newHouses = new List<(int id, double sqft, int bedrooms, double bathrooms, int age)>
        {
            (101, 1500, 3, 2.0, 10),  // Medium house
            (102, 2500, 4, 3.0, 2),   // Large new house
            (103, 1000, 2, 1.0, 40),  // Small old house
        };

        var newDf = spark.CreateDataFrame(
            newHouses.Select(d => new object[] { d.id, d.sqft, d.bedrooms, d.bathrooms, d.age }),
            new[] { "id", "sqft", "bedrooms", "bathrooms", "age" }
        );

        var newAssembled = assembler.Transform(newDf);
        var newPredictions = model.Transform(newAssembled);

        newPredictions
            .Select(
                Col("id"),
                Col("sqft"),
                Col("bedrooms"),
                Col("bathrooms"),
                Col("age"),
                Round(Col("prediction"), Lit(1)).Alias("predicted_price_k")
            )
            .Show();

        Console.WriteLine("\nLinear Regression Example completed!");
    }
}
