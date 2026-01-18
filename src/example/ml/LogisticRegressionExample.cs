using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace ml;

/// <summary>
/// Logistic Regression Example
/// Demonstrates binary classification using Spark .NET ML
/// Predicts whether a customer will churn based on features
/// </summary>
public static class LogisticRegressionExample
{
    public static void Run(SparkSession spark)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("Logistic Regression Classification Example");
        Console.WriteLine("========================================\n");

        // Create sample customer churn data
        // Features: tenure (months), monthly_charges, total_charges, num_support_tickets
        // Label: churned (1 = yes, 0 = no)
        var customerData = new List<(int id, double tenure, double monthlyCharges, double totalCharges, int supportTickets, int churned)>
        {
            // Customers who churned (short tenure, high charges, many support tickets)
            (1, 2, 89.99, 179.98, 5, 1),
            (2, 1, 95.50, 95.50, 8, 1),
            (3, 3, 78.00, 234.00, 6, 1),
            (4, 4, 82.50, 330.00, 7, 1),
            (5, 2, 91.00, 182.00, 4, 1),
            (6, 1, 88.00, 88.00, 9, 1),
            (7, 3, 99.00, 297.00, 5, 1),
            (8, 2, 85.00, 170.00, 6, 1),

            // Customers who stayed (longer tenure, moderate charges, fewer support tickets)
            (9, 24, 65.00, 1560.00, 1, 0),
            (10, 36, 55.00, 1980.00, 0, 0),
            (11, 48, 70.00, 3360.00, 2, 0),
            (12, 60, 45.00, 2700.00, 1, 0),
            (13, 18, 60.00, 1080.00, 2, 0),
            (14, 30, 50.00, 1500.00, 0, 0),
            (15, 42, 75.00, 3150.00, 1, 0),
            (16, 54, 58.00, 3132.00, 0, 0),
            (17, 12, 62.00, 744.00, 2, 0),
            (18, 20, 68.00, 1360.00, 1, 0),
        };

        var df = spark.CreateDataFrame(
            customerData.Select(d => new object[] { d.id, d.tenure, d.monthlyCharges, d.totalCharges, d.supportTickets, d.churned }),
            new[] { "id", "tenure", "monthly_charges", "total_charges", "support_tickets", "churned" }
        );

        Console.WriteLine("=== Customer Churn Dataset ===");
        df.Show();

        // Show class distribution
        Console.WriteLine("\n=== Class Distribution ===");
        df.GroupBy("churned")
            .Agg(Count("*").Alias("count"))
            .Show();

        // Step 1: Assemble features into a single vector
        var assembler = new VectorAssembler(spark);
        assembler.SetInputCols(new[] { "tenure", "monthly_charges", "total_charges", "support_tickets" });
        assembler.SetOutputCol("features");

        var assembledData = assembler.Transform(df);
        Console.WriteLine("\n=== After VectorAssembler ===");
        assembledData.Select("id", "features", "churned").Show(truncate: 50);

        // Rename churned to label for LogisticRegression
        var labeledData = assembledData.WithColumn("label", Col("churned").Cast("double"));

        // Step 2: Split data into training and test sets
        // Use Sample to create approximate splits (since RandomSplit isn't implemented)
        var trainingData = labeledData.Where(Col("id") <= 14); // First 14 for training
        var testData = labeledData.Where(Col("id") > 14);       // Last 4 for testing

        Console.WriteLine($"\n=== Data Split ===");
        Console.WriteLine($"Training samples: {trainingData.Count()}");
        Console.WriteLine($"Test samples: {testData.Count()}");

        // Step 3: Train Logistic Regression model
        var lr = new LogisticRegression();
        lr.SetMaxIter(100);
        lr.SetRegParam(0.01);
        lr.SetFeaturesCol("features");
        lr.SetLabelCol("label");

        Console.WriteLine("\n=== Training Logistic Regression Model ===");
        var model = lr.Fit(trainingData);

        // Step 4: Make predictions on test data
        var predictions = model.Transform(testData);

        Console.WriteLine("\n=== Predictions on Test Data ===");
        predictions.Select("id", "tenure", "monthly_charges", "support_tickets", "label", "prediction", "probability")
            .Show(truncate: 50);

        // Step 5: Evaluate the model
        var correct = predictions.Where(Col("label") == Col("prediction")).Count();
        var total = predictions.Count();
        var accuracy = (double)correct / total;

        Console.WriteLine("\n=== Model Evaluation ===");
        Console.WriteLine($"Correct predictions: {correct}");
        Console.WriteLine($"Total predictions: {total}");
        Console.WriteLine($"Accuracy: {accuracy:P2}");

        // Show confusion matrix breakdown
        Console.WriteLine("\n=== Prediction Breakdown ===");
        predictions
            .GroupBy("label", "prediction")
            .Agg(Count("*").Alias("count"))
            .OrderBy("label", "prediction")
            .Show();

        // Step 6: Make predictions on new customers
        Console.WriteLine("\n=== Predicting Churn for New Customers ===");

        var newCustomers = new List<(int id, double tenure, double monthlyCharges, double totalCharges, int supportTickets)>
        {
            (101, 3, 92.00, 276.00, 7),   // Likely to churn (short tenure, high charges, many tickets)
            (102, 36, 55.00, 1980.00, 1), // Likely to stay (long tenure, low charges, few tickets)
            (103, 6, 75.00, 450.00, 3),   // Uncertain
        };

        var newDf = spark.CreateDataFrame(
            newCustomers.Select(d => new object[] { d.id, d.tenure, d.monthlyCharges, d.totalCharges, d.supportTickets }),
            new[] { "id", "tenure", "monthly_charges", "total_charges", "support_tickets" }
        );

        var newAssembled = assembler.Transform(newDf);
        var newPredictions = model.Transform(newAssembled);

        newPredictions.Select(
            Col("id"),
            Col("tenure"),
            Col("monthly_charges"),
            Col("support_tickets"),
            Col("prediction").Alias("will_churn"),
            Col("probability")
        ).Show(truncate: 50);

        Console.WriteLine("\nLogistic Regression Example completed!");
    }
}
