# Release Notes - spark-connect-dotnet

## Version 4.1 (January 2026)

### Overview

This is a major release introducing **Declarative Pipeline Support** and comprehensive **Machine Learning (ML) expansion** for the spark-connect-dotnet library. It adds ~4,500 lines of new code including pipeline infrastructure, 50+ new ML classes, and 40+ new SQL functions for Spark 4.x compatibility.

---

## New Features

### Declarative Pipelines

A complete declarative pipeline framework built on Apache Spark's Dataflow Execution Engine:

**Pipeline Attributes:**

| Attribute | Purpose |
|-----------|---------|
| `[DeclarativePipeline]` | Mark a class as a pipeline definition with optional default catalog, database, and storage location |
| `[StreamingTable]` | Define streaming data sources with partition/clustering columns and format |
| `[MaterializedView]` | Define persistent materialized views with schema and table properties |
| `[TemporaryView]` | Define ephemeral temporary views |
| `[Sink]` | Define streaming sink outputs (e.g., Kafka) |
| `[SchemaFor]` | Link schema definition methods to tables |
| `[TableOptionsFor]` | Configure table-level properties |
| `[SqlConfFor]` | Configure SQL settings per table |

**Example Usage:**

```csharp
[DeclarativePipeline(DefaultCatalog = "main", DefaultDatabase = "books")]
public class BooksPipeline
{
    [StreamingTable]
    public DataFrame BronzeBooks(PipelineGraph graph) =>
        graph.Read.Format("json").Load("/data/books_raw.json");

    [MaterializedView]
    public DataFrame SilverBooks(PipelineGraph graph) =>
        graph.Read.Table("bronze_books").Filter("price > 0");

    [MaterializedView]
    public DataFrame GoldBooksSummary(PipelineGraph graph) =>
        graph.Read.Table("silver_books")
            .GroupBy("author")
            .Agg(Functions.Count("*").Alias("book_count"));
}
```

**Capabilities:**

- Multi-layer medallion architecture support (Bronze → Silver → Gold)
- Automatic schema inference or explicit schema definition
- Full lifecycle management: create, refresh (full/selective), dry-run
- Cross-database table definitions
- Integration with Spark's dataflow execution engine

---

### Machine Learning Expansion

#### New Classification Algorithms

- `DecisionTreeClassifier` / `DecisionTreeClassifierModel`
- `RandomForestClassifier` / `RandomForestClassifierModel`

#### New Clustering Algorithms

- `KMeans` / `KMeansModel`
- `GaussianMixture` / `GaussianMixtureModel`

#### New Feature Transformers

| Transformer | Purpose |
|-------------|---------|
| `PCA` / `PCAModel` | Principal Component Analysis |
| `Imputer` / `ImputerModel` | Missing value imputation |
| `CountVectorizer` / `CountVectorizerModel` | Text vectorization with vocabulary |
| `NGram` | N-gram generation |
| `StopWordsRemover` | Remove stop words from text |
| `RegexTokenizer` | Regex-based tokenization |
| `StandardScaler` / `StandardScalerModel` | Standardize features (zero mean, unit variance) |
| `MinMaxScaler` / `MinMaxScalerModel` | Scale features to [0, 1] range |
| `MaxAbsScaler` / `MaxAbsScalerModel` | Scale features by max absolute value |
| `OneHotEncoder` / `OneHotEncoderModel` | One-hot encoding for categorical features |
| `Normalizer` | Normalize vectors to unit norm |
| `IndexToString` | Convert label indices back to strings |

#### New Evaluation Metrics

| Evaluator | Metrics |
|-----------|---------|
| `BinaryClassificationEvaluator` | areaUnderROC, areaUnderPR |
| `MulticlassClassificationEvaluator` | f1, accuracy, weightedPrecision, weightedRecall |
| `RegressionEvaluator` | rmse, mse, r2, mae |

#### New Regression Algorithms

- `LinearRegression` / `LinearRegressionModel`

#### Enhanced Estimator.Fit()

The `Estimator.Fit()` method now supports automatic casting to 15+ model types:

- **Classification:** LogisticRegressionModel, NaiveBayesModel, GBTClassifierModel, DecisionTreeClassifierModel, RandomForestClassifierModel
- **Regression:** LinearRegressionModel
- **Feature:** IDFModel, StringIndexerModel, Word2VecModel, StandardScalerModel, MinMaxScalerModel, OneHotEncoderModel, CountVectorizerModel, PCAModel, MaxAbsScalerModel, ImputerModel
- **Clustering:** KMeansModel, GaussianMixtureModel

#### ML Infrastructure Improvements

- `Transformer.Load()` now supports an `operatorType` parameter to distinguish between Model and Transformer types
- Feature transformers (Binarizer, Bucketizer, HashingTF, Tokenizer) now correctly load as `Transformer` type instead of `Model`

---

### New SQL Functions (40+ additions)

#### Time/Date Functions (Spark 4.1)

| Function | Description |
|----------|-------------|
| `CurrentTime()` | Returns current time |
| `MakeTime(hour, minute, second)` | Creates time value |
| `ToTime(col)` / `ToTime(col, format)` | Convert to time type |
| `TryToTime(col)` / `TryToTime(col, format)` | Safe time conversion |
| `TimeDiff(unit, start, end)` | Calculate time difference |
| `TimeTrunc(unit, time)` | Truncate time to unit |

#### String Functions

| Function | Description | Spark Version |
|----------|-------------|---------------|
| `ILike(col, pattern)` | Case-insensitive LIKE | 3.3.0+ |
| `Quote(col)` | SQL escape quoting | 4.0.0+ |
| `Space(n)` | Generate n spaces | 3.5+ |
| `Randstr(length)` | Random string generation | 4.0.0+ |
| `Uuid()` | Generate UUID | 3.5+ |
| `Nullifzero(col)` | Convert 0 to null | 4.0.0+ |
| `Zeroifnull(col)` | Convert null to 0 | 4.0.0+ |

#### UTF-8 Validation (Spark 4.0.0+)

| Function | Description |
|----------|-------------|
| `IsValidUtf8(col)` | Check UTF-8 validity |
| `ValidateUtf8(col)` | Validate and report UTF-8 issues |
| `MakeValidUtf8(col)` | Fix invalid UTF-8 sequences |
| `LuhnCheck(col)` | Validate Luhn checksum (credit card validation) |

---

### New Data Type

- **`TimeType`** - New Spark data type for time values (Spark 4.1+) with Arrow `Time64Type` support

---

## Breaking Changes

### DataFrame.WithColumn() Behavior

- Removed `RunAnalyze()` call and explicit schema passing
- Schema is now lazily evaluated rather than eagerly fetched
- This change was required for pipeline execution compatibility

## Cleanup

- Removed `ArrowFixes.cs` from ML examples (no longer needed)

---

## Protocol Buffer Updates

| File | Change |
|------|--------|
| `pipelines.proto` | **NEW** - Complete pipeline protocol definition (307 lines) |
| `base.proto` | Added pipeline responses, compression support (ZSTD), Arrow batch chunking |
| `commands.proto` | Pipeline command support |
| `common.proto` | Updated message definitions |
| `expressions.proto` | Expression enhancements |
| `relations.proto` | Relation updates |
| `types.proto` | New type definitions |
| `ml.proto` | ML protocol updates |

---

## Compatibility

| Spark Version | Status |
|---------------|--------|
| Spark 4.0.0 | Tested |
| Spark 3.5.3 | Tested |
| Spark 3.4+ | Backwards compatible |

New functions are marked with their minimum Spark version requirements.

---

## Statistics

| Category | Count |
|----------|-------|
| New Pipeline Files | 6 (~1,200 lines) |
| New Pipeline Attributes | 8 |
| New ML Classes | 50+ |
| New ML Test Files | 15+ |
| New SQL Functions | 40+ |
| Proto File Changes | 7 files |
| Total Lines Added | ~4,500 |

---

## Examples

New example applications demonstrating key features:

| Example | Description |
|---------|-------------|
| `LogisticRegressionExample.cs` | Binary classification for customer churn prediction using VectorAssembler and LogisticRegression |
| `TfidfExample.cs` | Document similarity using TF-IDF with Tokenizer, HashingTF, IDF, and Normalizer |
| `BooksPipeline.cs` | Medallion architecture pipeline (Bronze → Silver → Gold) processing JSON data |
| `StreamingPipeline.cs` | Streaming table example with rate source |

---

## Getting Started

### Pipelines

```csharp
using Spark.Connect.Dotnet.Pipelines;

// Create and run a pipeline
var runner = new PipelineRunner(spark);
await runner.RunAsync<BooksPipeline>();
```

### Machine Learning

```csharp
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.Classification;

// Feature engineering
var tokenizer = new Tokenizer().SetInputCol("text").SetOutputCol("words");
var hashingTF = new HashingTF().SetInputCol("words").SetOutputCol("features");

// Classification
var rf = new RandomForestClassifier()
    .SetFeaturesCol("features")
    .SetLabelCol("label")
    .SetNumTrees(100);

var model = rf.Fit(trainingData);
var predictions = model.Transform(testData);

// Evaluation
var evaluator = new MulticlassClassificationEvaluator()
    .SetMetricName("accuracy");
var accuracy = evaluator.Evaluate(predictions);
```

---

## Acknowledgments

This release represents a significant step toward feature parity with PySpark, enabling .NET developers to build production-grade ETL/ELT pipelines and machine learning workflows with Apache Spark.
