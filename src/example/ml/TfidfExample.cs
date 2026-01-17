using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace ml;

/// <summary>
/// TF-IDF with Cosine Similarity Example
/// Demonstrates document similarity using Spark .NET ML transformers and DataFrame API
/// </summary>
public static class TfidfExample
{
    public static void Run(SparkSession spark)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("TF-IDF with Cosine Similarity Example");
        Console.WriteLine("========================================\n");

        // Create sample documents
        var documents = new List<(int id, string text)>
        {
            (1, "Apache Spark is a fast and general engine for big data processing"),
            (2, "Spark SQL provides support for structured data processing"),
            (3, "Machine learning is a subset of artificial intelligence"),
            (4, "Deep learning uses neural networks for pattern recognition"),
            (5, "Spark MLlib is a scalable machine learning library"),
            (6, "Big data analytics with Apache Spark and machine learning")
        };

        var df = spark.CreateDataFrame(
            documents.Select(d => new object[] { d.id, d.text }),
            new[] { "id", "text" }
        );

        Console.WriteLine("=== Input Documents ===");
        df.Show(truncate: 100);

        // Step 1: Tokenize the text
        var tokenizer = new Tokenizer(spark);
        tokenizer.SetInputCol("text");
        tokenizer.SetOutputCol("words");

        var wordsData = tokenizer.Transform(df);
        Console.WriteLine("\n=== After Tokenization ===");
        wordsData.Show(truncate: 100);

        // Step 2: Apply HashingTF to convert words to term frequency vectors
        var hashingTF = new HashingTF(spark);
        hashingTF.SetInputCol("words");
        hashingTF.SetOutputCol("rawFeatures");
        hashingTF.SetNumFeatures(1000);

        var featurizedData = hashingTF.Transform(wordsData);
        Console.WriteLine("\n=== After HashingTF ===");
        featurizedData.Select("id", "rawFeatures").Show(truncate: 100);

        // Step 3: Apply IDF to compute TF-IDF weights
        var idf = new IDF();
        idf.SetInputCol("rawFeatures");
        idf.SetOutputCol("tfidf");

        var idfModel = idf.Fit(featurizedData);
        var tfidfData = idfModel.Transform(featurizedData);
        Console.WriteLine("\n=== After IDF (TF-IDF vectors) ===");
        tfidfData.Select("id", "text", "tfidf").Show(truncate: 100);

        // Step 4: Normalize TF-IDF vectors using L2 norm
        var normalizer = new Normalizer(spark);
        normalizer.SetInputCol("tfidf");
        normalizer.SetOutputCol("normTfidf");
        normalizer.SetP(2.0);

        var normalizedData = normalizer.Transform(tfidfData);
        Console.WriteLine("\n=== After L2 Normalization ===");
        normalizedData.Select("id", "normTfidf").Show(truncate: 100);

        // Step 5: Extract sparse vector components by parsing string representation
        var withComponents = ParseVectorComponents(normalizedData, "normTfidf");

        Console.WriteLine("\n=== Sparse vector components ===");
        withComponents.PrintSchema();
        withComponents.Show(truncate: 80);

        // Step 6: Compute pairwise cosine similarity
        var similarities = ComputeCosineSimilarity(withComponents);

        Console.WriteLine("\n=== Pairwise Cosine Similarities ===");
        similarities.Show(truncate: 60);

        Console.WriteLine("\n=== Top 5 Most Similar Document Pairs ===");
        similarities
            .Select("id1", "id2", "cosine_similarity")
            .Show(5);

        Console.WriteLine("\n=== Documents most similar to Document 1 (Apache Spark) ===");
        similarities
            .Where((Col("id1") == 1) | (Col("id2") == 1))
            .Select(
                When(Col("id1") == 1, Col("id2")).Otherwise(Col("id1")).Alias("similar_doc_id"),
                When(Col("id1") == 1, Col("text2")).Otherwise(Col("text1")).Alias("similar_text"),
                Col("cosine_similarity")
            )
            .OrderBy(Col("cosine_similarity").Desc())
            .Show(truncate: 80);

        Console.WriteLine("\nTF-IDF Example completed!");
    }

    /// <summary>
    /// Parse sparse vector string representation into indices and values arrays
    /// Vector format: (size,[idx1,idx2,...],[val1,val2,...])
    /// </summary>
    private static DataFrame ParseVectorComponents(DataFrame df, string vectorCol)
    {
        var vecStr = Col(vectorCol).Cast("string").Alias("vec_str");
        var withVecStr = df.Select(Col("id"), Col("text"), vecStr);

        // Extract indices: content between first [ and ]
        var firstBracket = Instr(Col("vec_str"), "[");
        var closeBracket = Instr(Col("vec_str"), "]");
        var indicesStr = Substr(Col("vec_str"), firstBracket + Lit(1), closeBracket - firstBracket - Lit(1));

        // Extract values: content between ],[ and final )
        var valueStart = Instr(Col("vec_str"), "],[");
        var valuesStr = Substr(Col("vec_str"), valueStart + Lit(3), Length(Col("vec_str")) - valueStart - Lit(4));

        var parsed = withVecStr.Select(
            Col("id"),
            Col("text"),
            indicesStr.Alias("indices_str"),
            valuesStr.Alias("values_str")
        );

        return parsed.Select(
            Col("id"),
            Col("text"),
            Transform(Split(Col("indices_str"), ","), x => x.Cast("int")).Alias("indices"),
            Transform(Split(Col("values_str"), ","), x => x.Cast("double")).Alias("values")
        );
    }

    /// <summary>
    /// Compute pairwise cosine similarity between all documents
    /// For L2-normalized vectors, cosine similarity = dot product
    /// </summary>
    private static DataFrame ComputeCosineSimilarity(DataFrame withComponents)
    {
        // Create array of structs from indices and values, then explode
        var zipped = withComponents.Select(
            Col("id"),
            Col("text"),
            ArraysZip(Col("indices"), Col("values")).Alias("idx_val_pairs")
        );

        // Explode the array of structs
        var exploded = zipped.Select(
            Col("id"),
            Col("text"),
            Explode(Col("idx_val_pairs")).Alias("pair")
        ).Select(
            Col("id"),
            Col("text"),
            Col("pair.indices").Alias("idx"),
            Col("pair.values").Alias("val")
        );

        // Create two copies for self-join
        var df1 = exploded.Select(
            Col("id").Alias("id1"),
            Col("text").Alias("text1"),
            Col("idx").Alias("idx1"),
            Col("val").Alias("val1")
        );

        var df2 = exploded.Select(
            Col("id").Alias("id2"),
            Col("text").Alias("text2"),
            Col("idx").Alias("idx2"),
            Col("val").Alias("val2")
        );

        // Join on matching indices where id1 < id2
        var joined = df1.Join(df2, (Col("idx1") == Col("idx2")) & (Col("id1") < Col("id2")));

        // Compute dot product grouped by document pairs
        return joined
            .GroupBy(Col("id1"), Col("text1"), Col("id2"), Col("text2"))
            .Agg(Round(Sum(Col("val1") * Col("val2")), Lit(4)).Alias("cosine_similarity"))
            .OrderBy(Col("cosine_similarity").Desc());
    }
}
