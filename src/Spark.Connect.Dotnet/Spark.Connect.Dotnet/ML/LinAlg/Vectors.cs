namespace Spark.Connect.Dotnet.ML.LinAlg;

public static class Vectors
{
    /// <summary>
    /// Helper method to mimic python, you can just new DenseVector
    /// </summary>
    /// <param name="values"></param>
    /// <returns>DenseVector</returns>
    public static DenseVector Dense(List<double> values) => new DenseVector(values);
    
    /// <summary>
    /// Helper method to mimic python, you can just new SparseVector
    /// </summary>
    /// <param name="size"></param>
    /// <param name="indices"></param>
    /// <param name="values"></param>
    /// <returns>SparseVector</returns>
    public static SparseVector Sparse(int size, List<int> indices, List<double> values) => new SparseVector(size, indices, values);
}