# Adding Functions

The `spark.sql.functions.*` aren't completly implemented yet so if you would like to use a function that is not available then you can follow this process to implement the function (contributions are welcome if you feel like creating a pr).

1. Find the PySpark version of the function you want to implement
1. Decide on the types of parameters you want to accept, if you see the Python definition is ColumnOrName then you might want to consider using an overload to support both and doing `Col(col)` for the string version to map it to a column.
1. I have written a generator that created most of the functions so thee functions are split across the [Generated Functions](../src/Spark.Connect.Dotnet/Spark.Connect.Dotnet/Sql/Functions.cs) partial class and the [Manually implemented Functions](../src/Spark.Connect.Dotnet/Spark.Connect.Dotnet/Sql/ManualFunctions.cs) partial class. Add your new function to the `ManualFunctions` file.
1. If you find another function that has a similar signature then it should be fairly easy to replicate
1. For the test I ensure that the function is called and a `DataFrame.Show()` is called which will make sure that our logic is not causing an error, I will look at the output to make sure it looks correct and we can also do a collect or count to verify the new function.
1. Test your function and when you are happy raise a PR (or don't, whatever!)
1. I am not completely sure what we should put for the docs, it is a bit random at the moment. Probably at a minimum we should copy what Python function docs say but I am not sure tbh.