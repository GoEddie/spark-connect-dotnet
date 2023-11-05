Imports Spark.Connect.Dotnet.Sql

Module Program
    Sub Main(args As String())
        dim spark as SparkSession = SparkSession _
                                        .Builder _
                                        .Remote("http://localhost:15002") _
                                        .GetOrCreate()
        
        dim dataFrame as DataFrame = spark _
                                        .Range(100)
        
        dataFrame.Show()
        
        dataFrame.WithColumn("source", Functions.Lit("VB.NET!")).Show()
        
    End Sub
End Module
