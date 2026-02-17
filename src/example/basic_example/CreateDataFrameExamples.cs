using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using static Spark.Connect.Dotnet.Sql.Functions;
using f=Spark.Connect.Dotnet.Sql.Functions;


public class CreateDataFrameExamples
{
    private readonly SparkSession _spark;

    public CreateDataFrameExamples(SparkSession spark)
    {
        _spark = spark;
    }

    public void Run()
    {
        var spark = _spark;
        
        var data = new List<(int, string)>()
        {
            (1, "Mark"),
            (2, "Pauline"),
            (3, "Ajay")
        };

        var schema = new  StructType(
            new StructField("id", new IntegerType(), false),
            new StructField("first_name", new StringType(), false));

        var students = spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        students.Show();
        students = students.WithColumn("row_key", f.Xxhash64(f.Col("id")));
        students.Show();

        var filteredStudents = students.Filter(students["id"] < 10);
        filteredStudents.PrintSchema();
        filteredStudents.Show();
        
        var moreFilteredStudents = students.Filter(students["id"] > 1 & students["first_name"].StartsWith("P"));
        var filteredStudentsUsingAnd = students.Filter((students["id"] > 1).And(students["first_name"].StartsWith("P")));
        moreFilteredStudents.Show();
        filteredStudentsUsingAnd.Show();
        
        var testScoresData = new List<(int, int, DateTime)>()
        {
            (1, 97, DateTime.Today),
            (2, 99, DateTime.Today),
            (3, 98, DateTime.Today)
        };

        var testScoreSchema = new  StructType(
            new StructField("id", new IntegerType(), false),
            new StructField("score", new IntegerType(), false),
            new StructField("date", new DateType(), false)
            );

        var testScores = spark.CreateDataFrame(testScoresData.Cast<ITuple>(), testScoreSchema);
        testScores.Show();
        
        var studentTestScores = students.Join(testScores, (students["id"] == testScores["id"]), JoinType.Inner);
        studentTestScores.Select(
            students["id"],
            students["first_name"],
            students["date"],
            testScores["score"]
        ).OrderBy(Desc(testScores["score"]))
            .Show();
    }
}