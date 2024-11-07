using Spark.Connect;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

var spark = SparkSession.Builder.Remote("sc://127.0.0.1:15002").GetOrCreate();
var jsonData = @"
[
	{
		""id"": ""0001"",
		""type"": ""donut"",
		""name"": ""Cake"",
		""ppu"": 0.55,
		""batters"":
			{
				""batter"":
					[
						{ ""id"": ""1001"", ""type"": ""Regular"" },
						{ ""id"": ""1002"", ""type"": ""Chocolate"" },
						{ ""id"": ""1003"", ""type"": ""Blueberry"" },
						{ ""id"": ""1004"", ""type"": ""Devil's Food"" }
					]
			},
		""topping"":
			[
				{ ""id"": ""5001"", ""type"": ""None"" },
				{ ""id"": ""5002"", ""type"": ""Glazed"" },
				{ ""id"": ""5005"", ""type"": ""Sugar"" },
				{ ""id"": ""5007"", ""type"": ""Powdered Sugar"" },
				{ ""id"": ""5006"", ""type"": ""Chocolate with Sprinkles"" },
				{ ""id"": ""5003"", ""type"": ""Chocolate"" },
				{ ""id"": ""5004"", ""type"": ""Maple"" }
			]
	},
	{
		""id"": ""0002"",
		""type"": ""donut"",
		""name"": ""Raised"",
		""ppu"": 0.55,
		""batters"":
			{
				""batter"":
					[
						{ ""id"": ""1001"", ""type"": ""Regular"" }
					]
			},
		""topping"":
			[
				{ ""id"": ""5001"", ""type"": ""None"" },
				{ ""id"": ""5002"", ""type"": ""Glazed"" },
				{ ""id"": ""5005"", ""type"": ""Sugar"" },
				{ ""id"": ""5003"", ""type"": ""Chocolate"" },
				{ ""id"": ""5004"", ""type"": ""Maple"" }
			]
	},
	{
		""id"": ""0003"",
		""type"": ""donut"",
		""name"": ""Old Fashioned"",
		""ppu"": 0.55,
		""batters"":
			{
				""batter"":
					[
						{ ""id"": ""1001"", ""type"": ""Regular"" },
						{ ""id"": ""1002"", ""type"": ""Chocolate"" }
					]
			},
		""topping"":
			[
				{ ""id"": ""5001"", ""type"": ""None"" },
				{ ""id"": ""5002"", ""type"": ""Glazed"" },
				{ ""id"": ""5003"", ""type"": ""Chocolate"" },
				{ ""id"": ""5004"", ""type"": ""Maple"" }
			]
	}
]
";

var id = "adobeData";

var df = spark.CreateDataFrame(new (object, object)[]{(jsonData, id),}, "json", "id");
df.Show();

df = df.Select(ParseJson(Col("json")).Alias("variant_data"));
df.Show();

df.Select(SchemaOfVariant(Col("variant_data"))).Show(1, 10000, true);
df.PrintSchema();

foreach (var schemaField in df.Schema.Fields)
{
	Console.WriteLine($"{schemaField.Name} of type {schemaField.DataType.TypeName}");
}

df.Select(
		VariantGet(df["variant_data"], "$[0].batters.batter[0].id").Alias("batterId"),
		VariantGet(df["variant_data"], "$[0].batters.batter[0].type").Alias("batterType"),
		VariantGet(df["variant_data"], "$[0].id").Alias("id")
	).Show();

df.CreateOrReplaceTempView("data");

spark.Sql(@"
	SELECT u.* FROM variant_explode((SELECT variant_data FROM data)) AS t, LATERAL variant_explode(t.value) as u
").Show();

df.Select(
	new Column(new Expression
	{
		UnresolvedFunction = new Expression.Types.UnresolvedFunction
		{
			FunctionName = "variant_explode", IsDistinct = false, IsUserDefinedFunction = false, Arguments = { Col("variant_data").Expression }
		}
	})
).Show();



