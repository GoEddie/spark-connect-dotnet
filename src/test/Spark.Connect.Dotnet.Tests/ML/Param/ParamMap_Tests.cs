using Spark.Connect.Dotnet.ML.Param;

namespace Spark.Connect.Dotnet.Tests.ML.Param;


public class ParamMap_Tests
{
   [Fact]
   public void AddParam_Test()
   {
      var param = new Dotnet.ML.Param.Param("a", 123);
      var map = new ParamMap([] );
      
      map.Add(param);
      var retrieved = map.Get(param.Name);
      
      Assert.NotNull(retrieved);
      Assert.Equal(param.Name, retrieved.Name);
      Assert.Equal(param.Value, retrieved.Value);
   }
   
   [Fact]
   public void GetDefaultParam_Test()
   {
      var param = new Dotnet.ML.Param.Param("a", 123);
      var map = new ParamMap([param] );
      
      
      var retrieved = map.Get(param.Name);
      
      Assert.NotNull(retrieved);
      Assert.Equal(param.Name, retrieved.Name);
      Assert.Equal(param.Value, retrieved.Value);
   }
   
   [Fact]
   public void GetAllParam_Test()
   {
      var param = new Dotnet.ML.Param.Param("a", 123);
      var map = new ParamMap([param] );
      
      var retrieved = map.GetAll().FirstOrDefault();
      
      Assert.NotNull(retrieved);
      Assert.Equal(param.Name, retrieved.Name);
      Assert.Equal(param.Value, retrieved.Value);
   }
}