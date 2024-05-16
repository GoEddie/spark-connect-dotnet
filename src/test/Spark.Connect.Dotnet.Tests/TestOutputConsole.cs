using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests;

/// <summary>
/// This is so we get the output in the test ui rather than having to go and view the diagnostics log each time
/// </summary>
public class TestOutputConsole : LocalConsole
{
    private readonly ITestOutputHelper _helper;

    public TestOutputConsole(ITestOutputHelper helper)
    {
        _helper = helper;
    }

    public override void WriteLine(string what)
    {
        Console.WriteLine($"TestOutputConsole: {_helper.GetHashCode()}::{what}");
        _helper.WriteLine(what);
    }
}