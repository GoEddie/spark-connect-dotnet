# Versioning

Package versions follow `SparkVersion-build.ReleaseNumber`, for example
`4.0.0-build.40`. The Spark prefix identifies the protocol/API baseline, while the
build number identifies a release of this client. The suffix makes these NuGet
prerelease versions.

Both packages share the version in
[`src/Spark.Connect.Dotnet/Directory.Build.props`](../src/Spark.Connect.Dotnet/Directory.Build.props):

- `GOEddie.Spark.Dotnet`
- `GOEddie.Spark.Dotnet.GrpcClient`

The package version is not a guarantee of compatibility with every earlier Spark
server. See [supported Spark versions](supported-spark-versions.md) for the CI matrix
and feature limitations.

## Preparing packages locally

From the repository root, restore and build:

```sh
dotnet restore src/Spark.Connect.Dotnet.sln
dotnet build src/Spark.Connect.Dotnet.sln --configuration Release --no-restore
```

With the Spark 4.0.0 Connect server and Delta extension running, run the tests and
pack both libraries:

```sh
dotnet test src/test/Spark.Connect.Dotnet.Tests/Spark.Connect.Dotnet.Tests.csproj --configuration Release --no-build
dotnet pack src/Spark.Connect.Dotnet/Spark.Connect.Dotnet.GrpcClient/Spark.Connect.Dotnet.GrpcClient.csproj --configuration Release --no-build --output artifacts/packages
dotnet pack src/Spark.Connect.Dotnet/Spark.Connect.Dotnet/Spark.Connect.Dotnet.csproj --configuration Release --no-build --output artifacts/packages
```

Check that both `.nupkg` files have the same version and that the main package's
`.nuspec` references that version of `GOEddie.Spark.Dotnet.GrpcClient`. Install the
main package into a fresh console application using `artifacts/packages` as an
additional NuGet source, and run the [getting-started example](getting-started.md)
against the local server before publishing. These commands only prepare local
packages; publishing remains a separate maintainer action.

CI also packs both libraries after the Spark 4 tests and makes them available as
workflow artifacts.
