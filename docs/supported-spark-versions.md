# Supported Spark Versions

The client targets .NET 8 and uses Spark Connect. CI is configured for:

| Server | Coverage |
|--------|----------|
| Spark 4.0.0 with Delta Lake 4.0.0 | Full test suite, including core DataFrame, SQL, Arrow, ML, streaming, and Delta tests; explicitly skipped tests remain excluded |
| Spark 3.5.6 | Smoke tests for DataFrame creation, SQL and `IsIn`, and JSON read/write |

These describe the configured checks, not a guarantee that every Spark API is implemented.
Spark 3.4 and other server versions are not covered by this matrix. Databricks has a
separate workflow and requires its own configured credentials and runtime validation.

The library does not check whether a SQL function exists on the connected server.
Calling a function introduced in a newer Spark version can return an
`UnresolvedRoutineException`. Newer features such as Spark Connect ML and the Delta
Connect extension also require a server that supports them.

Use the [function status](function-status.md) and the documentation for your server
version when choosing APIs. See [versioning](versioning.md) for the package naming scheme.
