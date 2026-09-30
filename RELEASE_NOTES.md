# Release notes

## 4.0.0-build.40 (unreleased)

- Fix `Column.IsIn()` adding an unintended string literal when an argument is a
  `Column`. Both the params and list overloads now preserve column arguments.
- Add regression tests for column, literal, and mixed arguments.
- Run the full test suite on Spark 4.0.0 with Delta Lake 4.0.0, and add a Spark 3.5.6
  smoke-test job. Use archived Spark downloads and retain test results and server logs.
- Give the local Spark and Databricks workflows separate concurrency groups.
- Share the version of both NuGet packages and pack them in CI.
- Refresh the local quickstart, compatibility documentation, and packaging instructions.

The .NET 8 target and existing Spark API baseline are unchanged.
