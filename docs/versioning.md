# Versioning

The nuget packages will follow this schema:

SparkVersion-OurReleaseVersion for example:

- "3.5.1-build.1" is the first release of this and it supports up to Spark 3.5.1 (backwards compatible to 3.4.0).
- "4.0.0-build.24" is the 24th release of this lib and it supports up to Spark 4.0.0 (backwards compatible to 3.4.0).

If you are using Spark 3.5.3 and you use this lib's 4.0.0-build.24 then it will work but there will be functions that do not exist in 3.5.3 and if you call them you will get an error but if you stick to functions that are available in the version you are calling then everything will be fine.
