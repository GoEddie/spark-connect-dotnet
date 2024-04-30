# Supported Spark Versions

This project relies on Spark Connect which was first released in Apache Spark 3.4.0, the gRPC interface is backwards compatible so you can use any higher version.

Different versions of Spark include new functions, in 3.5.1 quite a lot of new functions were created. We do no checks in this project as to whether the function you are trying to call is available in the version of Spark you are connected to. You are responsible for knowing if a function is available, if it is not available then you will get an error from Spark.

If you try calling a function that is only available in a higher version of Spark you will get a `UnresolvedRoutineException` exception and it will list the name of the PySpark function you are trying to call. For example, Spark 3.5.0 introduced [`bitmap_bucket_number`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bitmap_bucket_number.html), if you try calling this against Spark 3.4.* you will get this error:

```shell
[UNRESOLVED_ROUTINE] Cannot resolve function `bitmap_bucket_number` on search path [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`].
```

To know exactly what functions you can use see this page: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html