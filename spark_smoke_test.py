from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("311-smoke-test")
    .master("local[2]")
    .getOrCreate()
)

spark.range(5).show()
print("Spark version:", spark.version)

spark.stop()