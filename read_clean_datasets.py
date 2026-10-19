from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("View311Preview").master("local[2]").getOrCreate()

for filename in ["nyc_merge_ready.parquet", "la_merge_ready.parquet"]:
    df = spark.read.parquet(filename)
    print(f"\n{filename}")
    df.printSchema()
    df.show(100, truncate=False)

spark.stop()