from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("View311Preview").master("local[2]").getOrCreate()

for filename in ["nyc_merge_ready.parquet", "la_merge_ready.parquet", "merged_311.parquet"]:
    df = spark.read.parquet(filename)
    print(f"\n{filename}")
    df.printSchema()
    if filename == "merged_311.parquet":
        print("\nNYC Records:")
        df.groupBy("source_city", "status").count().show()
        df.filter(df.source_city == "NYC").show(50, truncate=False)

        print("\nLA Records:")
        df.filter(df.source_city == "LA").show(50, truncate=False)
        
    else:
        df.show(100, truncate=False)

spark.stop()