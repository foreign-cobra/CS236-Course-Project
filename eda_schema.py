from pathlib import Path
from pyspark.sql import SparkSession, functions as F

# Source files live next to this script. The script only reads the CSVs and
# writes a text report under ./reports; it does not edit either source file.
BASE = Path(__file__).resolve().parent
NYC_PATH = BASE / "nyc_311_800k.csv"
LA_PATH = BASE / "la_311_800k.csv"
REPORT_DIR = BASE / "reports"
REPORT_PATH = REPORT_DIR / "eda_report.txt"

NYC_ID = "unique_key"
LA_ID = "srnumber"
NYC_DATES = ["created_date", "closed_date", "due_date", "resolution_action_updated_date"]
LA_DATES = ["createddate", "updateddate", "servicedate", "closeddate"]
NYC_CATEGORICAL = ["agency", "agency_name", "complaint_type", "descriptor", "descriptor_2", "location_type", "status", "borough", "open_data_channel_type"]
LA_CATEGORICAL = ["owner", "requesttype", "status", "requestsource", "createdbyuserorganization", "mobileos", "anonymous", "assignto", "apc", "ncname", "policeprecinct"]
NYC_GEO = ["latitude", "longitude", "x_coordinate_state_plane", "y_coordinate_state_plane"]
LA_GEO = ["latitude", "longitude"]

REPORT_DIR.mkdir(exist_ok=True)
with REPORT_PATH.open("w", encoding="utf-8") as report:
    def say(text=""):
        print(text)
        report.write(str(text) + "\n")
        report.flush()

    spark = (
        SparkSession.builder
        .appName("311-EDA")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    def load_csv(path, infer_schema):
        if not path.exists():
            raise FileNotFoundError(f"CSV not found: {path}")
        return (spark.read.option("header", True)
                .option("inferSchema", infer_schema)
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
                .option("mode", "PERMISSIVE")
                .csv(str(path)))

    def schema_text(df):
        return "\n".join(
            f"  {field.name}: {field.dataType.simpleString()} (nullable={field.nullable})"
            for field in df.schema.fields
        )

    def profile(label, df, inferred_df, id_col, date_cols, cat_cols, geo_cols):
        say(f"\n{'=' * 76}\n{label}\nFile: {label} CSV\n")
        say("Raw schema (strings preserve the CSV text for quality checks):")
        say(schema_text(df))
        say("Spark-inferred schema (use as type suggestions; verify identifiers and codes):")
        say(schema_text(inferred_df))
        say(f"Columns: {len(df.columns)}")
        row_count = df.count()
        say(f"Rows: {row_count:,}")
        say("Representative rows:")
        for row in df.limit(5).toJSON().collect():
            say(row)

        # Treat null and whitespace-only values as missing for this report.
        missing_exprs = []
        for name in df.columns:
            value = F.col(name).cast("string")
            missing_exprs.append(
                F.sum(F.when(F.col(name).isNull() | (F.trim(value) == ""), 1).otherwise(0)).alias(name)
            )
        missing = df.agg(*missing_exprs).first().asDict()
        say("Missing/blank values by column (only nonzero columns):")
        nonzero = [(k, int(v or 0)) for k, v in missing.items() if v]
        for name, count in sorted(nonzero, key=lambda item: (-item[1], item[0])):
            say(f"  {name}: {count:,} ({count / max(row_count, 1):.2%})")
        if not nonzero:
            say("  None found")

        if id_col in df.columns:
            id_text = F.trim(F.col(id_col).cast("string"))
            id_counts = df.select(id_text.alias("key")).groupBy("key").count()
            id_stats = id_counts.agg(
                F.sum(F.when(F.col("key").isNull() | (F.col("key") == ""), F.col("count")).otherwise(0)).alias("missing_ids"),
                F.sum(F.when(F.col("key").isNotNull() & (F.col("key") != ""), 1).otherwise(0)).alias("distinct_ids"),
                F.sum(F.when(F.col("key").isNotNull() & (F.col("key") != "") & (F.col("count") > 1), 1).otherwise(0)).alias("duplicate_id_groups")
            ).first()
            say(f"Identifier {id_col}: missing rows={int(id_stats['missing_ids'] or 0):,}, distinct values={int(id_stats['distinct_ids'] or 0):,}, duplicate-value groups={int(id_stats['duplicate_id_groups'] or 0):,}")

        # Hashing rows avoids shuffling all wide CSV columns during exact-duplicate analysis.
        row_hash = F.sha2(F.to_json(F.struct(*[F.col(c) for c in df.columns])), 256)
        duplicate_rows = (df.select(row_hash.alias("row_hash")).groupBy("row_hash").count()
                          .filter(F.col("count") > 1)
                          .agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("extra_rows"))
                          .first()["extra_rows"])
        say(f"Exact duplicate rows beyond first occurrence (row hash): {int(duplicate_rows or 0):,}")

        say("Date ranges and parse failures:")
        present_dates = [c for c in date_cols if c in df.columns]
        if present_dates:
            date_exprs = []
            for name in present_dates:
                raw = F.trim(F.col(name).cast("string"))
                parsed = F.to_timestamp(F.col(name).cast("string"))
                date_exprs.extend([
                    F.min(parsed).alias(f"{name}__min"),
                    F.max(parsed).alias(f"{name}__max"),
                    F.sum(F.when((raw != "") & raw.isNotNull() & parsed.isNull(), 1).otherwise(0)).alias(f"{name}__unparsed")
                ])
            date_stats = df.agg(*date_exprs).first().asDict()
            for name in present_dates:
                say(f"  {name}: min={date_stats[f'{name}__min']}, max={date_stats[f'{name}__max']}, nonblank_unparsed={int(date_stats[f'{name}__unparsed'] or 0):,}")

        say("Top categorical values (top 10; nulls omitted):")
        present_categories = [c for c in cat_cols if c in df.columns]
        if present_categories:
            category_items = [F.struct(F.lit(name).alias("column"), F.col(name).cast("string").alias("value"))
                              for name in present_categories]
            long_categories = (df.select(F.explode(F.array(*category_items)).alias("item"))
                               .select("item.column", "item.value")
                               .filter(F.col("value").isNotNull() & (F.trim(F.col("value")) != "")))
            from pyspark.sql import Window
            ranked = (long_categories.groupBy("column", "value").count()
                      .withColumn("rank", F.row_number().over(Window.partitionBy("column").orderBy(F.desc("count"), F.asc("value"))))
                      .filter(F.col("rank") <= 10)
                      .orderBy("column", "rank"))
            top_rows = ranked.collect()
            for name in present_categories:
                say(f"  {name}:")
                for row in top_rows:
                    if row["column"] == name:
                        say(f"    {str(row['value'])[:100]}: {row['count']:,}")

        present_geo = [c for c in geo_cols if c in df.columns]
        if present_geo:
            geo_exprs = []
            for name in present_geo:
                numeric = F.col(name).cast("double")
                raw = F.trim(F.col(name).cast("string"))
                geo_exprs.extend([
                    F.min(numeric).alias(f"{name}__min"),
                    F.max(numeric).alias(f"{name}__max"),
                    F.sum(F.when((raw != "") & raw.isNotNull() & numeric.isNull(), 1).otherwise(0)).alias(f"{name}__unparsed")
                ])
            geo_stats = df.agg(*geo_exprs).first().asDict()
            for name in present_geo:
                say(f"Geographic/numeric {name}: min={geo_stats[f'{name}__min']}, max={geo_stats[f'{name}__max']}, nonblank_unparsed={int(geo_stats[f'{name}__unparsed'] or 0):,}")

    try:
        # Keep an all-string read for data-quality checks so malformed source
        # values remain visible; the inferred reads provide candidate types.
        nyc = load_csv(NYC_PATH, infer_schema=False)
        la = load_csv(LA_PATH, infer_schema=False)
        nyc_inferred = load_csv(NYC_PATH, infer_schema=True)
        la_inferred = load_csv(LA_PATH, infer_schema=True)
        profile("NYC", nyc, nyc_inferred, NYC_ID, NYC_DATES, NYC_CATEGORICAL, NYC_GEO)
        profile("LA", la, la_inferred, LA_ID, LA_DATES, LA_CATEGORICAL, LA_GEO)

        nyc_types = {field.name: field.dataType.simpleString() for field in nyc_inferred.schema.fields}
        la_types = {field.name: field.dataType.simpleString() for field in la_inferred.schema.fields}
        say(f"\n{'=' * 76}\nSCHEMA COMPARISON")
        say(f"NYC-only column names: {sorted(set(nyc_types) - set(la_types))}")
        say(f"LA-only column names: {sorted(set(la_types) - set(nyc_types))}")
        say("Exact-name matches and inferred types:")
        for name in sorted(set(nyc_types) & set(la_types)):
            say(f"  {name}: NYC={nyc_types[name]}, LA={la_types[name]}")
        say("\nInitial conceptual correspondences to review (not automatic equivalences):")
        for pair in [
            ("unique_key", "srnumber", "request identifier"),
            ("created_date", "createddate", "creation time"),
            ("closed_date", "closeddate", "closure time"),
            ("complaint_type / descriptor", "requesttype", "request category/detail"),
            ("status", "status", "workflow status"),
            ("open_data_channel_type", "requestsource", "intake channel"),
            ("latitude / longitude", "latitude / longitude", "coordinates"),
        ]:
            say(f"  NYC {pair[0]} <-> LA {pair[1]}: {pair[2]}")
        say(f"\nReport saved to: {REPORT_PATH}")
    finally:
        spark.stop()
