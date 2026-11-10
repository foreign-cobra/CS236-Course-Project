"""Analyze only the merged_311.parquet dataset with PySpark.

Writes analysis CSV folders and a findings summary under reports/phase2_analysis/.

"""
from pathlib import Path
from pyspark.sql import SparkSession, Window, functions as F

BASE = Path(__file__).resolve().parent

REPORT_DIR = BASE / "reports" / "phase2_analysis"
MERGED_SINGLE_PATH = BASE / "merged_311.parquet"


def standardized_status(column):
    # Spark column expressions describe transformations without changing the original dataset. The output is a new column with 
    # standardized status labels. The original status column is kept in the merged dataset.
    # We do this to ensure that the analysis is consistent across cities, even if they use different labels for the same status.

    value = F.lower(F.trim(F.col(column)))
    return (
        F.when(F.col(column).isNull() | value.isin("", "unknown", "unspecified"), "unknown")
        .when(value == "closed", "closed")
        .when(value == "open", "open")
        .when(value == "pending", "pending")
        .when(value.isin("in progress", "started", "assigned"), "active")
        .when(value.isin("cancelled", "canceled"), "cancelled")
        .when(value == "referred out", "referred_out")
        .otherwise("other")
    )


def standardized_channel(column):
    # Align clearly similar labels into a single category. Other labels are normalized. 
    value = F.lower(F.trim(F.col(column)))
    return (
        F.when(F.col(column).isNull() | (value == ""), "unknown")
        .when(value.isin("online", "web", "website", "self service", "self-service"), "online_self_service")
        .when(value.isin("phone", "call", "voicemail"), "phone")
        .when(value.isin("mobile", "mobile app", "app"), "mobile_app")
        .when(value == "email", "email")
        .when(value == "fax", "fax")
        .when(value.isin("walk-in", "walk in"), "walk_in")
        .when(value == "social", "social")
        .when(value == "driver self report", "driver_self_report")
        .when(value.isin("council's office", "councils office", "council office"), "council_office")
        .otherwise(F.regexp_replace(value, r"[\s-]+", "_"))
    )


def write_csv(df, name):
    """Write one readable CSV part in a Spark output folder."""
    target = REPORT_DIR / name
    # We use coalesce(1) to write a single CSV file for easier reading, even though Spark can write multiple part files in parallel.
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", True)
       .csv(str(target)))


def add_weekly_periods(df):
    # Rows without creation timestamps cannot be assigned to a month-period.
    dated = df.filter(F.col("created_at").isNotNull())
    day = F.dayofmonth("created_at")
    return (
        dated
        # Keep the calendar month so each month's four periods are counted separately rather than pooling every January (for example) together.
        .withColumn("month", F.date_format("created_at", "yyyy-MM"))
        .withColumn(
            "period_number",
            F.when(day <= 7, 1).when(day <= 14, 2).when(day <= 21, 3).otherwise(4)
        )
        .withColumn(
            "month_period",
            F.when(day <= 7, "days 1-7")
             .when(day <= 14, "days 8-14")
             .when(day <= 21, "days 15-21")
             .otherwise("day 22-end")
        )
        .groupBy("source_city", "month", "period_number", "month_period")
        .agg(F.count(F.lit(1)).alias("request_count"))
        .orderBy("source_city", "month", "period_number")
    )


def weekly_extremes(weekly):
    # Find each city's maximum and minimum weekly request counts.
    limits = weekly.groupBy("source_city").agg(
        F.max("request_count").alias("highest_count"),
        F.min("request_count").alias("lowest_count"),
    )
    joined = weekly.join(limits, "source_city")
    # A bucket can be both highest and lowest if only one bucket is observed. 
    tagged = joined.withColumn(
        "extreme",
        F.explode(F.array(
            F.when(F.col("request_count") == F.col("highest_count"), "highest"),
            F.when(F.col("request_count") == F.col("lowest_count"), "lowest"),
        )),
    )
    return (tagged.filter(F.col("extreme").isNotNull())
            .select("source_city", "extreme", "month", "period_number", "month_period", "request_count")
            .orderBy("source_city", "extreme", "month", "period_number"))


def postal_hotspots(df):
    # Postal-code rankings do not include missing/blank postal codes. We only analyze real postal codes. 
    postal_df = df.filter(
        F.col("postal_code").isNotNull() & (F.trim(F.col("postal_code")) != "")
    )
    postal_counts = postal_df.groupBy("source_city", "postal_code").agg(
        F.count(F.lit(1)).alias("postal_request_count")
    )
    postal_rank = Window.partitionBy("source_city").orderBy(
        F.desc("postal_request_count"), F.asc("postal_code")
    )
    # row_number ranks postal codes separately within NYC and LA;
    # .
    top_postals = (postal_counts
                   .withColumn("postal_rank", F.row_number().over(postal_rank))
                   .filter(F.col("postal_rank") <= 10))

    top_keys = top_postals.select("source_city", "postal_code")
    in_hotspots = postal_df.join(top_keys, ["source_city", "postal_code"], "inner")

    denominators = in_hotspots.groupBy("source_city", "postal_code").agg(
        F.count(F.lit(1)).alias("postal_request_count")
    )
    typed = in_hotspots.filter(
        F.col("request_type").isNotNull() & (F.trim(F.col("request_type")) != "")
    )
    type_counts = typed.groupBy("source_city", "postal_code", "request_type").agg(
        F.count(F.lit(1)).alias("type_request_count")
    )
    type_rank = Window.partitionBy("source_city", "postal_code").orderBy(
        F.desc("type_request_count"), F.asc("request_type")
    )
    top_types = (type_counts.join(denominators, ["source_city", "postal_code"])
                 .withColumn(
                     "percent_of_postal_requests",
                     F.round(100.0 * F.col("type_request_count") / F.col("postal_request_count"), 2)
                 )
                 .withColumn("type_rank", F.row_number().over(type_rank))
                 .filter(F.col("type_rank") <= 3)
                 .orderBy("source_city", "postal_code", "type_rank"))
    return top_postals.orderBy("source_city", "postal_rank"), top_types


def resolution_results(df):
    # If resolution_hours is already present, use it; otherwise, compute it from the timestamps.
    if "resolution_hours" in df.columns:
        duration = F.col("resolution_hours")
    else:
        duration = (
            F.col("closed_at").cast("double") - F.col("created_at").cast("double")
        ) / 3600.0

    # Only positive, valid resolution times are included in the analysis. 
    valid_completed = (
        df.filter(
            (F.col("status_category") == "closed")
            & F.col("created_at").isNotNull()
            & F.col("closed_at").isNotNull()
            & (F.col("closed_at") >= F.col("created_at"))
            & duration.isNotNull()
            & (duration >= 0)
        )
        .withColumn("resolution_hours", duration)
    )
    by_city = valid_completed.groupBy("source_city").agg(
        F.count(F.lit(1)).alias("valid_completed_request_count"),
        F.round(F.avg("resolution_hours"), 2).alias("average_resolution_hours"),
        F.round(F.percentile_approx("resolution_hours", 0.5, 10000), 2).alias("median_resolution_hours"),
    ).orderBy("source_city")

    by_type = (valid_completed
               .filter(F.col("request_type").isNotNull() & (F.trim(F.col("request_type")) != ""))
               .groupBy("source_city", "request_type")
               .agg(
                   F.count(F.lit(1)).alias("valid_completed_request_count"),
                   F.round(F.avg("resolution_hours"), 2).alias("average_resolution_hours"),
               ))
    type_rank = Window.partitionBy("source_city").orderBy(
        F.desc("average_resolution_hours"), F.asc("request_type")
    )
    slowest_types = (by_type
                     .withColumn("rank", F.row_number().over(type_rank))
                     .filter(F.col("rank") <= 5)
                     .orderBy("source_city", "rank"))
    return by_city, slowest_types


def categorical_distributions(df, column, output_name):
    # The denominator is every record in that city, so the percentages across categories sum to 100% (up to rounding), including the unknown category.
    totals = df.groupBy("source_city").agg(F.count(F.lit(1)).alias("city_request_count"))
    counts = df.groupBy("source_city", column).agg(F.count(F.lit(1)).alias("request_count"))
    return (counts.join(totals, "source_city")
            .withColumn("percent_of_city_requests", F.round(100.0 * F.col("request_count") / F.col("city_request_count"), 2))
            .withColumnRenamed(column, output_name)
            .orderBy("source_city", F.desc("request_count"), F.asc(output_name)))


def common_categories(distribution, category_column):
    # Rank categories within each city and keep the first one.
    rank = Window.partitionBy("source_city").orderBy(
        F.desc("request_count"), F.asc(category_column)
    )
    return (distribution.withColumn("rank", F.row_number().over(rank))
            .filter(F.col("rank") == 1)
            .drop("rank")
            .orderBy("source_city"))


def write_findings_summary(unified_source, unified_count, date_ranges, extremes,
                           top_postals, resolution_city, slowest_types, common_status, common_channel):
    # collect() is used only for compact summary results, not the raw 1.6M rows.
    path = REPORT_DIR / "findings_summary.txt"
    with path.open("w", encoding="utf-8") as out:
        out.write("CS 236 Phase 2 Spark Analysis\n")
        out.write("=============================\n\n")
        out.write(f"Unified records: {unified_count:,}\n")
        out.write(f"Unified dataset read from: {unified_source}\n\n")
        out.write("Creation-date coverage by city:\n")
        for row in date_ranges.collect():
            out.write(f"  {row['source_city']}: {row['min_created_at']} through {row['max_created_at']}\n")
        out.write("Compare city trends cautiously because the source extracts cover different date ranges.\n\n")

        out.write("Weekly request-volume extremes (ties are included; bucket is month plus day period):\n")
        for row in extremes.collect():
            out.write(
                f"  {row['source_city']} {row['extreme']}: {row['month']}, {row['month_period']} "
                f"({row['request_count']:,} requests)\n"
            )
        out.write("\nTop ten postal codes by request count:\n")
        for row in top_postals.select("source_city", "postal_code", "postal_request_count").collect():
            out.write(f"  {row['source_city']} {row['postal_code']}: {row['postal_request_count']:,}\n")

        out.write("\nResolution performance uses status=Closed, a non-null nonnegative resolution_hours value, non-null timestamps, and closed_at >= created_at.\n")
        out.write("The merge-ready status mapping groups original Cancelled/Canceled requests into Closed, so these cannot be separated from completed requests in this output; interpret resolution results with that limitation.\n")
        out.write("City averages and medians (hours):\n")
        for row in resolution_city.collect():
            out.write(
                f"  {row['source_city']}: n={row['valid_completed_request_count']:,}, "
                f"average={row['average_resolution_hours']}, median={row['median_resolution_hours']}\n"
            )
        out.write("Five request types with the longest mean resolution time (see CSV for counts):\n")
        for row in slowest_types.collect():
            out.write(
                f"  {row['source_city']} #{row['rank']} {row['request_type']}: "
                f"{row['average_resolution_hours']} hours (n={row['valid_completed_request_count']:,})\n"
            )

        out.write("\nMost common standardized status by city:\n")
        for row in common_status.collect():
            out.write(f"  {row['source_city']}: {row['status_category']} ({row['request_count']:,}, {row['percent_of_city_requests']}%)\n")
        out.write("Most common standardized submission channel by city:\n")
        for row in common_channel.collect():
            out.write(f"  {row['source_city']}: {row['channel_category']} ({row['request_count']:,}, {row['percent_of_city_requests']}%)\n")

        out.write("\nMethods and output files:\n")
        out.write("  Weekly counts use calendar-month periods: days 1-7, 8-14, 15-21, and 22-end.\n")
        out.write("  Postal code percentages use all requests in that city/postal code as denominator, including missing request types.\n")
        out.write("  Status categories are those standardized upstream to Open, Pending, and Closed (with original cancelled values included in Closed); null/unmapped statuses are reported as unknown.\n")
        out.write("  Channel groups align clear equivalents: ONLINE/Self Service, PHONE/Call, and MOBILE/Mobile App; other labels are normalized for case and spaces and retained.\n")
        out.write("  Detailed tables are saved as headered CSV output folders in this directory.\n")


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    # If no merged dataset is found in the same directory as this script, the user must run merge_datasets.py first.
    if not MERGED_SINGLE_PATH.is_file():
        raise FileNotFoundError(
            f"Required unified dataset not found: {MERGED_SINGLE_PATH}\n"
            "Run merge_datasets.py first to create merged_311.parquet."
        )

    spark = (
        SparkSession.builder
        .appName("311-Phase2-Analysis")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Spark reads the standalone parquet file as a dataframe. 
        unified = spark.read.parquet(str(MERGED_SINGLE_PATH))
        unified_source = MERGED_SINGLE_PATH

        required = {
            "source_city", "record_id", "created_at", "closed_at", "status",
            "responsible_organization", "request_type", "submission_channel",
            "postal_code", "latitude", "longitude",
        }
        missing = sorted(required - set(unified.columns))
        if missing:
            raise ValueError(f"Merge-ready data is missing required fields: {', '.join(missing)}")

        # count() is an action: it scans the unified dataset and confirms its total number of records.
        unified_count = unified.count()

        # Add analysis-only categories without changing the merged dataset.
        analyzed = (unified
                    .withColumn("source_city", F.upper(F.trim(F.col("source_city"))))
                    .withColumn("status_category", standardized_status("status"))
                    .withColumn("channel_category", standardized_channel("submission_channel")))

        date_ranges = analyzed.groupBy("source_city").agg(
            F.min("created_at").alias("min_created_at"),
            F.max("created_at").alias("max_created_at"),
        ).orderBy("source_city")

        # Build each required analysis as a spark dataframe, then write its result to a separate CSV folder.
        weekly = add_weekly_periods(analyzed)
        extremes = weekly_extremes(weekly)
        top_postals, top_types_by_postal = postal_hotspots(analyzed)
        resolution_city, slowest_types = resolution_results(analyzed)
        status_distribution = categorical_distributions(analyzed, "status_category", "status_category")
        channel_distribution = categorical_distributions(analyzed, "channel_category", "channel_category")
        common_status = common_categories(status_distribution, "status_category")
        common_channel = common_categories(channel_distribution, "channel_category")

        # Each write is an action that runs the corresponding Spark plan.
        write_csv(weekly, "weekly_volume")
        write_csv(extremes, "weekly_extremes")
        write_csv(top_postals, "top_postal_codes")
        write_csv(top_types_by_postal, "top_request_types_by_postal_code")
        write_csv(resolution_city, "resolution_by_city")
        write_csv(slowest_types, "slowest_request_types")
        write_csv(status_distribution, "status_distribution")
        write_csv(channel_distribution, "channel_distribution")
        write_csv(common_status, "most_common_status")
        write_csv(common_channel, "most_common_channel")

        write_findings_summary(
            unified_source, unified_count, date_ranges, extremes, top_postals,
            resolution_city, slowest_types, common_status, common_channel
        )
        print(f"Unified Parquet dataset read from: {unified_source}")
        print(f"Unified row count: {unified_count:,}")
        print(f"Analysis tables and findings summary: {REPORT_DIR}")
        print("Weekly extremes:")
        extremes.show(20, truncate=False)
        print("Top postal codes:")
        top_postals.show(20, truncate=False)
        print("Top request types within the top postal codes:")
        top_types_by_postal.show(60, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
