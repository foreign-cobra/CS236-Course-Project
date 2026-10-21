
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = Path(__file__).resolve().parent

NYC_PATH = BASE / "processed" / "nyc_merge_ready_parquet"
LA_PATH = BASE / "processed" / "la_merge_ready_parquet"
OUTPUT_PATH = BASE / "processed" / "merged_311_parquet"

EXPECTED_TYPES = {
    "source_city": "string",
    "record_id": "string",
    "created_at": "timestamp",
    "closed_at": "timestamp",
    "status": "string",
    "resolution_hours": "double",
    "responsible_organization": "string",
    "request_type": "string",
    "submission_channel": "string",
    "address": "string",
    "council_district": "string",
    "postal_code": "string",
    "latitude": "double",
    "longitude": "double",
    "location": "string",
}

spark = (
    SparkSession.builder
    .appName("Merge311Datasets")
    .master("local[*]")
    .getOrCreate()
)

try:
    nyc = spark.read.parquet(str(NYC_PATH))
    la = spark.read.parquet(str(LA_PATH))

    print("\nVALIDATING SCHEMAS")

    # Check for missing or unexpected columns before attempting the merge.
    # Comparing sets allows the columns to appear in different orders.
    expected_columns = set(EXPECTED_TYPES.keys())

    assert set(nyc.columns) == expected_columns, (
        "NYC columns do not match the expected schema!"
    )

    assert set(la.columns) == expected_columns, (
        "LA columns do not match the expected schema!"
    )

    # Verify that corresponding columns use the expected Spark data types.
    # This prevents incompatible values from being combined or cast implicitly.
    for city, df in [("NYC", nyc), ("LA", la)]:
        actual_types = dict(df.dtypes)

        for column, expected_type in EXPECTED_TYPES.items():
            assert actual_types[column] == expected_type, (
                f"{city}: {column} has type "
                f"{actual_types[column]}, expected {expected_type}"
            )

    print("PASS: Both datasets have the expected columns and data types.")

    # Stack NYC and LA records into one DataFrame.
    # unionByName matches columns by name rather than position,
    # preserving every row without performing a SQL-style join.
    merged = nyc.unionByName(la)

    # Store the unified dataset in Parquet format for efficient
    # reading and processing in later phases of the project.
    merged.write.mode("overwrite").parquet(str(OUTPUT_PATH))

    result = spark.read.parquet(str(OUTPUT_PATH))

    print(f"\nMerged dataset saved to: {OUTPUT_PATH}")

    print("\nVALIDATING RECORD COUNTS")

    nyc_count = nyc.count()
    la_count = la.count()
    merged_count = result.count()

    print(f"NYC rows: {nyc_count:,}")
    print(f"LA rows: {la_count:,}")
    print(f"Merged rows: {merged_count:,}")
    print(f"Expected rows: {nyc_count + la_count:,}")

    assert merged_count == nyc_count + la_count, (
        "ERROR: Merged row count does not match!"
    )

    print("PASS: All records preserved.")

    result.groupBy("source_city").count().show()

    print("\nVALIDATING RECORD IDS")

    # record_id includes a city prefix, allowing requests from
    # different cities to have distinct identifiers in one database.
    # Grouping by ID reveals identifiers assigned to multiple records.
    duplicate_ids = (
        result.groupBy("record_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    missing_ids = result.filter(
        F.col("record_id").isNull() |
        (F.trim(F.col("record_id")) == "")
    ).count()

    print(f"Duplicate record IDs: {duplicate_ids}")
    print(f"Missing record IDs: {missing_ids}")

    if duplicate_ids == 0 and missing_ids == 0:
        print("PASS: All record IDs are unique and present.")
    else:
        print("WARNING: Invalid or duplicate record IDs found.")

    print("\nNULL VALUES PER COLUMN")

    # Count missing values across all 15 columns to identify
    # incomplete attributes and document data quality.
    # Some nulls are expected, such as closed_at for open requests.
    null_counts = result.select([
        F.count(
            F.when(F.col(column).isNull(), 1)
        ).alias(column)
        for column in result.columns
    ])

    null_counts.show(vertical=True, truncate=False)

    print("\nVALIDATING RESOLUTION HOURS")

    # Resolution time represents the elapsed hours between request
    # creation and closure. Negative values indicate invalid timing.
    negative_hours = result.filter(
        F.col("resolution_hours") < 0
    ).count()

    hours_without_closure = result.filter(
        F.col("closed_at").isNull() &
        F.col("resolution_hours").isNotNull()
    ).count()

    hours_without_creation = result.filter(
        F.col("created_at").isNull() &
        F.col("resolution_hours").isNotNull()
    ).count()

    expected_hours = (
        F.col("closed_at").cast("long") -
        F.col("created_at").cast("long")
    ) / 3600.0

    incorrect_hours = result.filter(
        F.col("resolution_hours").isNotNull() &
        F.col("created_at").isNotNull() &
        F.col("closed_at").isNotNull() &
        (
            F.abs(
                F.col("resolution_hours") - expected_hours
            ) > 0.001
        )
    ).count()

    # Requests with two valid, chronologically ordered timestamps
    # should have a calculated resolution time.
    missing_resolution = result.filter(
        F.col("created_at").isNotNull() &
        F.col("closed_at").isNotNull() &
        (F.col("closed_at") >= F.col("created_at")) &
        F.col("resolution_hours").isNull()
    ).count()

    print(f"Negative resolution hours: {negative_hours}")
    print(f"Hours without closure date: {hours_without_closure}")
    print(f"Hours without creation date: {hours_without_creation}")
    print(f"Incorrect calculations: {incorrect_hours}")
    print(f"Missing hours for valid date pairs: {missing_resolution}")

    # Summarize the number of issues found by the individual checks.
    # These counts can overlap if a record violates multiple conditions.
    resolution_errors = (
        negative_hours +
        hours_without_closure +
        hours_without_creation +
        incorrect_hours +
        missing_resolution
    )

    if resolution_errors == 0:
        print("PASS: Resolution hours are valid.")
    else:
        print("WARNING: Resolution-hour issues detected.")

    print("\nVALIDATING COORDINATES")

    invalid_coordinates = result.filter(
        (F.col("latitude").isNotNull() &
         ((F.col("latitude") < -90) |
          (F.col("latitude") > 90))) |
        (F.col("longitude").isNotNull() &
         ((F.col("longitude") < -180) |
          (F.col("longitude") > 180)))
    ).count()

    print(f"Out-of-range coordinates: {invalid_coordinates}")

    if invalid_coordinates == 0:
        print("PASS: All non-null coordinates are within world bounds.")
    else:
        print("WARNING: Invalid coordinates found.")

    # Display the final schema and a small selection of records
    # to confirm that the merged output is readable and structured.
    print("\nFINAL MERGED SCHEMA")
    result.printSchema()

    print("\nSAMPLE RECORDS")
    result.select(
        "source_city",
        "record_id",
        "created_at",
        "closed_at",
        "status",
        "resolution_hours",
        "request_type"
    ).show(10, truncate=False)

    print("\n========== VALIDATION SUMMARY ==========")

    print(f"Total records: {merged_count:,}")
    print(f"Total columns: {len(result.columns)}")
    print(f"Duplicate IDs: {duplicate_ids}")
    print(f"Missing IDs: {missing_ids}")
    print(f"Resolution errors: {resolution_errors}")
    print(f"Invalid coordinates: {invalid_coordinates}")

    # Report whether the major integrity checks passed.
    # Null counts are reported separately because missing values
    # are not necessarily data errors.
    if (
        duplicate_ids == 0
        and missing_ids == 0
        and resolution_errors == 0
        and invalid_coordinates == 0
    ):
        print("PASS: All validation checks passed.")
    else:
        print("WARNING: Some validation checks need review.")

finally:
    spark.stop()
