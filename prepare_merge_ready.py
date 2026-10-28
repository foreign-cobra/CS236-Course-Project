
"""Create compact, consistently named Parquet datasets for NYC and LA 311.

Reads the full cleaned Parquet datasets and writes selected merge-ready columns
under processed/. The full cleaned datasets are left untouched.
"""
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

BASE = Path(__file__).resolve().parent
PROCESSED_DIR = BASE / "processed"

INPUTS = {
    "NYC": PROCESSED_DIR / "nyc_cleaned_parquet",
    "LA": PROCESSED_DIR / "la_cleaned_parquet",
}
OUTPUTS = {
    "NYC": PROCESSED_DIR / "nyc_merge_ready_parquet",
    "LA": PROCESSED_DIR / "la_merge_ready_parquet",
}

# Each tuple is: output field, source field, Spark type.
COMMON_FIELDS = [
    ("source_city", "source_city", "string"),
    ("record_id", "record_id", "string"),
    ("created_at", "created_at", "timestamp"),
    ("closed_at", "closed_at", "timestamp"),
    ("status", "status_original", "string"),
    ("resolution_hours", "resolution_time_hours", "double"),
]
GEO_FIELDS = [
    ("postal_code", "postal_code", "string"),
    ("latitude", "latitude", "double"),
    ("longitude", "longitude", "double"),
]
CITY_FIELDS = {
    "NYC": [
        ("responsible_organization", "agency", "string"),
        ("request_type", "complaint_type", "string"),
        ("submission_channel", "open_data_channel_type", "string"),
        ("address", "incident_address", "string"),
        ("council_district", "council_district", "string"),
    ],
    "LA": [
        ("responsible_organization", "owner", "string"),
        ("request_type", "requesttype", "string"),
        ("submission_channel", "requestsource", "string"),
        ("address", "address", "string"),
        ("council_district", "cd", "string"),
    ],
}


def select_merge_fields(df, city):
    mappings = COMMON_FIELDS + CITY_FIELDS[city] + GEO_FIELDS
    missing = [source for _, source, _ in mappings if source not in df.columns]
    if missing:
        raise ValueError(
            f"{city} cleaned dataset is missing expected columns: {', '.join(missing)}. "
            "Check that clean_datasets.py completed successfully and that this script "
            "is in the same project folder."
        )

    selected = df.select([
        F.col(source).cast(dtype).alias(output)
        for output, source, dtype in mappings
    ])

    # Standardize status into Open, Pending, and Closed.
    status = F.lower(F.trim(F.col("status")))

    selected = selected.withColumn(
        "status",
        F.when(status.isin("open", "started", "assigned", "in progress"), "Open")
         .when(status.isin("pending", "forward", "referred out"), "Pending")
         .when(status.isin("closed", "cancelled", "canceled"), "Closed")
         .otherwise(F.lit(None).cast("string"))
    )

    # Standardize submission channels across both cities.
    channel = F.lower(F.trim(F.col("submission_channel")))

    selected = selected.withColumn(
        "submission_channel",
        F.when(channel.isin("online", "self service"), "Online")
         .when(channel.isin("mobile", "mobile app"), "Mobile App")
         .when(channel.isin("phone", "call"), "Phone")
         .when(channel == "email", "Email")
         .when(channel.isin("unknown", ""), "Unknown")
         .otherwise(F.col("submission_channel"))
    )

    # Standardize five-digit ZIP codes.
    selected = selected.withColumn(
        "postal_code",
        F.regexp_extract(F.trim(F.col("postal_code")), r"^(\d{5})(?:-\d{4})?$", 1)
    )

    selected = selected.withColumn(
        "postal_code",
        F.when(F.col("postal_code") == "", None)
         .otherwise(F.col("postal_code"))
    )

    # Remove invalid coordinates from the merge-ready output.
    selected = selected.withColumn(
        "latitude",
        F.when(F.col("latitude").between(-90, 90), F.col("latitude"))
    ).withColumn(
        "longitude",
        F.when(F.col("longitude").between(-180, 180), F.col("longitude"))
    )

    # Generate a consistent WKT location from coordinates.
    selected = selected.withColumn(
        "location",
        F.when(
            F.col("latitude").isNotNull() & F.col("longitude").isNotNull(),
            F.concat(
                F.lit("POINT ("),
                F.col("longitude").cast("string"),
                F.lit(" "),
                F.col("latitude").cast("string"),
                F.lit(")")
            )
        )
    )

    return selected


def main():
    for city, input_path in INPUTS.items():
        if not input_path.is_dir():
            raise FileNotFoundError(
                f"Could not find {city} cleaned Parquet dataset: {input_path}\n"
                "Run clean_datasets.py first."
            )

    spark = (
        SparkSession.builder
        .appName("311-merge-ready-extracts")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        for city in ("NYC", "LA"):
            cleaned = spark.read.parquet(str(INPUTS[city]))
            merge_ready = select_merge_fields(cleaned, city)
            output = OUTPUTS[city]
            # These are derived extracts; rerunning replaces only these extracts.
            merge_ready.write.mode("overwrite").parquet(str(output))
            print(f"{city} merge-ready dataset: {output}")
            print(f"Columns ({len(merge_ready.columns)}): {', '.join(merge_ready.columns)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
