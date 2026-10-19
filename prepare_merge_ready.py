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
    ],
    "LA": [
        ("responsible_organization", "owner", "string"),
        ("request_type", "requesttype", "string"),
        ("submission_channel", "requestsource", "string"),
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
    return df.select(
        *[F.col(source).cast(data_type).alias(output) for output, source, data_type in mappings]
    )


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
