from functools import reduce
from datetime import datetime
import os
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

BASE = Path(__file__).resolve().parent
NYC_PATH = BASE / "data" / "nyc_311_800k.csv"
LA_PATH = BASE / "data" / "la_311_800k.csv"
OUTPUT_DIR = BASE / "processed"
REPORT_DIR = BASE / "reports"
SUMMARY_PATH = REPORT_DIR / "cleaning_summary.txt"

NYC_DATE_COLUMNS = ["created_date", "closed_date", "due_date", "resolution_action_updated_date"]
LA_DATE_COLUMNS = ["createddate", "updateddate", "servicedate", "closeddate"]
NYC_NUMERIC_COLUMNS = ["latitude", "longitude", "x_coordinate_state_plane", "y_coordinate_state_plane"]
LA_NUMERIC_COLUMNS = ["latitude", "longitude"]


def require_windows_hadoop_helper():
    if os.name != "nt":
        return
    hadoop_home = os.environ.get("HADOOP_HOME")
    winutils = Path(hadoop_home) / "bin" / "winutils.exe" if hadoop_home else None
    if not winutils or not winutils.is_file():
        raise RuntimeError(
            "Writing Parquet from Spark on Windows requires winutils.exe. "
            "Install Windows Hadoop helper binaries compatible with Hadoop 3.3.4, "
            "set HADOOP_HOME to their parent folder (containing bin\\winutils.exe), "
            "then restart this terminal and rerun the script."
        )


require_windows_hadoop_helper()

spark = (
    SparkSession.builder
    .appName("311-cleaning")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def read_raw(path):
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    return (spark.read.option("header", True)
            .option("inferSchema", False)
            .option("mode", "PERMISSIVE")
            .csv(str(path)))


def trim_and_null_blanks(df):
    for name in df.columns:
        value = F.trim(F.col(name))
        df = df.withColumn(name, F.when(value == "", F.lit(None).cast("string")).otherwise(value))
    return df


def status_group(status_col):
    value = F.lower(F.trim(F.col(status_col)))
    return (F.when(F.col(status_col).isNull() | value.isin("unknown", "unspecified"), "unknown")
            .when(value == "closed", "closed")
            .when(value == "open", "open")
            .when(value == "pending", "pending")
            .when(value.isin("in progress", "started", "assigned"), "active")
            .when(value.isin("cancelled", "canceled"), "cancelled")
            .when(value == "referred out", "referred_out")
            .otherwise("other"))


def build_clean(city, path, id_col, date_columns, numeric_columns,
                created_col, closed_col, category_col, detail_col,
                channel_col, unit_col, address_col, postal_col):
    df = trim_and_null_blanks(read_raw(path))
    source_columns = df.columns

    # Record malformed nonblank date/coordinate values before casting. Original
    # CSV files remain untouched, so their exact source text is always recoverable.
    date_errors = []
    for name in date_columns:
        if name in source_columns:
            date_value = F.to_timestamp(F.col(name))
            date_errors.append(F.col(name).isNotNull() & date_value.isNull())
    df = df.withColumn(
        "date_parse_error",
        reduce(lambda left, right: left | right, date_errors) if date_errors else F.lit(False)
    )

    numeric_errors = []
    for name in numeric_columns:
        if name in source_columns:
            numeric_value = F.col(name).cast("double")
            numeric_errors.append(F.col(name).isNotNull() & numeric_value.isNull())
    df = df.withColumn(
        "coordinate_parse_error",
        reduce(lambda left, right: left | right, numeric_errors) if numeric_errors else F.lit(False)
    )

    for name in date_columns:
        if name in source_columns:
            df = df.withColumn(name, F.to_timestamp(F.col(name)))
    for name in numeric_columns:
        if name in source_columns:
            df = df.withColumn(name, F.col(name).cast("double"))

    created_at = F.col(created_col)
    closed_at = F.col(closed_col)
    status_original = F.col("status")
    both_dates = created_at.isNotNull() & closed_at.isNotNull()
    temporal_valid = F.when(both_dates, closed_at >= created_at)
    resolution_hours = F.when(
        both_dates & (closed_at >= created_at),
        (closed_at.cast("double") - created_at.cast("double")) / F.lit(3600.0)
    )

    # These fields have corresponding concepts, but original labels remain intact.
    common_values = {
        "source_city": F.lit(city),
        "source_request_id": F.col(id_col).cast("string"),
        "created_at": created_at,
        "closed_at": closed_at,
        "status_original": status_original,
        "status_group": status_group("status"),
        "category_original": F.col(category_col),
        "category_detail_original": (F.col(detail_col) if detail_col else F.lit(None).cast("string")),
        "intake_channel_original": F.col(channel_col),
        "responsible_unit_original": F.col(unit_col),
        "address_text": F.col(address_col),
        "postal_code": F.col(postal_col),
        "location_wkt": (F.col("location") if "location" in source_columns else F.lit(None).cast("string")),
        "temporal_order_valid": temporal_valid,
        "resolution_time_hours": resolution_hours,
        "closed_date_missing_for_closed_status": (status_group("status") == "closed") & closed_at.isNull(),
        "coordinates_within_world_bounds": F.when(
            F.col("latitude").isNotNull() & F.col("longitude").isNotNull(),
            F.col("latitude").between(-90.0, 90.0) & F.col("longitude").between(-180.0, 180.0)
        )
    }
    for name, expression in common_values.items():
        df = df.withColumn(name, expression)
    df = df.withColumn(
        "record_id",
        F.concat(F.col("source_city"), F.lit(":"), F.col("source_request_id"))
    )

    # Shared fields first; every source column remains in the cleaned output.
    common_columns = [
        "source_city", "source_request_id", "record_id", "created_at", "closed_at",
        "status_original", "status_group", "category_original", "category_detail_original",
        "intake_channel_original", "responsible_unit_original", "address_text", "postal_code",
        "latitude", "longitude", "location_wkt", "resolution_time_hours",
        "temporal_order_valid", "closed_date_missing_for_closed_status",
        "date_parse_error", "coordinate_parse_error", "coordinates_within_world_bounds"
    ]
    output_columns = common_columns + [name for name in df.columns if name not in common_columns]
    df = df.select(*output_columns)

    summary = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("date_parse_error").cast("long")).alias("date_parse_error_rows"),
        F.sum(F.col("coordinate_parse_error").cast("long")).alias("coordinate_parse_error_rows"),
        F.sum((F.col("temporal_order_valid") == F.lit(False)).cast("long")).alias("closed_before_created_rows"),
        F.sum(F.col("closed_date_missing_for_closed_status").cast("long")).alias("closed_status_missing_close_time_rows"),
        F.sum((F.col("coordinates_within_world_bounds") == F.lit(False)).cast("long")).alias("coordinates_outside_world_bounds_rows"),
        F.min("resolution_time_hours").alias("min_resolution_hours"),
        F.max("resolution_time_hours").alias("max_resolution_hours")
    ).first().asDict()
    return df, summary


def main():
    REPORT_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)
    nyc_output = OUTPUT_DIR / "nyc_cleaned_parquet"
    la_output = OUTPUT_DIR / "la_cleaned_parquet"
    completed = {}
    for output in (nyc_output, la_output):
        if output.exists():
            if (output / "_SUCCESS").is_file():
                completed[output] = True
                print(f"Keeping completed output: {output}")
            else:
                # A failed Spark write can leave an incomplete directory. Move
                # it aside intact so this run can retry without overwriting it.
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                archived = output.with_name(f"{output.name}_incomplete_{stamp}")
                output.rename(archived)
                completed[output] = False
                print(f"Preserved incomplete output at: {archived}")
        else:
            completed[output] = False

    nyc, nyc_summary = build_clean(
        "NYC", NYC_PATH, "unique_key", NYC_DATE_COLUMNS, NYC_NUMERIC_COLUMNS,
        "created_date", "closed_date", "complaint_type", "descriptor",
        "open_data_channel_type", "agency", "incident_address", "incident_zip"
    )
    la, la_summary = build_clean(
        "LA", LA_PATH, "srnumber", LA_DATE_COLUMNS, LA_NUMERIC_COLUMNS,
        "createddate", "closeddate", "requesttype", None,
        "requestsource", "owner", "address", "zipcode"
    )

    if not completed[nyc_output]:
        nyc.write.mode("errorifexists").parquet(str(nyc_output))
    if not completed[la_output]:
        la.write.mode("errorifexists").parquet(str(la_output))

    with SUMMARY_PATH.open("w", encoding="utf-8") as summary_file:
        for city, output, values in [
            ("NYC", nyc_output, nyc_summary),
            ("LA", la_output, la_summary),
        ]:
            summary_file.write(f"{city} cleaning summary\n")
            summary_file.write(f"Output: {output}\n")
            for key, value in values.items():
                summary_file.write(f"{key}: {value}\n")
            summary_file.write("\n")
        summary_file.write(
            "No source columns or rows are intentionally dropped. Blank strings are nullified, "
            "selected dates and coordinates are typed, source fields are preserved, and shared "
            "integration fields are added. See docs/eda_findings.md for the rationale.\n"
        )
    print(f"NYC cleaned dataset: {nyc_output}")
    print(f"LA cleaned dataset: {la_output}")
    print(f"Cleaning summary: {SUMMARY_PATH}")


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
