"""Combine each merge-ready Parquet dataset into one standalone Parquet file.

The existing partitioned datasets under processed/ are not changed.
"""
from pathlib import Path
import os
import shutil
import tempfile

from pyspark.sql import SparkSession

BASE = Path(__file__).resolve().parent
PROCESSED_DIR = BASE / "processed"
DATASETS = {
    "NYC": (PROCESSED_DIR / "nyc_merge_ready_parquet", BASE / "nyc_merge_ready.parquet"),
    "LA": (PROCESSED_DIR / "la_merge_ready_parquet", BASE / "la_merge_ready.parquet"),
}


def write_single_file(spark, city, input_dir, output_file):
    if not input_dir.is_dir():
        raise FileNotFoundError(f"{city} merge-ready dataset not found: {input_dir}")

    temp_dir = Path(tempfile.mkdtemp(prefix=f"_{city.lower()}_single_", dir=PROCESSED_DIR))
    try:
        spark.read.parquet(str(input_dir)).coalesce(1).write.mode("overwrite").parquet(str(temp_dir))
        part_files = list(temp_dir.glob("part-*.parquet"))
        if len(part_files) != 1:
            raise RuntimeError(f"Expected one {city} Parquet part file, found {len(part_files)}")
        os.replace(part_files[0], output_file)
        print(f"{city} single-file dataset: {output_file}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    if not PROCESSED_DIR.is_dir():
        raise FileNotFoundError(f"Processed directory not found: {PROCESSED_DIR}")

    spark = (
        SparkSession.builder
        .appName("311-single-file-exports")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        for city, (input_dir, output_file) in DATASETS.items():
            write_single_file(spark, city, input_dir, output_file)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
