"""Display Phase 2 Spark analysis tables in the terminal.

The Spark CSV outputs are directories containing one or more part-*.csv files.
This script reads those files and prints each result in a table. 
"""
import argparse
import csv
import sys
from pathlib import Path

TABLES = [
    "weekly_volume",
    "weekly_extremes",
    "top_postal_codes",
    "top_request_types_by_postal_code",
    "resolution_by_city",
    "slowest_request_types",
    "status_distribution",
    "channel_distribution",
    "most_common_status",
    "most_common_channel",
]


def read_spark_csv_folder(folder):
    """Read and combine Spark CSV part files, skipping repeated headers."""
    part_files = sorted(folder.glob("part-*.csv"))
    if not part_files:
        return None, []

    header = None
    rows = []
    for part_file in part_files:
        with part_file.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.reader(csv_file)
            file_rows = list(reader)
        if not file_rows:
            continue
        if header is None:
            header = file_rows[0]
            rows.extend(file_rows[1:])
        else:
            # Spark writes a header in every part file when header=True.
            if file_rows[0] == header:
                rows.extend(file_rows[1:])
            else:
                rows.extend(file_rows)
    return header, rows


def print_table(title, header, rows):
    print(f"\n--- {title} ---")
    if header is None:
        print("No CSV part files found.")
        return
    if not rows:
        print("The CSV contains a header but no data rows.")
        print(", ".join(header))
        return

    column_count = len(header)
    widths = [len(str(value)) for value in header]
    for row in rows:
        for index in range(column_count):
            value = row[index] if index < len(row) else ""
            widths[index] = max(widths[index], len(value))

    def format_row(row):
        cells = [row[index] if index < len(row) else "" for index in range(column_count)]
        return " | ".join(cell.ljust(widths[index]) for index, cell in enumerate(cells))

    print(format_row(header))
    print("-+-".join("-" * width for width in widths))
    for row in rows:
        print(format_row(row))
    print(f"Rows: {len(rows):,}")


def main():
    parser = argparse.ArgumentParser(
        description="Print the Phase 2 Spark analysis CSV tables."
    )
    parser.add_argument(
        "--analysis-dir",
        type=Path,
        help="Folder containing the Spark table folders (default: reports/phase2_analysis beside this script).",
    )
    args = parser.parse_args()

    base = Path(__file__).resolve().parent
    analysis_dir = args.analysis_dir or (base / "reports" / "phase2_analysis")
    if not analysis_dir.is_dir():
        print(f"Analysis folder not found: {analysis_dir}", file=sys.stderr)
        return 1

    missing = []
    for table in TABLES:
        header, rows = read_spark_csv_folder(analysis_dir / table)
        if header is None:
            missing.append(table)
            print_table(table, None, [])
        else:
            print_table(table, header, rows)

    if missing:
        print(
            "\nMissing analysis tables: " + ", ".join(missing)
            + "\nRun phase2_analysis.py to generate them."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
