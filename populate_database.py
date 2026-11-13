
from pathlib import Path
import os

import psycopg2
from psycopg2 import sql

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    ShortType,
    ByteType,
    FloatType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
)

BASE = Path(__file__).resolve().parent
PROCESSED = BASE / "processed"

DATASETS = {
    "nyc_requests": PROCESSED / "nyc_cleaned_parquet",
    "la_requests": PROCESSED / "la_cleaned_parquet",
    "merged_requests": PROCESSED / "merged_311_parquet",
}

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "cs236_db")
DB_USER = os.getenv("DB_USER", "cs236")
DB_PASSWORD = os.environ["DB_PASSWORD"]

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
    "batchsize": "5000",
}

def postgres_type(data_type):
    if isinstance(data_type, StringType):
        return "TEXT"
    if isinstance(data_type, IntegerType):
        return "INTEGER"
    if isinstance(data_type, LongType):
        return "BIGINT"
    if isinstance(data_type, (ShortType, ByteType)):
        return "SMALLINT"
    if isinstance(data_type, FloatType):
        return "REAL"
    if isinstance(data_type, DoubleType):
        return "DOUBLE PRECISION"
    if isinstance(data_type, BooleanType):
        return "BOOLEAN"
    if isinstance(data_type, DateType):
        return "DATE"
    if isinstance(data_type, TimestampType):
        return "TIMESTAMP"
    if isinstance(data_type, DecimalType):
        return f"NUMERIC({data_type.precision}, {data_type.scale})"

    raise TypeError(f"Unsupported Spark type: {data_type}")

def create_table(connection, table_name, schema):
    columns = []

    for field in schema.fields:
        column_type = postgres_type(field.dataType)

        if field.name == "record_id":
            definition = sql.SQL("{} {} PRIMARY KEY").format(
                sql.Identifier(field.name),
                sql.SQL(column_type),
            )
        else:
            definition = sql.SQL("{} {}").format(
                sql.Identifier(field.name),
                sql.SQL(column_type),
            )

        columns.append(definition)

    statement = sql.SQL("CREATE TABLE {} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(columns),
    )

    with connection.cursor() as cursor:
        cursor.execute(statement)

    connection.commit()

def validate_ids(df, table_name):
    missing = df.filter(
        F.col("record_id").isNull()
        | (F.trim(F.col("record_id")) == "")
    ).count()

    duplicates = (
        df.groupBy("record_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if missing or duplicates:
        raise ValueError(
            f"{table_name}: {missing} missing IDs, "
            f"{duplicates} duplicate IDs"
        )

def verify_table(connection, table_name, df, expected_count):
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(table_name)
            )
        )
        actual_count = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        actual_columns = cursor.fetchall()

        cursor.execute(
            sql.SQL(
                "SELECT COUNT(*) FROM {} WHERE record_id IS NULL"
            ).format(sql.Identifier(table_name))
        )
        null_ids = cursor.fetchone()[0]

        cursor.execute(
            sql.SQL("SELECT * FROM {} LIMIT 3").format(
                sql.Identifier(table_name)
            )
        )
        sample_rows = cursor.fetchall()

    print(f"\nTable: {table_name}")
    print(f"Expected rows: {expected_count}")
    print(f"Actual rows:   {actual_count}")
    print(f"Columns:       {len(actual_columns)}")
    print(f"Null IDs:      {null_ids}")

    for name, data_type in actual_columns:
        print(f"  {name}: {data_type}")

    print("Sample records:")
    for row in sample_rows:
        print(row)

    if actual_count != expected_count:
        raise ValueError(f"Row count mismatch in {table_name}")

    if len(actual_columns) != len(df.schema.fields):
        raise ValueError(f"Column count mismatch in {table_name}")

    if null_ids:
        raise ValueError(f"Null record IDs in {table_name}")

    print(f"PASS: {table_name}")

def main():
    for table_name, path in DATASETS.items():
        if not path.is_dir():
            raise FileNotFoundError(
                f"Missing dataset for {table_name}: {path}"
            )

    spark = (
        SparkSession.builder
        .appName("CS236-PostgreSQL-Population")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    connection = None

    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )

        # Load and validate all inputs before replacing tables.
        datasets = {}

        for table_name, path in DATASETS.items():
            print(f"\nReading {table_name}...")

            df = spark.read.parquet(str(path))
            validate_ids(df, table_name)

            row_count = df.count()
            datasets[table_name] = (df, row_count)

            print(f"Rows: {row_count}")
            df.printSchema()

        for table_name, (df, row_count) in datasets.items():
            print(f"\nCreating table: {table_name}")

            # Recreate the project table on each run.
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}").format(
                        sql.Identifier(table_name)
                    )
                )
            connection.commit()

            create_table(connection, table_name, df.schema)

            print(f"Loading {row_count} records...")

            (
                df.write
                .mode("append")
                .jdbc(
                    url=JDBC_URL,
                    table=table_name,
                    properties=JDBC_PROPERTIES,
                )
            )

            verify_table(
                connection,
                table_name,
                df,
                row_count,
            )

        print("\nAll three datasets loaded successfully!")

    finally:
        if connection is not None:
            connection.close()
        spark.stop()


if __name__ == "__main__":
    main()
