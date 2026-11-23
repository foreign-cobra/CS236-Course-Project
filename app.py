"""Streamlit search, filter, pagination, and insert UI for the 311 PostgreSQL database."""

from datetime import date, datetime, timedelta
import os
import re
from uuid import uuid4

import pandas as pd
import psycopg2
import streamlit as st
from psycopg2 import sql

 
# Table and column names are fixed here; user-provided filter values are always
# passed as SQL parameters rather than interpolated into SQL text.
DATASETS = {
    "NYC cleaned": {
        "table": "nyc_requests",
        "status": "status_group",
        "organization": "responsible_unit_original",
        "request_type": "category_original",
        "channel": "intake_channel_original",
        "display": [
            "record_id", "created_at", "closed_at", "status_group",
            "responsible_unit_original", "category_original",
            "intake_channel_original", "postal_code", "latitude", "longitude",
        ],
    },
    "LA cleaned": {
        "table": "la_requests",
        "status": "status_group",
        "organization": "responsible_unit_original",
        "request_type": "category_original",
        "channel": "intake_channel_original",
        "display": [
            "record_id", "created_at", "closed_at", "status_group",
            "responsible_unit_original", "category_original",
            "intake_channel_original", "postal_code", "latitude", "longitude",
        ],
    },
    "Unified": {
        "table": "merged_requests",
        "status": "status",
        "organization": "responsible_organization",
        "request_type": "request_type",
        "channel": "submission_channel",
        "display": [
            "record_id", "source_city", "created_at", "closed_at", "status",
            "responsible_organization", "request_type", "submission_channel",
            "postal_code", "latitude", "longitude",
        ],
    },
}


def connect_db():
    """Open a PostgreSQL connection using environment variables."""
    password = os.getenv("DB_PASSWORD")
    if not password:
        raise RuntimeError("DB_PASSWORD is not set. Copy .env.example to .env and set it.")
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "cs236_db"),
        user=os.getenv("DB_USER", "cs236"),
        password=password,
        connect_timeout=5,
    )


def get_distinct_values(connection, table, column):
    """Read bounded filter options from PostgreSQL."""
    query = sql.SQL(
        "SELECT DISTINCT {column} FROM {table} "
        "WHERE {column} IS NOT NULL AND BTRIM({column}::text) <> '' "
        "ORDER BY {column} LIMIT 1000"
    ).format(column=sql.Identifier(column), table=sql.Identifier(table))
    with connection.cursor() as cursor:
        cursor.execute(query)
        return [str(row[0]) for row in cursor.fetchall()]


def build_where(config, selections, start_date, end_date, postal_code):
    """Build a parameterized WHERE clause for any combination of filters."""
    conditions = []
    values = []

    if selections.get("source_city") is not None:
        conditions.append(sql.SQL("{} = %s").format(sql.Identifier("source_city")))
        values.append(selections["source_city"])

    for key, value in selections.items():
        if key == "source_city" or value is None:
            continue
        conditions.append(sql.SQL("{} = %s").format(sql.Identifier(config[key])))
        values.append(value)

    if start_date:
        conditions.append(sql.SQL("{} >= %s").format(sql.Identifier("created_at")))
        values.append(start_date)
    if end_date:
        # Use an exclusive next-day bound so the selected end date includes all
        # timestamps on that date.
        conditions.append(sql.SQL("{} < %s").format(sql.Identifier("created_at")))
        values.append(end_date + timedelta(days=1))
    if postal_code:
        conditions.append(sql.SQL("{} = %s").format(sql.Identifier("postal_code")))
        values.append(postal_code)

    where_clause = (
        sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conditions)
        if conditions else sql.SQL("")
    )
    return where_clause, values


def fetch_page(connection, config, where_clause, values, page_size, page_number):
    """Count matching rows and fetch only the requested page from PostgreSQL."""
    table = sql.Identifier(config["table"])
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}{}").format(table, where_clause),
            values,
        )
        total = cursor.fetchone()[0]

        columns = sql.SQL(", ").join(
            sql.Identifier(name) for name in config["display"]
        )
        query = sql.SQL(
            "SELECT {columns} FROM {table}{where} "
            "ORDER BY created_at DESC NULLS LAST, record_id "
            "LIMIT %s OFFSET %s"
        ).format(columns=columns, table=table, where=where_clause)
        offset = (page_number - 1) * page_size
        cursor.execute(query, values + [page_size, offset])
        rows = cursor.fetchall()
    return total, pd.DataFrame(rows, columns=config["display"])


def parse_optional_float(value, label):
    value = value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be a number.") from exc


def insert_unified_request(connection, payload):
    """Insert one validated request directly into the unified PostgreSQL table."""
    columns = list(payload.keys())
    statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier("merged_requests"),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    with connection.cursor() as cursor:
        cursor.execute(statement, list(payload.values()))
    connection.commit()


def main():
    st.set_page_config(page_title="311 Service Requests", layout="wide")
    st.title("311 Service Request Explorer")
    st.caption("Searches run against PostgreSQL. Results are paginated and limited.")

    dataset_label = st.sidebar.selectbox("Dataset", list(DATASETS))
    config = DATASETS[dataset_label]

    try:
        connection = connect_db()
    except (psycopg2.Error, RuntimeError, ValueError) as exc:
        st.error(f"Could not connect to PostgreSQL: {exc}")
        st.info("Start the database and check the DB_* values in your local .env file.")
        st.stop()

    try:
        with st.sidebar.form("search_filters"):
            st.subheader("Filters")
            city_options = get_distinct_values(connection, config["table"], "source_city")
            city_choice = st.selectbox("Source city", ["All"] + city_options)

            status_options = get_distinct_values(connection, config["table"], config["status"])
            status_choice = st.selectbox("Request status", ["All"] + status_options)

            organization_options = get_distinct_values(
                connection, config["table"], config["organization"]
            )
            organization_choice = st.selectbox(
                "Responsible organization", ["All"] + organization_options
            )

            type_options = get_distinct_values(
                connection, config["table"], config["request_type"]
            )
            type_choice = st.selectbox("Request type", ["All"] + type_options)

            channel_options = get_distinct_values(
                connection, config["table"], config["channel"]
            )
            channel_choice = st.selectbox(
                "Submission channel", ["All"] + channel_options
            )

            postal_choice = st.text_input("Postal code (exact match)").strip()
            start_text = st.text_input("Created on or after (YYYY-MM-DD)").strip()
            end_text = st.text_input("Created on or before (YYYY-MM-DD)").strip()
            st.form_submit_button("Apply filters")

        try:
            start_date = date.fromisoformat(start_text) if start_text else None
            end_date = date.fromisoformat(end_text) if end_text else None
        except ValueError:
            st.error("Enter dates in YYYY-MM-DD format.")
            st.stop()
        if start_date and end_date and start_date > end_date:
            st.error("The start date must be on or before the end date.")
            st.stop()
        if postal_choice and not re.fullmatch(r"\d{5}", postal_choice):
            st.error("Enter a five-digit postal code.")
            st.stop()

        selections = {
            "source_city": None if city_choice == "All" else city_choice,
            "status": None if status_choice == "All" else status_choice,
            "organization": None if organization_choice == "All" else organization_choice,
            "request_type": None if type_choice == "All" else type_choice,
            "channel": None if channel_choice == "All" else channel_choice,
        }
        where_clause, values = build_where(
            config, selections, start_date, end_date, postal_choice
        )

        page_size = st.sidebar.selectbox("Rows per page", [25, 50, 100, 200], index=1)
        total, _ = fetch_page(connection, config, where_clause, values, 1, 1)
        page_count = max(1, (total + page_size - 1) // page_size)
        page_number = st.sidebar.number_input(
            "Page", min_value=1, max_value=page_count, value=1, step=1
        )
        total, results = fetch_page(
            connection, config, where_clause, values, page_size, int(page_number)
        )

        st.subheader(f"{dataset_label} results")
        st.write(f"{total:,} matching records · page {int(page_number)} of {page_count}")
        st.dataframe(results, use_container_width=True, hide_index=True)

        if dataset_label == "Unified":
            st.divider()
            st.subheader("Add a request to the unified dataset")
            with st.form("insert_request", clear_on_submit=True):
                left, right = st.columns(2)
                with left:
                    source_city = st.selectbox("Source city *", ["NYC", "LA"])
                    request_type = st.text_input("Request type *")
                    responsible_organization = st.text_input("Responsible organization")
                    submission_channel = st.text_input("Submission channel")
                    status = st.selectbox("Status *", ["Open", "Pending", "Closed"])
                    created_date = st.date_input("Created date *", value=date.today())
                    created_time = st.time_input(
                        "Created time *", value=datetime.now().time().replace(second=0, microsecond=0)
                    )
                with right:
                    has_closed_at = st.checkbox("Provide a close timestamp")
                    closed_date = st.date_input("Closed date", value=date.today(), disabled=not has_closed_at)
                    closed_time = st.time_input(
                        "Closed time",
                        value=datetime.now().time().replace(second=0, microsecond=0),
                        disabled=not has_closed_at,
                    )
                    postal_code = st.text_input("Postal code")
                    address = st.text_input("Address")
                    council_district = st.text_input("Council district")
                    latitude_text = st.text_input("Latitude (optional)")
                    longitude_text = st.text_input("Longitude (optional)")
                submitted = st.form_submit_button("Insert request")

            if submitted:
                try:
                    if not request_type.strip():
                        raise ValueError("Request type is required.")
                    created_at = datetime.combine(created_date, created_time)
                    closed_at = (
                        datetime.combine(closed_date, closed_time)
                        if has_closed_at else None
                    )
                    if status == "Closed" and closed_at is None:
                        raise ValueError("A Closed request needs a close timestamp.")
                    if status != "Closed" and closed_at is not None:
                        raise ValueError("Only Closed requests may have a close timestamp.")
                    if closed_at and closed_at < created_at:
                        raise ValueError("Close time cannot be earlier than creation time.")

                    postal_code = postal_code.strip() or None
                    if postal_code and not re.fullmatch(r"\d{5}", postal_code):
                        raise ValueError("Postal code must contain five digits.")
                    latitude = parse_optional_float(latitude_text, "Latitude")
                    longitude = parse_optional_float(longitude_text, "Longitude")
                    if (latitude is None) != (longitude is None):
                        raise ValueError("Enter both latitude and longitude, or leave both blank.")
                    if latitude is not None and not -90 <= latitude <= 90:
                        raise ValueError("Latitude must be between -90 and 90.")
                    if longitude is not None and not -180 <= longitude <= 180:
                        raise ValueError("Longitude must be between -180 and 180.")

                    resolution_hours = (
                        (closed_at - created_at).total_seconds() / 3600.0
                        if closed_at else None
                    )
                    location = (
                        f"POINT ({longitude} {latitude})"
                        if latitude is not None else None
                    )
                    record_id = f"MANUAL:{source_city}:{uuid4().hex}"
                    payload = {
                        "source_city": source_city,
                        "record_id": record_id,
                        "created_at": created_at,
                        "closed_at": closed_at,
                        "status": status,
                        "resolution_hours": resolution_hours,
                        "responsible_organization": responsible_organization.strip() or None,
                        "request_type": request_type.strip(),
                        "submission_channel": submission_channel.strip() or None,
                        "address": address.strip() or None,
                        "council_district": council_district.strip() or None,
                        "postal_code": postal_code,
                        "latitude": latitude,
                        "longitude": longitude,
                        "location": location,
                    }
                    insert_unified_request(connection, payload)
                    st.success(f"Inserted request {record_id}.")
                except (ValueError, psycopg2.Error) as exc:
                    connection.rollback()
                    st.error(f"Request was not inserted: {exc}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()

