# Phase 1 EDA findings and integration implications

## Scope and method

The PySpark scan loaded the two CSV extracts from their original text values and compared those with Spark-inferred type suggestions. It profiled dimensions, representative records, blank/null counts, request-ID uniqueness, exact duplicate rows, selected date ranges and parse failures, top values for selected categorical fields, and numeric ranges for geographic coordinates. This combination helps identify schema mismatches, data-quality work, and attributes suitable for later cross-city analysis. The measurements below describe these extracts only; they should not be generalized to all NYC or LA 311 data.

## Dimensions and schema

- NYC: 800,000 rows and 44 columns.
- LA: 800,000 rows and 34 columns.
- Only four column names match exactly: `status`, `latitude`, `longitude`, and `location`.
- Likely conceptual overlaps include request ID (`unique_key` / `srnumber`), created time (`created_date` / `createddate`), closed time (`closed_date` / `closeddate`), category (`complaint_type` and `descriptor` / `requesttype`), and intake channel (`open_data_channel_type` / `requestsource`). These are candidate mappings, not proof that category, channel, or status values have identical meanings.
- Both sources contain latitude/longitude and a WKT-like `location` value. NYC also has State Plane X/Y coordinates; LA has TBM grid references. Keep city-specific coordinate systems and administrative geographies separate.
- Spark inferred NYC `unique_key` as an integer, NYC `bbl` as bigint, NYC `council_district` as integer, and several LA address/grid/district fields as integers. These are identifiers or codes, not quantities. Preserve them as strings in cleaned output so leading zeros and formatting are not lost. Treat latitude/longitude and State Plane coordinates as numeric.

**Benefit:** Comparing raw headers and inferred types identifies where shared names are absent and where automatic type inference could damage identifiers. The cleaned schema should therefore use explicit common fields and preserve source fields for traceability.

## Completeness and record identity

- Both request identifiers were present on all 800,000 rows and had 800,000 distinct values.
- Exact duplicate-row count was zero in each extract. No deduplication is currently justified; verify again after transformations if any rows are filtered or joined.
- NYC highest blank rates: taxi company borough 99.92%, road ramp 99.75%, bridge highway direction 99.73%, due date 99.61%, bridge/highway name and segment about 99.52%, taxi pickup location 99.02%, facility type 96.16%, vehicle type 95.05%, descriptor_2 53.03%, landmark 32.47%, and closed_date 10.51%.
- LA highest blank rates: mobile OS 65.32%, approximate-address flag 40.56%, house number and street name about 14.34%, service date 6.63%, and closed date 4.53%. Most core fields were nearly complete.
- Blanks are counted as missing; literal category values such as `N/A`, `UNKNOWN`, and `Unspecified` were retained as values by this scan. Their meanings should be reviewed before any recoding.

**Benefit:** Missingness profiles show which fields are suitable for broad analysis and prevent blanket deletion or unsafe imputation. Sparse fields can remain in the cleaned data for relevant subsets without being required in the shared schema.

## Time coverage and temporal quality

- NYC `created_date`: 2026-07-22 09:27:15 through 2026-10-05 02:05:08.
- LA `createddate`: 2024-01-01 00:00:06 through 2024-07-24 23:14:35.
- The creation-date windows do not overlap. Direct comparisons of overall request volume or trends would therefore be misleading unless a later extract supplies overlapping periods or the analysis explicitly controls for coverage.
- The selected date fields had zero nonblank parse failures in both extracts.
- NYC `closed_date` minimum (2026-07-08) precedes its `created_date` minimum (2026-07-22); LA service dates extend from 2022-08-30 through 2025-09-03. Dataset-level ranges alone do not establish row-level errors. Check each record for close-before-create and report the count before deriving resolution duration.
- NYC closed date is blank on 10.51% of rows; LA closed date is blank on 4.53%. Keep blank closure times null and do not infer closure solely from status.

**Benefit:** Date ranges expose a major coverage mismatch; row-level chronology checks will prevent negative or invalid resolution durations.

## Status and categorical distributions

- NYC status: Closed 704,789; In Progress 54,731; Open 36,761; Assigned 3,322; Pending 330; Unspecified 53; Started 14.
- LA status: Closed 730,126; Cancelled 34,725; Pending 17,972; Open 16,081; Referred Out 932; Forward 164.
- NYC most frequent complaint types include Illegal Parking (128,241), Noise - Residential (77,800), and Noise - Street/Sidewalk (54,255). The LA top request types include Bulky Items (378,124), Graffiti Removal (177,470), and Metal/Household Appliances (67,616).
- NYC top intake values are ONLINE, PHONE, and MOBILE; LA top request sources are Call, Mobile App, and Self Service. Phone and mobile-app concepts may support a carefully documented channel crosswalk; other values should retain their originals until definitions are checked.
- NYC top agencies are NYPD (372,269), HPD (135,875), and DSNY (72,196); LA top owners are LASAN (593,483), OCB (177,440), and BSL (22,429). Keep original organization codes and categories because the two city taxonomies differ.

**Benefit:** Frequency summaries identify dominant categories and rare workflow values. Preserve original status/category/channel values and add only a cautious broad status grouping; defer a detailed cross-city category mapping until a reviewed crosswalk exists.

## Geographic profile

- NYC latitude spans 40.4989–40.9129 and longitude -74.2549–-73.7004.
- LA latitude spans 33.7061–34.3343 and longitude -118.6678–-118.1558.
- Nonblank latitude/longitude values parsed as numbers in both files. NYC State Plane coordinate fields also parsed numerically.
- NYC borough and community-board fields and LA APC, neighborhood-council, and precinct fields represent different city-specific geographic systems. Do not treat similarly named precinct fields as the same boundary system.

**Benefit:** Numeric ranges help detect grossly malformed coordinates and support maps or spatial aggregation. Keep latitude/longitude as shared coordinates while retaining city-specific geography fields separately.

## Initial processing decisions

1. Keep the source CSVs unchanged. In cleaned Parquet outputs, trim surrounding whitespace and convert blank strings to null while preserving all original columns.
2. Preserve request IDs, ZIP codes, BBL, districts, precincts, grid references, and address components as strings. Parse dates to timestamps and coordinates to numeric types.
3. Add `source_city`, `source_request_id`, and a city-prefixed `record_id` to guarantee unique identity after a future union.
4. Add shared fields for created/closed timestamps, original status and a broad status group, original category, intake-channel label, address text, postal code, latitude/longitude, and location WKT. Keep original city-specific agency, category detail, administrative geography, routing, and resolution fields.
5. Do not drop rows for missing optional values. Do not remove duplicates based on the current scan (none found). Preserve literal `N/A`/`Unknown` categories pending semantic review.
6. Add `resolution_time_hours` only when both timestamps exist and closed time is not before created time; flag invalid chronology and parse failures rather than silently repairing them.
7. Keep NYC and LA creation-time comparisons restricted to an overlapping observation window when one becomes available. Their current windows do not overlap.

The EDA report was generated by `eda_schema.py`. Next, run `clean_datasets.py` and review `reports/cleaning_summary.txt` before using the outputs in the integration phase.
