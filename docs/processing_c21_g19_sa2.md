# Processing C21_G19_SA2 (Long-term Health Conditions by Age by Sex, SA2+)

This guide explains how to transform the raw ABS Census 2021 G19 dataset into tidy Parquet outputs using the project processing utilities.

## Prerequisites

- Raw Parquet file from the extractor at `data/raw/c21_g19_sa2_health_conditions.parquet` (see `data_extraction/extract_c21_g19_sa2.py`).
- Environment set up with project dependencies (`uv sync`).

## Steps

1. **Ensure raw data exists**

   - Run the extractor if needed:
     ```bash
     uv run python data_extraction/extract_c21_g19_sa2.py
     ```
   - Confirm the raw file is present at `data/raw/c21_g19_sa2_health_conditions.parquet`.

2. **Run the processor**

   ```python
   from processing import process_c21_g19_sa2

   outputs = process_c21_g19_sa2()  # uses default raw path
   ```

   - To use a custom raw file or skip writing outputs:
     ```python
     outputs = process_c21_g19_sa2(
         input_path="/path/to/raw.parquet",
         write_output=False,
     )
     ```
     - To keep lookups consolidated with the C21_G01 base files (default):
       ```python
       outputs = process_c21_g19_sa2(reuse_base_lookups=True)
       ```
     - To write separate G19-specific lookup files:
       ```python
       outputs = process_c21_g19_sa2(reuse_base_lookups=False)
       ```

- To merge health-condition codes into a shared cross-dataset lookup (default):
  ```python
  outputs = process_c21_g19_sa2(merge_health_lookup=True)
  ```

3. **Outputs (written to `data/processed/` by default)**

   - Always written:
     - `c21_g19_sa2_health_conditions.parquet` — fact table
     - `c21_g19_sa2_health_condition_lookup.parquet`
   - Shared lookups (default behavior):
     - Sex/age/geog-type/state/geography codes are merged into the C21_G01 lookup files:
       - `c21_g01_sa2_geo_lookup.parquet`
       - `c21_g01_sa2_sex_lookup.parquet`
       - `c21_g01_sa2_age_lookup.parquet`
       - `c21_g01_sa2_geog_type_lookup.parquet`
       - `c21_g01_sa2_state_lookup.parquet`
     - Health conditions are merged into a cross-dataset lookup:
       - `common_health_condition_lookup.parquet`
   - If `reuse_base_lookups=False`, G19-specific lookup files are written instead for geo/sex/age/geog_type/state:
     - `c21_g19_sa2_geo_lookup.parquet`
     - `c21_g19_sa2_sex_lookup.parquet`
     - `c21_g19_sa2_age_lookup.parquet`
     - `c21_g19_sa2_geog_type_lookup.parquet`
     - `c21_g19_sa2_state_lookup.parquet`

4. **Fact table schema** (`c21_g19_sa2_health_conditions.parquet`)

   - `sex` (string): code (1,2,3)
   - `lthc` (string): long-term health condition code
   - `age_group` (string): e.g., `25_34`, `65_74`
   - `geog_id` (string): geography code
   - `geog_type` (string): geography level (SA2/SA3/SA4/LGA/STE)
   - `state` (string): state code
   - `year` (Int64): year (e.g., 2021)
   - `persons` (Int64): observed count

5. **Lookup tables**
   - Health condition lookup maps `lthc` to `health_condition` (labels from the source).
   - Geography lookup includes `geog_id`, `geog_name`, `geog_type`, `state`.
   - Other lookups map codes to labels (`sex`, `age_group`, `geog_type`, `state`).

## Notes and rationale

- Values of the form `CODE: Label` are split; only the code is stored in the fact table, with labels preserved in lookup tables.
- Parquet is used for both fact and lookup tables to keep the pipeline file-based and columnar; no database is required.
