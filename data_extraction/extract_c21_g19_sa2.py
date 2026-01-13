"""
Extract C21_G19_SA2 - Long-term Health Conditions by Age by Sex at SA2 level.

This matches the Health section data from QuickStats showing conditions like:
- Arthritis, Asthma, Cancer, Dementia, Diabetes, Heart disease
- Kidney disease, Lung condition, Mental health condition, Stroke
- Any other long-term condition, No long-term condition

Usage:
    uv run python data_extraction/extract_c21_g19_sa2.py
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.explorer import CensusExplorer
from src.data_extractor import DataExtractor
from src.api_client import ABSClient


def main():
    client = ABSClient()
    explorer = CensusExplorer(client=client)
    extractor = DataExtractor(client=client)

    dataflow_id = "C21_G19_SA2"

    print("=" * 70)
    print(f"Extracting: {dataflow_id}")
    print("Census 2021 - Type of long-term health condition by age by sex (SA2)")
    print("=" * 70)

    # Step 1: Get structure
    print("\n[Step 1] Getting dataflow structure...")
    details = explorer.get_dataflow_details(dataflow_id)

    print("\nDimensions:")
    print(details["dimensions"].to_string(index=False))

    # Step 2: Show available codelists
    print("\n[Step 2] Available codelists (non-geographic):")
    for codelist_id, codes_df in details["codelists"].items():
        if len(codes_df) < 50:
            print(f"\n  {codelist_id} ({len(codes_df)} codes):")
            print(codes_df.to_string(index=False))

    # Step 3: Fetch sample data to see structure
    print("\n[Step 3] Fetching sample data...")
    sample_df = client.get_data(
        dataflow_id=dataflow_id,
        data_key="all",
        start_period="2021",
        end_period="2021",
        response_format="csv_labels",
    )

    print(f"\nData shape: {sample_df.shape}")
    print(f"Columns: {list(sample_df.columns)}")
    print("\nFirst 10 rows:")
    print(sample_df.head(10))

    # Show unique values in key columns
    print("\n[Step 4] Unique values in key columns:")

    # Health condition column (LTHP)
    lthp_col = [c for c in sample_df.columns if "LTHP" in c or "health" in c.lower()]
    if lthp_col:
        print(f"\nHealth conditions ({lthp_col[0]}):")
        for val in sample_df[lthp_col[0]].unique():
            print(f"  - {val}")

    # Region types
    region_type_col = [c for c in sample_df.columns if "REGION_TYPE" in c]
    if region_type_col:
        print(f"\nRegion types ({region_type_col[0]}):")
        for val in sample_df[region_type_col[0]].unique():
            print(f"  - {val}")

    # Step 5: Save all data
    print("\n[Step 5] Saving full dataset to Parquet...")
    extractor.extract_dataflow(
        dataflow_id=dataflow_id,
        start_period="2021",
        end_period="2021",
        output_name="c21_g19_sa2_health_conditions",
        save_raw=True,
    )

    print("\n" + "=" * 70)
    print("Done!")
    print(f"Total rows: {len(sample_df)}")
    print("File: data/raw/c21_g19_sa2_health_conditions.parquet")
    print("=" * 70)

    # Usage examples
    print("\n--- Example Usage ---")
    print("""
# Load the data
from src.data_extractor import DataExtractor
extractor = DataExtractor()
df = extractor.load_parquet("c21_g19_sa2_health_conditions")

# Filter to SA2 level only
sa2_only = df[df["REGION_TYPE: Region Type"] == "SA2: Statistical Area Level 2"]

# Get mental health conditions for a specific SA2
mental_health = sa2_only[
    sa2_only["LTHP: Type of long-term health condition"].str.contains("Mental health")
]

# Get all health conditions for persons (total) in a region
region_data = sa2_only[
    (sa2_only["REGION: Region"].str.contains("Ipswich")) &
    (sa2_only["SEXP: Sex"] == "3: Persons")
]
""")


if __name__ == "__main__":
    main()
