"""
Example: Extract C21_G18_SA2 - Core activity need for assistance by age by sex

This is SA2-level health/disability data from the 2021 Census.

Usage:
    uv run python data_extraction/extract_c21_g18_sa2.py
"""

import sys
from pathlib import Path

# Add project root to path when running directly
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.explorer import CensusExplorer
from src.data_extractor import DataExtractor
from src.api_client import ABSClient


def main():
    client = ABSClient()
    explorer = CensusExplorer(client=client)
    extractor = DataExtractor(client=client)

    dataflow_id = "C21_G18_SA2"

    print("=" * 70)
    print(f"Exploring: {dataflow_id}")
    print("Census 2021 - Core activity need for assistance by age by sex (SA2)")
    print("=" * 70)

    # Step 1: Get structure
    print("\n[Step 1] Getting dataflow structure...")
    explorer.summarize_dataflow(dataflow_id)

    details = explorer.get_dataflow_details(dataflow_id)
    print("\nDimensions:")
    print(details["dimensions"].to_string(index=False))

    # Step 2: Show available codelists
    print("\n[Step 2] Available codelists:")
    for codelist_id, codes_df in details["codelists"].items():
        # Only show relevant codelists (not huge geographic ones)
        if len(codes_df) < 50:
            print(f"\n  {codelist_id} ({len(codes_df)} codes):")
            print(codes_df.to_string(index=False))

    # Step 3: Get SA2 region codes
    print("\n[Step 3] Getting SA2 region codes...")
    geo_codes = explorer.list_geography_codes(dataflow_id)
    if not geo_codes.empty:
        print(f"Found {len(geo_codes)} SA2 regions")
        print("\nFirst 20 SA2 regions:")
        print(geo_codes.head(20).to_string(index=False))

        # Save full list for reference
        geo_codes.to_csv("data/sa2_region_codes.csv", index=False)
        print("\nFull SA2 code list saved to: data/sa2_region_codes.csv")

    # Step 4: Fetch sample data
    print("\n[Step 4] Fetching sample data...")
    sample_df = client.get_data(
        dataflow_id=dataflow_id,
        data_key="all",
        start_period="2021",
        end_period="2021",
        response_format="csv_labels",
    )

    print(f"\nData shape: {sample_df.shape}")
    print(f"Columns: {list(sample_df.columns)}")
    print("\nFirst few rows:")
    print(sample_df.head(10))

    # Step 5: Save all data
    print("\n[Step 5] Saving full dataset to Parquet...")
    extractor.extract_dataflow(
        dataflow_id=dataflow_id,
        start_period="2021",
        end_period="2021",
        output_name="c21_g18_sa2_all",
        save_raw=True,
    )

    print("\n" + "=" * 70)
    print("Done!")
    print(f"Total rows: {len(sample_df)}")
    print("File: data/raw/c21_g18_sa2_all.parquet")
    print("=" * 70)

    # Example: How to filter for specific SA2s later
    print("\n--- Example: Filtering for specific SA2 regions ---")
    print("""
# To extract only specific SA2 regions:
selected_sa2s = ["101021007", "101021008", "101021009"]  # Example codes

df = extractor.extract_multiple_regions(
    dataflow_id="C21_G18_SA2",
    region_codes=selected_sa2s,
    region_dimension="REGION",
    start_period="2021",
    end_period="2021",
    output_name="c21_g18_selected_sa2s",
)

# Or reload and filter the full dataset:
df = extractor.load_parquet("c21_g18_sa2_all")
filtered = df[df["REGION: Region"].str.contains("Sydney")]
""")


if __name__ == "__main__":
    main()
