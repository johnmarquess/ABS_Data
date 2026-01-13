"""
Example: Extract C21_G19_CED Census data for multiple regions.

This script demonstrates:
1. Exploring a dataflow's structure
2. Finding available region codes
3. Extracting data for selected regions
4. Saving to Parquet

Usage:
    uv run python data_extraction/extract_c21_g19.py
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
    # Initialize our tools
    client = ABSClient()
    explorer = CensusExplorer(client=client)
    extractor = DataExtractor(client=client)

    dataflow_id = "C21_G19_CED"

    print("=" * 70)
    print(f"Exploring dataflow: {dataflow_id}")
    print("=" * 70)

    # Step 1: Get and display the dataflow structure
    print("\n[Step 1] Getting dataflow structure...")
    explorer.summarize_dataflow(dataflow_id)

    # Step 2: Get the detailed structure to find the region dimension
    print("\n[Step 2] Getting dimension details...")
    details = explorer.get_dataflow_details(dataflow_id)

    print("\nDimensions:")
    print(details["dimensions"].to_string(index=False))

    # Step 3: Find the geography/region dimension and list some codes
    print("\n[Step 3] Listing available region codes...")
    geo_codes = explorer.list_geography_codes(dataflow_id)

    if not geo_codes.empty:
        print(f"\nFound {len(geo_codes)} region codes. First 20:")
        print(geo_codes.head(20).to_string(index=False))

        # Save the full list of region codes for reference
        geo_codes.to_csv("data/ced_region_codes.csv", index=False)
        print("\nFull region code list saved to: data/ced_region_codes.csv")

    # Step 4: Extract data for a few example regions
    # NOTE: Adjust these codes based on what's available in the codelist
    print("\n[Step 4] Extracting sample data...")

    # First, let's get a small sample to understand the data
    print("\nFetching sample data (first 5 observations)...")
    sample_df = client.get_data(
        dataflow_id=dataflow_id,
        data_key="all",
        start_period="2021",
        end_period="2021",
        response_format="csv_labels",
    )

    print(f"\nSample data shape: {sample_df.shape}")
    print(f"Columns: {list(sample_df.columns)}")
    print("\nFirst few rows:")
    print(sample_df.head())

    # Step 5: Save the full dataset (or a filtered subset) to Parquet
    print("\n[Step 5] Saving to Parquet...")

    # Option A: Save all data for this dataflow
    extractor.extract_dataflow(
        dataflow_id=dataflow_id,
        start_period="2021",
        end_period="2021",
        output_name="c21_g19_ced_all",
        save_raw=True,
    )

    # Option B: If you want specific regions only, you would do:
    # (Uncomment and modify region codes as needed)
    #
    # selected_regions = ["101", "102", "103"]  # Example CED codes
    # df = extractor.extract_multiple_regions(
    #     dataflow_id=dataflow_id,
    #     region_codes=selected_regions,
    #     region_dimension="CED",  # Adjust based on actual dimension name
    #     start_period="2021",
    #     end_period="2021",
    #     output_name="c21_g19_ced_selected",
    # )

    print("\n" + "=" * 70)
    print("Done! Check the data/raw/ folder for the Parquet file.")
    print("=" * 70)

    # Show how to reload the data
    print("\nTo reload this data later:")
    print('  df = extractor.load_parquet("c21_g19_ced_all")')


if __name__ == "__main__":
    main()
