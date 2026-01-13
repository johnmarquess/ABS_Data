"""
ABS Census Data Exploration Script

Run this script to discover available Census dataflows and explore their structures.
This is the entry point for understanding what data is available from the ABS API.

Usage:
    uv run python main.py
"""

from src.explorer import CensusExplorer
from src.config import Settings


def main():
    """Main exploration entry point."""
    print("=" * 70)
    print("ABS Census Data Explorer")
    print("=" * 70)
    print()

    explorer = CensusExplorer()

    # List available topics we can search for
    print("Available topic categories to search:")
    print("-" * 40)
    for topic in explorer.get_available_topics():
        print(f"  - {topic}")
    print()

    # Find Census dataflows
    print("Searching for Census dataflows...")
    print("-" * 40)

    # Check all Census years
    for year in ["2021", "2016", "2011", "2006"]:
        census_df = explorer.find_census_dataflows(year=year)
        print(f"\n{year} Census dataflows found: {len(census_df)}")
        if not census_df.empty and len(census_df) <= 10:
            print(census_df[["id", "name"]].to_string(index=False))
        elif not census_df.empty:
            print(census_df[["id", "name"]].head(5).to_string(index=False))
            print(f"  ... and {len(census_df) - 5} more")

    # Look for SA2-level data specifically (2021)
    print("\n" + "-" * 40)
    print("SA2-level 2021 Census dataflows:")
    sa2_dataflows = explorer.find_census_dataflows(year="2021", geography_level="SA2")
    if not sa2_dataflows.empty:
        print(sa2_dataflows[["id", "name"]].head(20).to_string(index=False))
    else:
        print("  No SA2-specific dataflows found. Try exploring by topic instead.")

    # Search for population data
    print("\n" + "-" * 40)
    print("Population-related dataflows:")
    population_dfs = explorer.find_dataflows_by_topic("population")
    if not population_dfs.empty:
        # Filter for Census only
        census_pop = population_dfs[
            population_dfs["id"].str.match(r"^C\d{2}", na=False)
        ]
        if not census_pop.empty:
            print(census_pop[["id", "name"]].head(10).to_string(index=False))

    # Search for health data
    print("\n" + "-" * 40)
    print("Health-related dataflows:")
    health_dfs = explorer.find_dataflows_by_topic("health")
    if not health_dfs.empty:
        census_health = health_dfs[health_dfs["id"].str.match(r"^C\d{2}", na=False)]
        if not census_health.empty:
            print(census_health[["id", "name"]].head(10).to_string(index=False))

    # Search for indigenous data
    print("\n" + "-" * 40)
    print("Indigenous-related dataflows:")
    indigenous_dfs = explorer.find_dataflows_by_topic("indigenous")
    if not indigenous_dfs.empty:
        census_indig = indigenous_dfs[
            indigenous_dfs["id"].str.match(r"^C\d{2}", na=False)
        ]
        if not census_indig.empty:
            print(census_indig[["id", "name"]].head(10).to_string(index=False))

    print("\n" + "=" * 70)
    print("Exploration complete!")
    print()
    print("Next steps:")
    print("1. Pick a dataflow ID from above")
    print("2. Use explorer.summarize_dataflow('DATAFLOW_ID') to see its structure")
    print("3. Use explorer.list_geography_codes('DATAFLOW_ID') to see available regions")
    print("=" * 70)


if __name__ == "__main__":
    main()
