"""
Search for Long-term Health Condition dataflows at SA2 level.

Usage:
    uv run python data_extraction/find_health_dataflows.py
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.explorer import CensusExplorer


def main():
    explorer = CensusExplorer()

    print("Searching for G19 (Long-term Health Conditions) dataflows...")
    print("=" * 70)

    # Search for G19 dataflows
    g19_dfs = explorer.search_dataflows("G19")
    if not g19_dfs.empty:
        print(f"\nFound {len(g19_dfs)} G19 dataflows:")
        print(g19_dfs[["id", "name"]].to_string(index=False))

    print("\n" + "=" * 70)
    print("Searching for 'long-term health' dataflows...")

    health_dfs = explorer.search_dataflows("long-term health")
    if not health_dfs.empty:
        print(f"\nFound {len(health_dfs)} dataflows:")
        print(health_dfs[["id", "name"]].to_string(index=False))

    print("\n" + "=" * 70)
    print("Searching for 'LTHP' dataflows...")

    lthp_dfs = explorer.search_dataflows("LTHP")
    if not lthp_dfs.empty:
        print(f"\nFound {len(lthp_dfs)} dataflows:")
        print(lthp_dfs[["id", "name"]].to_string(index=False))

    print("\n" + "=" * 70)
    print("All 2021 Census SA2 dataflows:")

    sa2_dfs = explorer.find_census_dataflows(year="2021", geography_level="SA2")
    if not sa2_dfs.empty:
        print(f"\nFound {len(sa2_dfs)} SA2 dataflows:")
        for _, row in sa2_dfs.iterrows():
            print(f"  {row['id']}: {row['name']}")


if __name__ == "__main__":
    main()
