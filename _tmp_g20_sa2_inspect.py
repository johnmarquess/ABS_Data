from src.api_client import ABSClient
from src.explorer import CensusExplorer

client = ABSClient()
explorer = CensusExplorer(client=client)

print("[1] Search G20 dataflows (expect SA2 variants)...")
g20 = explorer.search_dataflows("G20")
print(g20[["id", "name"]].head(20))

sa2_rows = g20[g20["id"].str.contains("SA2", na=False)]
if sa2_rows.empty:
    print("\nNo SA2 G20 dataflow found in search results.")
    raise SystemExit(0)

dataflow_id = sa2_rows.iloc[0]["id"]
print(f"\n[2] Inspecting dataflow: {dataflow_id}")

structure = explorer.get_dataflow_details(dataflow_id)
print("\nDimensions:")
print(structure["dimensions"].to_string(index=False))

print("\nCodelists (<=50 codes shown):")
for cid, df in structure["codelists"].items():
    if len(df) <= 50:
        print(f"\n{cid} ({len(df)} codes):")
        print(df.to_string(index=False))

print("\n[3] Sample data (labels)")
sample = client.get_data(
    dataflow_id=dataflow_id,
    data_key="all",
    start_period="2021",
    end_period="2021",
    response_format="csv_labels",
)
print(sample.head(10))
print("\nColumns:", list(sample.columns))
print("\nShape:", sample.shape)
