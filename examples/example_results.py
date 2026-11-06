"""Read a finished batch's results as data: tree crowns, detections, wheel ruts or a tree inventory.

Requires a real API key (see Readme.md: "Getting an API key") in FORESTSENS_GATEWAY_HOST/FORESTSENS_API_KEY
or ~/.forestsens/config.json. Usage: python example_results.py <batch_id>
"""
import sys

from forestsens import Client

client = Client()
batch = client.get_batch(sys.argv[1])
print(batch)

# Which result tables does this batch have, and how many rows?
for table, summary in (batch.get("results") or {}).items():
    if isinstance(summary, dict) and summary.get("count"):
        print(f"  {table}: {summary['count']} rows")

# All tree crowns of the batch (all pages), then only the confident ones.
crowns = client.get_results(batch.id, "segments")
print(f"{len(crowns)} segments, for example {crowns[0] if crowns else '-'}")
confident = client.get_results(batch.id, "segments", confidence_min=0.5)
print(f"{len(confident)} with confidence >= 0.5")

# Save every table the batch has as GeoJSON (opens in QGIS).
for path in client.save_results(batch.id, "results"):
    print("wrote", path)
