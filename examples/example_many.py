"""Run one pipeline on many datasets, a few at a time, with run_many().

The common case: a folder with one sub-folder per drone flight (or per point cloud), and the same pipeline for each.
run_many() uploads and starts at most `max_active` of them at once and starts the next one whenever one finishes, so
the platform's limit of 20 queued or running batches per user is never hit, and the machines start in a steady
stream instead of all at once. Don't write a loop of create_batch() for this: it sends everything immediately and is
refused after 20.

Requires a real API key -- generate one in the ForestSens web app (Profile, then API keys) and either export
FORESTSENS_GATEWAY_HOST/FORESTSENS_API_KEY or write them to ~/.forestsens/config.json (see Readme.md).

Usage: python example_many.py <folder with one sub-folder per dataset> [pipeline name]
"""
import sys
from pathlib import Path

from forestsens import Client

FOLDER = Path(sys.argv[1] if len(sys.argv) > 1 else "flights")
PIPELINE_NAME = sys.argv[2] if len(sys.argv) > 2 else "Orthophoto (fast)"  # see client.list_pipelines()

client = Client()
pipeline = client.find_pipeline(PIPELINE_NAME)
print(f"Using pipeline: {pipeline}")

# One job per sub-folder: its files become one dataset, named after the folder.
jobs = [
    {"pipeline": pipeline, "paths": folder, "name": folder.name}
    for folder in sorted(p for p in FOLDER.iterdir() if p.is_dir())
]
print(f"{len(jobs)} datasets in {FOLDER}")


def show(summary):
    print(
        f"\r{summary['done']}/{summary['total']} done, {summary['failed']} failed, "
        f"{summary['running']} running, {summary['waiting']} waiting",
        end="",
        flush=True,
    )


# Results of each finished batch go to results/<sub-folder name>/.
results = client.run_many(jobs, max_active=3, dest_dir="results", on_progress=show)
print()

for result in results:
    if result.status == "complete":
        print(f"  {result.job['name']}: {len(result.paths)} file(s) in results/{result.job['name']}/")
    else:
        # Give the batch id to the ForestSens team if you need help with a failure.
        print(f"  {result.job['name']}: {result.status} (batch {result.batch_id}): {result.error}")

# --- The same for datasets that are already in ForestSens (no upload): ---------------------------------------------
#
# uploads, _ = client.list_uploads(status="ready", limit=50)
# jobs = [{"pipeline": pipeline, "upload_id": u.id, "name": u.name} for u in uploads]
# results = client.run_many(jobs, max_active=3, on_progress=show)
