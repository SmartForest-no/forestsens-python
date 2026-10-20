"""End-to-end example: upload a file, discover a pipeline, run a batch,
wait for it, download whatever artifacts it produces.

Requires a real API key -- generate one in the ForestSens web app (Profile, then API keys)
(see Readme.md's "Getting an API key" section), then either export
FORESTSENS_GATEWAY_HOST/FORESTSENS_API_KEY or write them to
~/.forestsens/config.json (see Readme.md).
"""

from forestsens import BatchFailedError, Client

INPUT_PATH = "path/to/your/file.tif"  # a real local file, or a folder of them
PIPELINE_NAME = "Orthophoto (fast)"  # see client.list_pipelines() for the full catalog

client = Client()

# --- The step-by-step version -- what run() below does under the hood, useful when you
# need more than one input slot or want to inspect/act on the batch mid-flight (e.g. cancel it).

pipeline = client.find_pipeline(PIPELINE_NAME)
print(f"Using pipeline: {pipeline}")  # Pipeline's friendly repr -- just id/name, not the full graph

print(f"\nUploading {INPUT_PATH}...")
upload_id = client.upload_files(INPUT_PATH)
print(f"Upload complete: {upload_id}")

print(f"\nStarting batch against {pipeline}...")
batch = client.create_batch(pipeline.id, [{"slot": "input", "upload_id": upload_id}])
print(f"Batch created: {batch}")  # Batch's friendly repr -- id + status

print("\nWaiting for batch to finish...")
try:
    batch = client.wait_for_batch(batch.id, poll_interval=5, on_progress=print)
except BatchFailedError as exc:
    print(f"Batch failed: {exc.message}")
    for step in exc.batch.get("steps") or []:
        if step.get("error"):
            print(f"  step {step['node_id']}: {step['error']}")
    raise SystemExit(1)

print("Batch done. Downloading artifacts...")
paths = client.download_artifacts(batch.id, dest_dir="downloads")
for path in paths:
    print(f"  {path}")

# --- The one-call shortcut -- the whole block above, in one line:
#
#   pipeline = client.find_pipeline(PIPELINE_NAME)
#   paths = client.run(pipeline, INPUT_PATH, dest_dir="downloads", on_progress=print)
