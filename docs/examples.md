# Examples

Four runnable scripts in [`examples/`](../examples), each covering one real, distinct workflow.
All need a real API key (Profile, then API keys in the web app) -- see the [Readme](../Readme.md)'s "Getting an API key" section.

| Script | Shows | Pipeline used |
|---|---|---|
| [`example_batch.py`](../examples/example_batch.py) | The basic quickstart -- both the one-call `run()` form and the step-by-step calls it wraps, side by side. | `Orthophoto (fast)` (drone) |
| [`example_drone_folder.py`](../examples/example_drone_folder.py) | Uploading a whole folder of photos at once, and a real multi-step pipeline (not just one node). | `Tree species detector` (drone) |
| [`example_point_cloud.py`](../examples/example_point_cloud.py) | The LiDAR side of the catalog -- filtering pipelines by `sens`, a `.las`/`.laz` input instead of imagery. | `SegmentAnyTree` (point) |
| [`example_progress_and_errors.py`](../examples/example_progress_and_errors.py) | A custom `on_progress` logger, and handling `BatchFailedError` (the batch failed) separately from `ForestSensAPIError` (the request itself failed). | `Orthophoto (fast)` (drone) |

## Choosing a pipeline

Every pipeline belongs to one `sens` family, and each family expects a different input format.
This is a hand-maintained snapshot of the current catalog, not derived at runtime -- call
`client.list_pipelines()` (or `list_pipelines(sens=...)`) for the live, authoritative list.

| `sens` | Input format | Example pipelines |
|---|---|---|
| `"drone"` | `.jpg` imagery (some pipelines take one georeferenced GeoTIFF, see below) | `Orthophoto (fast)`, `Tree species detector`, `Forest damage`, `Wheel rut assessment`, `Seedling Detector`, `Canopy cover (DetecTree)` |
| `"point"` | `.las` / `.laz` point clouds | `SegmentAnyTree`, `SegmentAnyTree V2`, `Point2Tree - Semantic segmentation`, `ForAInet` |

Every pipeline in the current catalog uses the slot name `"input"` for its first step -- the
default `run()` and the examples above both assume it, and there's currently no case where you
need to override it.

**The input must match the pipeline.** A wrong input is only discovered after the machine has started, and then fails
with a bare "exit code 1" after several minutes. For example `Canopy cover (DetecTree)` needs a georeferenced
8-bit **RGB** GeoTIFF (three bands; a one-band grayscale or height raster fails). Each pipeline's `instructions`
field (`pipeline["instructions"]`) says what it expects and produces. More in the platform's API page.

## Common recipes

**List only drone pipelines:**
```python
drone_pipelines = client.list_pipelines(sens="drone")
```

**Wait for a batch with a timeout, instead of waiting forever:**
```python
from forestsens import BatchFailedError

try:
    batch = client.wait_for_batch(batch.id, timeout=1800)  # give up after 30 minutes
except TimeoutError:
    print("Still running after 30 minutes -- check back later with client.get_batch(batch.id)")
except BatchFailedError as exc:
    print(f"Failed: {exc.message}")
```

**Handle a typo'd pipeline name cleanly:**
```python
try:
    pipeline = client.find_pipeline("Orthophot (fast)")  # typo
except LookupError as exc:
    print(exc)  # "no pipeline named 'Orthophot (fast)' -- available: Forest damage, Orthophoto (fast), ..."
```

**Re-download a batch's artifacts later, without re-running it:**
```python
paths = client.download_artifacts(existing_batch_id, dest_dir="downloads")
```
