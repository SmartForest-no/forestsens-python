> **ForestSens 2** This README (and the `v3.4.0` tag) describe the client for the new ForestSens platform,
> which new API users should use. It is in beta: new endpoints and fields may be added at any time, and removing or
> changing anything documented is announced at least 90 days ahead (see "Stability and changelog" in the platform
> documentation, below).
>
> This repository was called `forestsens-api` before. Search results and older links may still use that name, and may show
> old versions (`v1.x`, `v2.x`) that target the old platform; use `v3.4.0` or newer with the new platform.


# forestsens

A Python client for the [ForestSens](https://forestsens.com) API -- a forestry data-processing
service. You upload a dataset (drone imagery, LiDAR point clouds), run it through a processing
pipeline (orthomosaic generation, tree detection, segmentation, wheel-rut assessment, and more),
and retrieve the results.

Using an AI coding agent to write your integration? See [`llms.txt`](llms.txt) -- a terse,
structured reference built for that instead of this narrative README.

The platform's own documentation is served by ForestSens itself at `https://<your host>/api-docs/`: Getting
started, the HTTP API, this Python SDK, use with AI assistants, the OpenAPI reference, and Stability and
changelog. Its `llms.txt` has the platform-side instructions for assistants.

## Before you start

1. **An account.** Sign up in the ForestSens web app (the link is on forestsens.com, below the video). Sign-up
   requests are approved by an operator or by your account's admin, so give it a little time.
2. **An API key** from the web app: **Profile**, then **API keys** (see below).
3. **Your host**: the address the web app is served from, for example
   `mvym2zsszqqwnjdr6ypbzgq7yq.apigateway.eu-frankfurt-1.oci.customer-oci.com` (with or without `https://`).

## Install

```bash
pip install git+https://github.com/SmartForest-no/forestsens-python.git@v3.4.0
```

(Or `@main` for the latest commit on the default branch, if you want to track development rather
than pin a release.)

## Quick start

```python
from forestsens import Client

client = Client()  # reads FORESTSENS_GATEWAY_HOST / FORESTSENS_API_KEY -- see Configuration below

pipeline = client.find_pipeline("Orthophoto (fast)")  # see client.list_pipelines() for the full catalog
paths = client.run(pipeline, "path/to/your/photos", dest_dir="downloads", on_progress=print)
```

That's the whole workflow: upload a file (or a whole folder of them -- non-recursive, top-level
files only), run a pipeline, wait for it, download whatever it produced. `on_progress=print` logs
each status change (`queued` -> `running` -> `complete`) so a long GPU job doesn't look hung.

<details>
<summary>What <code>run()</code> does under the hood, for when you need more control</summary>

```python
from forestsens import BatchFailedError

pipeline = client.find_pipeline("Orthophoto (fast)")

# Upload your data. Large files (>=16 MiB) are chunked and resumed automatically -- same call
# either way, whether you pass one file, a folder, or a list of either.
upload_id = client.upload_files("path/to/your/photos", name="my dataset")

# Start the pipeline and wait for it to finish. "input" is the right slot name for every
# pipeline in the current catalog -- run() assumes it too, by default.
batch = client.create_batch(pipeline.id, [{"slot": "input", "upload_id": upload_id}])
try:
    batch = client.wait_for_batch(batch.id, on_progress=print)
except BatchFailedError as exc:
    print(f"Batch failed: {exc.message}")
    raise

# Download whatever the batch produced.
paths = client.download_artifacts(batch.id, dest_dir="downloads")
```

Use this form instead of `run()` when a pipeline needs more than one input slot, or you want to
inspect or act on the batch mid-flight (cancel it, check intermediate step status, etc.).
</details>

See [`examples/example_batch.py`](examples/example_batch.py) for a full runnable version of both
forms. Three more worked examples -- a whole folder of drone photos, a LiDAR/point-cloud pipeline,
and production-grade progress/error handling -- are indexed in
[`docs/examples.md`](docs/examples.md). The sections below cover how to get a key, configure the
client, and handle errors.

## Getting an API key

An API key (`fs_...`) authenticates every call this client makes. Generate one yourself in the ForestSens web
app: **Profile**, then **API keys**. An account admin can also issue one for a member of the account.

The key is shown **exactly once** at creation -- save it immediately; there is no way to retrieve it later (only its
prefix stays visible). **Keys expire**: by default after 365 days (you can choose 1 to 365 days when you create one;
an expired key answers `401 unauthenticated`, "API key expired"), so create the next one in good time. If a key is
ever lost or compromised, revoke it and issue a new one. Keep it out of files you commit.

## Configuration

Every call needs the **gateway host** (where the API lives) and your **API key**. Pick whichever
of these is convenient -- checked in this order if more than one is set:

```python
# 1. Directly to Client()
from forestsens import Client
client = Client(
    gateway_host="mvym2zsszqqwnjdr6ypbzgq7yq.apigateway.eu-frankfurt-1.oci.customer-oci.com",
    api_key="fs_...",
)
```

```bash
# 2. Environment variables
export FORESTSENS_GATEWAY_HOST=mvym2zsszqqwnjdr6ypbzgq7yq.apigateway.eu-frankfurt-1.oci.customer-oci.com
export FORESTSENS_API_KEY=fs_...
```
```python
client = Client()  # picks up the env vars above
```

```json
// 3. ~/.forestsens/config.json (or pass config_path=... to Client())
{
  "gateway_host": "mvym2zsszqqwnjdr6ypbzgq7yq.apigateway.eu-frankfurt-1.oci.customer-oci.com",
  "api_key": "fs_..."
}
```

## Reference

| Method | Returns | Does |
|---|---|---|
| `list_pipelines(sens=None, include_retired=False, include_drafts=False)` | `list[Pipeline]` | Lists available pipelines (the newest version of each), optionally filtered by sens (`"drone"`, `"point"`, ...); `include_retired=True` adds older versions, `include_drafts=True` your account's drafts. `Pipeline` is a real `dict` (`pipeline["id"]` works) with a friendly `.id`/`.name`/`.description` and a clean repr -- printing one shows just its name, not the full internal graph/id payload. |
| `find_pipeline(name, sens=None)` | `Pipeline` | Looks up a single pipeline by its exact display name (e.g. copied from the web UI) instead of filtering/indexing `list_pipelines()` yourself. Raises `LookupError` (with the real available names) if there's no match, or more than one. |
| `upload_files(paths, name=None, on_progress=None, project=None, private=False, concurrency=4)` | `str` (upload id) | Uploads a file, a folder (its top-level files, not recursive), or a list mixing either, as a single dataset. Files up to 16 MB are sent `concurrency` at a time (4 by default, like the web app; `1` sends them one after another); larger files follow in 16 MB chunks. `on_progress`, if given, is called as `on_progress(filename, loaded_bytes, total_bytes)` while each file uploads (see below). `project` (a `Project`, id or exact name) puts it in a project; `private=True` makes it visible only to you, account admins and members you share it with. |
| `create_batch(pipeline_id, inputs, use_latest_version=False)` | `Batch` | Starts a pipeline run. `inputs` is `[{"slot": str, "upload_id": str}, ...]`. Warns (`ForestSensNewerVersionWarning`) when `pipeline_id` is an older version; `use_latest_version=True` runs the newest one instead. `Batch` is the same friendly-`dict` treatment as `Pipeline` (`.id`/`.status`, `batch["status"]` both work). |
| `get_batch(batch_id)` | `Batch` | Fetches a batch's current status/detail. |
| `list_batches(status=None, upload_id=None, created_by=None, sort="created", direction="desc", limit=20, cursor=None)` | `(list[Batch], next_cursor)` | One page of batches, newest first by default. No pipeline filter -- filter the returned batches' `pipeline_id` yourself if you only want one pipeline's. |
| `wait_for_batch(batch_id, poll_interval=5.0, timeout=None, on_progress=None)` | `Batch` | Polls until the batch reaches `"complete"` or `"failed"`. Raises `BatchFailedError` on failure. `on_progress`, if given, is called once per status change -- pass `on_progress=print` for a one-line log. |
| `run(pipeline, paths, dest_dir, slot="input", name=None, poll_interval=5.0, timeout=None, on_progress=None, on_upload_progress=None, project=None, private=False, use_latest=False)` | `list[str]` (local paths) | The common single-input case in one call: upload -> create a batch -> wait -> download. `pipeline` can be a `Pipeline` or a bare id string. `use_latest=True` follows newer versions of the pipeline. |
| `download_artifacts(batch_id, dest_dir)` | `list[str]` (local paths) | Downloads every artifact the batch produced. |
| `list_projects()` / `create_project(name)` / `rename_project(project, name)` / `delete_project(project)` | `list[Project]` / `Project` / `Project` / `None` | Projects group datasets. Deleting a project keeps its datasets. A `project` argument is a `Project`, an id or an exact name. |
| `list_uploads(project=None, no_project=False, created_by=None, status=None, sort="created", direction="desc", limit=20, cursor=None)` | `(list[Upload], next_cursor)` | One page of the datasets you can see, newest first. `Upload` has `.id`, `.name`, `.visibility`, `.project_name`, `.shared_with_count`, `.files`. |
| `get_upload(upload_id)` | `Upload` | One dataset. |
| `update_upload(upload_id, name=None, project=<unchanged>, private=None)` | `Upload` | Renames a dataset, moves it to a project (`project=None` takes it out of its project) or makes it private (`private=True`) or visible to the account again (`private=False`). Only what you pass changes. |
| `list_members()` | `list[Member]` | The members of your account (`.user_id`, `.email`, `.display_name`). |
| `get_upload_shares(upload_id)` / `set_upload_shares(upload_id, users)` | `list[Member]` | Whom a private dataset is shared with. `set_upload_shares` **replaces** the list: pass everyone who should have access (user ids, e-mail addresses or `Member`s); an empty list removes all shares. |

**Pipeline versions.** A pipeline gets a new version when ForestSens (or your account) changes it. Its **short id**
(`k3x9`, printed by `find_pipeline`) and its **name** always run the newest published version. An older version's
**id** or `k3x9@2` runs exactly that version, so results stay reproducible; the client then issues a
`ForestSensNewerVersionWarning` (a Python warning, not an error). To always follow new versions from a pinned id, pass
`use_latest=True` to `run()` (or `use_latest_version=True` to `create_batch()`). Each batch's `pipeline_id` says which
version ran.

**Upload progress.** Uploads are silent by default. Pass a callback to see them:

```python
client.upload_files("path/to/your/photos", on_progress=lambda name, done, total: print(f"{name}: {done / total:.0%}"))
# or, with run(): client.run(pipeline, "path/to/your/photos", "downloads", on_upload_progress=...)
```

With several files uploading at once (the default), the calls for different files interleave, one call at a time.
It is called with `0` when a file starts, about once per percent while it uploads (once per chunk for large files),
and with `total` when it is done. After a retry, `loaded_bytes` can start over from a lower value.

**Projects and private datasets.**

```python
project = client.create_project("Storm 2026")
upload_id = client.upload_files("path/to/your/photos", project=project, private=True)
client.set_upload_shares(upload_id, ["anna@example.com"])   # share a private dataset with a colleague
```


### Errors

Every call can raise `forestsens.ForestSensAPIError(status, code, message, details)` -- `code` is a machine-readable
string, not just an HTTP status. The ones you will meet:

| Status | `code` | Meaning |
|---|---|---|
| 401 | `unauthenticated` | The key is wrong, revoked or expired. |
| 403 | `forbidden` | Your role may not do this (for example deleting data needs an account admin). |
| 404 | `not_found` | Wrong id, or the dataset is private and not shared with you. |
| 413 | `storage_limit_exceeded` | A dataset or your account would hold more data than allowed. Retrying does not help: delete datasets, split the dataset, or ask for a higher limit. |
| 422 | `validation_error` | The request is wrong, or (on completing an upload) some files did not arrive: `details` names them; upload them and call again. |
| 429 | `rate_limited`, `quota_exceeded` | Too many requests or batches; wait the `Retry-After` seconds. |

The limits and the full error table are in the platform documentation (API page). `forestsens.BatchFailedError` is a
subclass raised specifically by `wait_for_batch` when the batch itself reaches `status="failed"` (as opposed to a
transport/request error) -- it carries the full batch dict (`exc.batch`), including each step's own
`error`/`log_tail`, so you can inspect what went wrong without a second call.

## What this client doesn't cover

It wraps uploading, datasets, projects and sharing, batches and artifact download. API-key management and browsing
typed result tables (detections, segments, tree inventory) are not included yet; reach them directly over HTTP with
your API key (`X-Api-Key` header, against the same host prefixed with `/v1-key/` instead of `/v1/`).

The complete, always-current API reference is public: `https://<your host>/api-docs/reference.html`, and as
machine-readable OpenAPI at `https://<your host>/api-docs/openapi.json`.

## Development

```bash
pip install -e ".[dev]"
pytest -q
```

## License

MIT -- see [LICENSE](LICENSE).
