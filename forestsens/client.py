"""The ForestSens API client -- one class, deliberately kept flat and
simple (v1 scope is upload/batch/download only, see the repo's own
Readme.md). Mirrors two things from the real frontend, which is the
verified reference implementation for how this API actually behaves in
practice (not just its OpenAPI shape): the request/error envelope
handling in forestsens/src/api/client.ts, and the upload orchestration
in forestsens/src/lib/uploadFiles.ts (small-file PUT-to-PAR vs
large-file backend-mediated chunked multipart, resume-aware).
"""

from __future__ import annotations

import json
import mimetypes
import os
import re
import threading
import time
import warnings
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any, Callable, Iterator

import requests

from .errors import BatchFailedError, ForestSensAPIError, ForestSensNewerVersionWarning

# Must match forestsens/services/uploads.py's MULTIPART_CHUNK_SIZE_BYTES --
# kept in sync by hand (same as the frontend does), not derived at
# runtime. 16 MiB: real margin under the API Gateway's confirmed 20MB
# request-body ceiling, which every multipart chunk has to pass through
# since pre-authenticated requests don't support multipart at all.
MULTIPART_CHUNK_SIZE_BYTES = 16 * 1024 * 1024

MAX_ATTEMPTS = 3
# Files uploaded at the same time by upload_files(), the same number the web app uses (frontend MAX_CONCURRENT_UPLOADS).
DEFAULT_UPLOAD_CONCURRENCY = 4
# run_many(): batches kept in flight by default, and the server's own cap (20 queued or running per user).
DEFAULT_MAX_ACTIVE = 4
MAX_ACTIVE_LIMIT = 20
# Waiting for a free batch slot when the server sends no Retry-After.
DEFAULT_SLOT_WAIT_SECONDS = 60.0
DEFAULT_CONFIG_PATH = Path.home() / ".forestsens" / "config.json"


def _load_config_file(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    with path.open() as f:
        return json.load(f)


class Pipeline(dict):
    """list_pipelines()/find_pipeline() wrap each raw pipeline dict in this so printing/repr-ing
    one (or a list of them) shows just id/name -- not the internal graph/account_id/sens_id/
    created_at noise a researcher never needs to pick a pipeline by name -- while staying a real
    dict throughout: pipeline["id"], **pipeline, json.dumps(pipeline), and every existing
    dict-based caller keep working unchanged. Attribute access (.id/.name/.description) is added
    on top as a nicer alternative to bracket indexing, not a replacement.
    """

    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    def __repr__(self) -> str:
        version = self.get("version") or 1
        suffix = f", version={version!r}" if version > 1 else ""
        return f"Pipeline(short_id={self.short_id!r}, name={self.get('name')!r}{suffix})"

    def __str__(self) -> str:
        return f"{self.get('name') or '(unnamed)'} [{self.short_id}]"

    @property
    def short_id(self) -> str:
        return self.get("short_id") or str(self.get("id"))[:8]


class Batch(dict):
    """Same treatment as Pipeline, for batches -- create_batch/get_batch/wait_for_batch return
    this instead of a bare dict. Still a real dict throughout (batch["status"], json.dumps(batch),
    ** unpacking all keep working); adds .id/.status attribute access and a repr/str that shows
    what actually matters at a glance instead of the full steps/results dump.
    """

    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    def __repr__(self) -> str:
        return f"Batch(id={self.get('id')!r}, status={self.get('status')!r})"

    def __str__(self) -> str:
        return f"{self.get('id')} ({self.get('status')})"


class RunResult(dict):
    """One job of run_many(): `.index`, `.job` (the dict you passed), `.batch_id`, `.status` ("complete", "failed",
    "cancelled" or "error" when it could not be uploaded or started), `.error` (a message), `.batch` (last seen),
    `.paths` (downloaded files, with dest_dir). A real dict, with attribute access."""

    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    def __repr__(self) -> str:
        label = self.get("job", {}).get("name") or self.get("batch_id") or f"job {self.get('index')}"
        return f"RunResult({label!r}, status={self.get('status')!r})"


class _View(dict):
    """Base for the friendly dict views below: a real dict throughout (item access, ** unpacking,
    json.dumps) with attribute access (.id/.name) added on top."""

    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc


class Project(_View):
    """A project groups datasets. `.id`, `.name`, `.dataset_count`, `.can_manage`."""

    def __repr__(self) -> str:
        return f"Project(id={self.get('id')!r}, name={self.get('name')!r})"

    def __str__(self) -> str:
        return f"{self.get('name')} ({self.get('dataset_count', 0)} datasets)"


class Upload(_View):
    """A dataset. `.id`, `.name`, `.status`, `.visibility` ("account" or "private"), `.project_id`,
    `.project_name`, `.shared_with_count`, `.files`, `.can_manage`."""

    def __repr__(self) -> str:
        return f"Upload(id={self.get('id')!r}, name={self.get('name')!r}, visibility={self.get('visibility')!r})"

    def __str__(self) -> str:
        return f"{self.get('name') or self.get('id')} [{self.get('visibility')}]"


class Member(_View):
    """A member of your account: `.user_id`, `.email`, `.display_name` (and `.is_me` from
    list_members())."""

    def __repr__(self) -> str:
        return f"Member(user_id={self.get('user_id')!r}, email={self.get('email')!r})"

    def __str__(self) -> str:
        return f"{self.get('display_name') or self.get('email')} <{self.get('email')}>"


class _ResultRow(_View):
    """One row of a typed result table: a real dict (row["class"], json.dumps(row)) with attribute access; `.class_`
    because `class` is a Python keyword. `.geom` is the GeoJSON geometry (WGS84)."""

    @property
    def class_(self) -> Any:
        return self.get("class")

    def __repr__(self) -> str:
        parts = [f"id={self.get('id')!r}"]
        for key in self._REPR_KEYS:
            if self.get(key) is not None:
                parts.append(f"{key}={self.get(key)!r}")
        return f"{type(self).__name__}({', '.join(parts)})"

    _REPR_KEYS: tuple[str, ...] = ("class", "confidence")


class Detection(_ResultRow):
    """A detected object (a point or box): `.class_`, `.confidence`, `.geom`, `.box_width`, `.box_height`,
    `.attributes`, `.step_id`."""


class Segment(_ResultRow):
    """A segmented outline, for example a tree crown: `.class_`, `.confidence`, `.geom` (a polygon), `.area_m2`,
    `.attributes`, `.step_id`."""

    _REPR_KEYS = ("class", "confidence", "area_m2")


class LinearFeature(_ResultRow):
    """A line, for example a wheel rut: `.class_`, `.confidence`, `.geom`, `.measurement` with
    `.measurement_unit`, `.attributes`, `.step_id`."""

    _REPR_KEYS = ("class", "measurement", "measurement_unit")


class TreeRecord(_ResultRow):
    """One tree of a tree inventory: `.species`, `.dbh_cm`, `.height_m`, `.volume_m3`, `.geom` (may be None),
    `.attributes`, `.step_id`."""

    _REPR_KEYS = ("species", "dbh_cm", "height_m")


# table name -> (route, row view). The names are the keys of a batch's `results`.
RESULT_TABLES: dict[str, tuple[str, type]] = {
    "detections": ("detections", Detection),
    "segments": ("segments", Segment),
    "linear_features": ("linear-features", LinearFeature),
    "tree_inventory": ("tree-inventory", TreeRecord),
}
RESULT_PAGE_SIZE = 1000  # the API's maximum for the result tables
RATE_LIMIT_ATTEMPTS = 5


def _retry_after(resp: Any) -> float | None:
    """The `Retry-After` seconds of a response, or None (missing, not a number, or a test double)."""
    try:
        value = resp.headers.get("Retry-After")
        return float(value) if value is not None else None
    except (AttributeError, TypeError, ValueError):
        return None


_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_KEEP: Any = object()  # "leave unchanged" marker for update_upload(project=...)


class _ProgressFile:
    """File-like wrapper that reports how many bytes have been read (i.e. sent) while a file is
    uploaded in one request. `requests` streams it using `__len__` for the Content-Length.
    Reports at most about once per percent, so a callback that prints does not flood the terminal."""

    def __init__(self, fileobj: Any, size: int, report: Callable[[int], None]) -> None:
        self._f = fileobj
        self._size = size
        self._report = report
        self._sent = 0
        self._last = 0
        self._step = max(size // 100, 256 * 1024)

    def __len__(self) -> int:
        return self._size

    def read(self, n: int = -1) -> bytes:
        data = self._f.read(n)
        self._sent += len(data)
        if self._sent - self._last >= self._step or self._sent >= self._size:
            self._last = self._sent
            self._report(min(self._sent, self._size))
        return data


class Client:
    def __init__(
        self,
        gateway_host: str | None = None,
        api_key: str | None = None,
        config_path: str | os.PathLike[str] | None = None,
    ) -> None:
        """Config priority: explicit args > environment variables
        (FORESTSENS_GATEWAY_HOST / FORESTSENS_API_KEY) > config file
        (~/.forestsens/config.json, or config_path if given) --
        {"gateway_host": "...", "api_key": "..."}. An API key can only be
        minted in the web UI (Profile, then API keys; there's no
        bootstrap path that starts from nothing but a key) -- this
        client only ever consumes an existing one.
        """
        file_config = _load_config_file(Path(config_path) if config_path else DEFAULT_CONFIG_PATH)

        self.gateway_host = (
            gateway_host
            or os.environ.get("FORESTSENS_GATEWAY_HOST")
            or file_config.get("gateway_host")
        )
        self.api_key = (
            api_key or os.environ.get("FORESTSENS_API_KEY") or file_config.get("api_key")
        )
        if not self.gateway_host or not self.api_key:
            raise ValueError(
                "gateway_host and api_key must be provided directly, via "
                "FORESTSENS_GATEWAY_HOST/FORESTSENS_API_KEY, or in "
                f"{config_path or DEFAULT_CONFIG_PATH}"
            )
        # Accept what people naturally paste: "https://host/" as well as "host".
        host = self.gateway_host.strip()
        for scheme in ("https://", "http://"):
            if host.lower().startswith(scheme):
                host = host[len(scheme):]
        self.gateway_host = host.rstrip("/")
        self._session = requests.Session()

    # -- low-level request core -------------------------------------------------

    def _url(self, path: str) -> str:
        # Every real API call (as opposed to a pre-authenticated Object
        # Storage URL) goes through /v1-key/*, the one Gateway route that
        # doesn't require a browser JWT -- see infra/api_gateway.tf.
        # Callers of this client only ever pass the plain /v1/... path;
        # this is the one place that prefix gets rewritten.
        assert path.startswith("/v1/"), f"expected a /v1/... path, got {path!r}"
        return f"https://{self.gateway_host}/v1-key{path[len('/v1'):]}"

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = kwargs.pop("headers", {})
        headers["X-Api-Key"] = self.api_key
        resp = self._session.request(method, self._url(path), headers=headers, **kwargs)
        try:
            body = resp.json()
        except ValueError:
            body = None
        error = body.get("error") if isinstance(body, dict) else None
        if not resp.ok or error:
            retry_after = _retry_after(resp)
            if error:
                raise ForestSensAPIError(
                    resp.status_code, error["code"], error["message"], error.get("details"), retry_after=retry_after
                )
            raise ForestSensAPIError(resp.status_code, "unknown_error", f"HTTP {resp.status_code}", retry_after=retry_after)
        return body["data"] if isinstance(body, dict) and "data" in body else body

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("GET", path, params=params)

    def _post(self, path: str, json_body: Any = None) -> Any:
        return self._request("POST", path, json=json_body if json_body is not None else {})

    # -- uploads ------------------------------------------------------------

    def upload_files(
        self,
        paths: str | os.PathLike[str] | list[str | os.PathLike[str]],
        name: str | None = None,
        on_progress: Callable[[str, int, int], None] | None = None,
        project: str | dict[str, Any] | None = None,
        private: bool = False,
        concurrency: int = DEFAULT_UPLOAD_CONCURRENCY,
    ) -> str:
        """Creates a dataset from one or more local files and returns its
        upload_id. Files up to 16 MB are sent `concurrency` at a time (default 4, like the web app; 1 sends one
        after another); larger files are then sent one after another in 16 MB chunks (resumable).
        Each file (or, for a large file, each chunk) is retried up to
        MAX_ATTEMPTS times with a short backoff before giving up. If a file still fails, the files not
        started yet are skipped and the error is raised; the dataset stays unfinished.

        `on_progress`, if given, is called as `on_progress(filename, loaded_bytes, total_bytes)`:
        with 0 when a file starts, while it uploads (about once per percent; per chunk for large
        files), and with `total_bytes` when it is done. After a retry `loaded_bytes` may start
        over from a lower value. With concurrency above 1 the calls for different files interleave (never
        two at the same moment). Default `None` is silent. Example:
        `on_progress=lambda name, done, total: print(f"{name}: {done / total:.0%}")`.

        `project` puts the dataset in a project: a `Project` from list_projects()/create_project(),
        a project id, or an exact project name (see list_projects()). `private=True` makes the
        dataset visible only to you, account admins and the members you share it with
        (set_upload_shares()); the default is visible to your whole account.

        `paths` can be a single file, a single folder (its top-level files
        are uploaded, see _resolve_paths), or a list mixing either.
        """
        paths = self._resolve_paths(paths)
        body: dict[str, Any] = {
            "files": [
                {"filename": p.name, "mime_type": mimetypes.guess_type(p.name)[0]} for p in paths
            ],
            "name": name,
        }
        if project is not None:
            body["project_id"] = self._project_id(project)
        if private:
            body["visibility"] = "private"
        created = self._post("/v1/uploads", body)
        upload_id = created["id"]
        file_by_name = {p.name: p for p in paths}
        progress_lock = threading.Lock()

        def reporter(name: str, total: int) -> Callable[[int], None] | None:
            if on_progress is None:
                return None

            def report(loaded: int) -> None:
                with progress_lock:  # several upload threads call this; one at a time
                    on_progress(name, loaded, total)

            return report

        small, large = [], []
        for file_out in created["files"]:
            local_path = file_by_name[file_out["filename"]]
            size = local_path.stat().st_size
            (small if size <= MULTIPART_CHUNK_SIZE_BYTES else large).append((file_out, local_path, size))

        # Small files straight to Object Storage (pre-authenticated URLs, no API calls), several at a time.
        if small:
            with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
                futures = [
                    pool.submit(self._upload_small_file, file_out["upload_url"], local_path, reporter(local_path.name, size))
                    for file_out, local_path, size in small
                ]
                done, _ = wait(futures, return_when=FIRST_EXCEPTION)
                failed = [f for f in done if f.exception() is not None]
                if failed:
                    for future in futures:
                        future.cancel()
                    raise failed[0].exception()
        # Large files go through the API chunk by chunk; one after another keeps within its rate limit.
        for file_out, local_path, size in large:
            self._upload_large_file(upload_id, file_out["id"], local_path, size, reporter(local_path.name, size))

        self._post(f"/v1/uploads/{upload_id}/complete", {})
        return upload_id

    def _resolve_paths(
        self, paths: str | os.PathLike[str] | list[str | os.PathLike[str]]
    ) -> list[Path]:
        """A single string/PathLike is treated as one path, not iterated
        character-by-character -- a real footgun the old list-only
        signature had if you passed a bare path instead of wrapping it in
        a list. Any directory among the given paths is expanded to its
        top-level files -- not recursive, deliberately: a folder with an
        unrelated nested subfolder (e.g. "processed/") shouldn't silently
        get swept in too. Hidden files (dotfiles) are skipped.
        """
        if isinstance(paths, (str, os.PathLike)):
            paths = [paths]
        resolved: list[Path] = []
        for p in paths:
            p = Path(p)
            if p.is_dir():
                resolved.extend(
                    sorted(f for f in p.iterdir() if f.is_file() and not f.name.startswith("."))
                )
            else:
                resolved.append(p)
        if not resolved:
            raise ValueError(f"no files found in {paths!r}")
        return resolved

    def _with_retry(self, attempt_fn: Callable[[], None]) -> None:
        last_err: Exception | None = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                attempt_fn()
                return
            except Exception as err:  # noqa: BLE001 -- deliberately broad, see retry loop
                last_err = err
                if attempt < MAX_ATTEMPTS:
                    time.sleep(0.5 * attempt)
        assert last_err is not None
        raise last_err

    def _upload_small_file(
        self, upload_url: str, local_path: Path, report: Callable[[int], None] | None = None
    ) -> None:
        # Pre-authenticated Object Storage URL -- no X-Api-Key here, the
        # URL itself is self-authenticating (same reasoning
        # putRawWithProgress in the frontend documents).
        size = local_path.stat().st_size

        def attempt() -> None:
            if report is not None:
                report(0)
            with local_path.open("rb") as f:
                body = _ProgressFile(f, size, report) if report is not None else f
                resp = requests.put(upload_url, data=body)
            if not resp.ok:
                raise ForestSensAPIError(resp.status_code, "upload_failed", f"HTTP {resp.status_code}")

        self._with_retry(attempt)
        if report is not None:
            report(size)

    def _upload_large_file(
        self,
        upload_id: str,
        file_id: str,
        local_path: Path,
        size: int,
        report: Callable[[int], None] | None = None,
    ) -> None:
        base = f"/v1/uploads/{upload_id}/files/{file_id}/multipart"
        started = self._post(f"{base}/start")
        already_done = {p["part_number"] for p in started["parts"]}
        chunk_size = started["chunk_size_bytes"]
        total_parts = -(-size // chunk_size)  # ceil division
        # Bytes storage already has (a resumed upload starts there, not at 0).
        loaded = sum(
            min(chunk_size, size - (n - 1) * chunk_size) for n in already_done if 1 <= n <= total_parts
        )
        if report is not None:
            report(loaded)

        with local_path.open("rb") as f:
            for part_number in range(1, total_parts + 1):
                start = (part_number - 1) * chunk_size
                length = min(chunk_size, size - start)
                if part_number in already_done:
                    continue  # real resume: skip what storage already has

                def attempt(part_number: int = part_number, start: int = start, length: int = length) -> None:
                    f.seek(start)
                    chunk = f.read(length)
                    resp = self._session.put(
                        self._url(f"{base}/parts/{part_number}"),
                        headers={"X-Api-Key": self.api_key},
                        data=chunk,
                    )
                    body = resp.json() if resp.content else None
                    if not resp.ok or (isinstance(body, dict) and body.get("error")):
                        err = (body or {}).get("error") or {}
                        raise ForestSensAPIError(
                            resp.status_code,
                            err.get("code", "unknown_error"),
                            err.get("message", f"HTTP {resp.status_code}"),
                        )

                self._with_retry(attempt)
                loaded += length
                if report is not None:
                    report(loaded)

        self._post(f"{base}/commit")

    # -- projects, datasets and sharing ---------------------------------------

    def list_projects(self) -> list[Project]:
        """The projects of your account."""
        return [Project(p) for p in self._get("/v1/projects")]

    def create_project(self, name: str) -> Project:
        return Project(self._post("/v1/projects", {"name": name}))

    def rename_project(self, project: str | dict[str, Any], name: str) -> Project:
        return Project(
            self._request("PATCH", f"/v1/projects/{self._project_id(project)}", json={"name": name})
        )

    def delete_project(self, project: str | dict[str, Any]) -> None:
        """Deletes the project only; its datasets are kept (they just belong to no project)."""
        self._request("DELETE", f"/v1/projects/{self._project_id(project)}")

    def _project_id(self, project: str | dict[str, Any]) -> str:
        """A Project/dict (its "id"), a project id, or an exact (case-insensitive) project name."""
        if isinstance(project, dict):
            return str(project["id"])
        if _UUID_RE.match(project):
            return project
        matches = [p for p in self.list_projects() if p["name"].lower() == project.lower()]
        if len(matches) == 1:
            return str(matches[0]["id"])
        names = sorted(p["name"] for p in self.list_projects())
        if not matches:
            raise LookupError(f"no project named {project!r}; projects: {names}")
        raise LookupError(f"several projects are named {project!r}; pass the project id instead")

    def list_uploads(
        self,
        project: str | dict[str, Any] | None = None,
        no_project: bool = False,
        created_by: str | None = None,
        status: str | None = None,
        sort: str = "created",
        direction: str = "desc",
        limit: int = 20,
        cursor: str | None = None,
    ) -> tuple[list[Upload], str | None]:
        """One page of datasets you can see (yours, your account's non-private ones, and private
        ones shared with you), newest first by default. Returns (uploads, next_cursor), like
        list_batches(). `project` limits it to one project (Project, id or name); `no_project=True`
        to datasets in no project; `created_by` is "me" or a user id.
        """
        params = {
            k: v
            for k, v in {
                "project_id": "none" if no_project else (self._project_id(project) if project else None),
                "created_by": created_by,
                "status": status,
                "sort": sort,
                "direction": direction,
                "limit": limit,
                "cursor": cursor,
            }.items()
            if v is not None
        }
        page = self._request_page("/v1/uploads", params)
        return [Upload(u) for u in page["items"]], page["next_cursor"]

    def get_upload(self, upload_id: str) -> Upload:
        return Upload(self._get(f"/v1/uploads/{upload_id}"))

    def update_upload(
        self,
        upload_id: str,
        *,
        name: str | None = None,
        project: Any = _KEEP,
        private: bool | None = None,
    ) -> Upload:
        """Changes only what you pass. `name` renames ("" restores the default name); `project`
        moves it to a project (Project, id or name) or, with `project=None`, out of its project;
        `private=True/False` makes it private or visible to your account again. Changing project or
        visibility is for the dataset's creator or an account admin.
        """
        body: dict[str, Any] = {}
        if name is not None:
            body["name"] = name
        if project is not _KEEP:
            body["project_id"] = None if project is None else self._project_id(project)
        if private is not None:
            body["visibility"] = "private" if private else "account"
        return Upload(self._request("PATCH", f"/v1/uploads/{upload_id}", json=body))

    def list_members(self) -> list[Member]:
        """The members of your account (to choose whom to share a dataset with)."""
        return [Member(m) for m in self._get("/v1/users/members")]

    def get_upload_shares(self, upload_id: str) -> list[Member]:
        """The members a private dataset is shared with."""
        return [Member(m) for m in self._get(f"/v1/uploads/{upload_id}/shares")]

    def set_upload_shares(self, upload_id: str, users: list[str | dict[str, Any]]) -> list[Member]:
        """Sets the members a private dataset is shared with; this REPLACES the previous list, so
        pass everyone who should have access (an empty list removes all shares). Each entry is a
        user id, an e-mail address, or a Member from list_members(). Returns the new list.
        """
        members: list[Member] | None = None
        ids: list[str] = []
        for user in users:
            if isinstance(user, dict):
                ids.append(str(user["user_id"]))
            elif _UUID_RE.match(user):
                ids.append(user)
            else:
                members = members if members is not None else self.list_members()
                found = [m for m in members if m["email"].lower() == user.lower()]
                if not found:
                    raise LookupError(f"no member with e-mail {user!r} in your account")
                ids.append(str(found[0]["user_id"]))
        return [
            Member(m)
            for m in self._request("PUT", f"/v1/uploads/{upload_id}/shares", json={"user_ids": ids})
        ]

    # -- pipelines ------------------------------------------------------------

    def list_pipelines(
        self, sens: str | None = None, *, include_retired: bool = False, include_drafts: bool = False
    ) -> list[Pipeline]:
        """The pipelines you can run: the newest version of each. `include_retired=True` also lists older versions
        (still runnable by their id or `short_id@N`), `include_drafts=True` your account's drafts."""
        params: dict[str, Any] = {}
        if sens:
            params["sens"] = sens
        if include_retired:
            params["include_retired"] = "true"
        if include_drafts:
            params["include_drafts"] = "true"
        return [Pipeline(d) for d in self._get("/v1/pipelines", params=params or None)]

    def find_pipeline(self, query: str, sens: str | None = None) -> Pipeline:
        """Finds one pipeline by its short id ("k3x9", as printed by
        Pipeline's repr), its exact name, a case-insensitive part of its
        name ("canopy", "detectree2"), or the start of its UUID. Earlier
        matches win: a short id beats a name, an exact name beats a partial
        one. Raises LookupError with the real candidates if nothing matches
        or if more than one does.
        """
        # "k3x9@2" names one (possibly older, retired) version: look among all versions.
        pipelines = self.list_pipelines(sens=sens, include_retired="@" in query)
        needle = query.lower()
        base, _, number = needle.partition("@")
        strategies = (
            lambda p: p.short_id.lower() == needle,
            # "k3x9@1": the first version keeps the plain short id "k3x9".
            lambda p: bool(number) and p.short_id.lower() == base and str(p.get("version") or 1) == number,
            lambda p: p.get("name") == query,
            lambda p: needle in (p.get("name") or "").lower(),
            lambda p: str(p.get("id")).startswith(needle),
        )
        matches: list[Pipeline] = []
        for matches_by in strategies:
            matches = [p for p in pipelines if matches_by(p)]
            if matches:
                break
        if not matches:
            available = ", ".join(sorted(str(p) for p in pipelines))
            raise LookupError(f"no pipeline matches {query!r} -- available: {available or '(none)'}")
        if len(matches) > 1:
            candidates = ", ".join(sorted(str(p) for p in matches))
            raise LookupError(f"{query!r} matches more than one pipeline: {candidates}")
        return matches[0]

    # -- batches ------------------------------------------------------------

    def create_batch(
        self,
        pipeline_id: str,
        inputs: list[dict[str, Any]],
        *,
        use_latest_version: bool = False,
        name: str | None = None,
        wait_for_slot: bool = False,
        slot_timeout: float | None = None,
    ) -> Batch:
        """inputs: [{"slot": str, "upload_id": str}, ...] -- see
        list_pipelines()'s returned graph for a given pipeline's expected
        slot names.

        Versions: a pipeline's short id runs its newest published version; an older version's id or `short_id@N`
        runs exactly that version, and a ForestSensNewerVersionWarning says a newer one exists.
        `use_latest_version=True` runs the newest version whatever id is given. The returned batch's
        `pipeline_id` is the version that runs.

        Each user may have 20 batches queued or running (an account 50); one more is refused with
        `429 quota_exceeded`. `wait_for_slot=True` waits the seconds the server names and tries again until a slot
        is free (or `slot_timeout` seconds have passed). To run many batches, use run_many(), which keeps only a
        few in flight. `name` labels the batch.
        """
        body: dict[str, Any] = {"pipeline_id": pipeline_id, "inputs": inputs}
        if use_latest_version:
            body["use_latest_version"] = True
        if name:
            body["name"] = name
        deadline = time.monotonic() + slot_timeout if slot_timeout is not None else None
        while True:
            try:
                created = self._post("/v1/batches", body)
                break
            except ForestSensAPIError as exc:
                if not wait_for_slot or exc.status != 429 or exc.code != "quota_exceeded":
                    raise
                wait_s = exc.retry_after if exc.retry_after is not None else DEFAULT_SLOT_WAIT_SECONDS
                if deadline is not None and time.monotonic() + wait_s > deadline:
                    raise
                time.sleep(max(wait_s, 1.0))
        batch = Batch(created)
        newer = batch.get("newer_pipeline_version")
        if newer:
            warnings.warn(
                f"pipeline {newer.get('name') or pipeline_id!r} has a newer version ({newer.get('version')}); this batch "
                f"runs the older one. Use the short id {newer.get('short_id')!r} or use_latest=True for the newest.",
                ForestSensNewerVersionWarning,
                stacklevel=2,
            )
        return batch

    def get_batch(self, batch_id: str) -> Batch:
        return Batch(self._get(f"/v1/batches/{batch_id}"))

    def list_batches(
        self,
        status: str | None = None,
        upload_id: str | None = None,
        created_by: str | None = None,
        sort: str = "created",
        direction: str = "desc",
        limit: int = 20,
        cursor: str | None = None,
    ) -> tuple[list[Batch], str | None]:
        """One page of batches for the caller's account, newest first by
        default. Returns (batches, next_cursor) -- pass next_cursor back
        in as cursor to fetch the next page; None means there are no
        more. There is no pipeline filter here: GET /v1/batches doesn't
        take one server-side -- filter the returned batches' pipeline_id
        yourself if you only want one pipeline's.
        """
        params = {
            k: v
            for k, v in {
                "status": status,
                "upload_id": upload_id,
                "created_by": created_by,
                "sort": sort,
                "direction": direction,
                "limit": limit,
                "cursor": cursor,
            }.items()
            if v is not None
        }
        page = self._request_page("/v1/batches", params)
        return [Batch(b) for b in page["items"]], page["next_cursor"]

    def wait_for_batch(
        self,
        batch_id: str,
        poll_interval: float = 5.0,
        timeout: float | None = None,
        on_progress: Callable[[Batch], None] | None = None,
    ) -> Batch:
        """Polls until the batch reaches status "complete", "failed" or "cancelled".
        Raises BatchFailedError (carrying the full batch dict, including
        per-step errors/log tails) on failure or cancellation (code "batch_cancelled") -- the "one call to get to
        a finished batch" convenience this kind of client should have.

        `on_progress`, if given, is called once each time the batch's
        status changes (e.g. "queued" -> "running" -> "complete"), not on
        every poll -- pass on_progress=print for a one-line progress log
        on an otherwise-silent long GPU job.

        Real bug, found 2026-09-15: this checked for status "done", a
        value the real API never actually returns (Batch.status is only
        ever "queued"/"running"/"complete"/"failed", per the backend's
        own DB check constraint) -- every call to this method looped
        until timeout (or forever, the default with no timeout given)
        even for a batch that completed successfully. The test suite's
        own mock fabricated "done" as the terminal status, which is
        exactly how this went uncaught: it validated a status this API
        doesn't produce, not the real contract.
        """
        deadline = time.monotonic() + timeout if timeout is not None else None
        last_status = None
        while True:
            batch = self.get_batch(batch_id)
            if on_progress is not None and batch["status"] != last_status:
                on_progress(batch)
                last_status = batch["status"]
            if batch["status"] == "complete":
                return batch
            if batch["status"] in ("failed", "cancelled"):
                raise BatchFailedError(batch)
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"batch {batch_id} did not finish within {timeout}s")
            time.sleep(poll_interval)

    def run(
        self,
        pipeline: str | dict[str, Any],
        paths: str | os.PathLike[str] | list[str | os.PathLike[str]],
        dest_dir: str | os.PathLike[str],
        *,
        slot: str = "input",
        name: str | None = None,
        poll_interval: float = 5.0,
        timeout: float | None = None,
        on_progress: Callable[[Batch], None] | None = None,
        on_upload_progress: Callable[[str, int, int], None] | None = None,
        project: str | dict[str, Any] | None = None,
        private: bool = False,
        use_latest: bool = False,
    ) -> list[str]:
        """Upload -> start a batch -> wait -> download, for the common
        single-input case -- collapses the whole upload/create_batch/
        wait_for_batch/download_artifacts sequence into one call. Accepts
        a Pipeline (from list_pipelines()/find_pipeline()) or a bare
        pipeline id string.

        Needing more than one input slot, or wanting to inspect/act on
        the batch mid-flight (e.g. cancel it)? Use create_batch/
        wait_for_batch/download_artifacts directly instead -- this is a
        shortcut for the common case, not a replacement for them.

        `on_progress` reports the batch's status changes; `on_upload_progress` reports the upload
        as `(filename, loaded_bytes, total_bytes)`, see upload_files(). `project` and `private` are
        passed to upload_files() too. `use_latest=True` runs the pipeline's newest published version even
        when `pipeline` is an older version (see create_batch()).
        """
        pipeline_id = pipeline["id"] if isinstance(pipeline, dict) else pipeline
        upload_id = self.upload_files(
            paths, name=name, on_progress=on_upload_progress, project=project, private=private
        )
        inputs = [{"slot": slot, "upload_id": upload_id}]
        if use_latest:
            batch = self.create_batch(pipeline_id, inputs, use_latest_version=True)
        else:
            batch = self.create_batch(pipeline_id, inputs)
        batch = self.wait_for_batch(
            batch["id"], poll_interval=poll_interval, timeout=timeout, on_progress=on_progress
        )
        return self.download_artifacts(batch["id"], dest_dir)

    def run_many(
        self,
        jobs: Any,
        *,
        max_active: int = DEFAULT_MAX_ACTIVE,
        poll_interval: float = 10.0,
        dest_dir: str | os.PathLike[str] | None = None,
        use_latest: bool = False,
        stop_on_failure: bool = False,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[RunResult]:
        """Runs many batches a few at a time: at most `max_active` of them are queued or running at once; when one
        ends, the next is started. Use this instead of a loop of create_batch(), which sends everything at once,
        is refused after 20 (the per-user cap) and starts many machines at the same moment.

        `jobs`: dicts, each with "pipeline" (a Pipeline or an id) and one of "upload_id" (a dataset in ForestSens),
        "inputs" ([{"slot", "upload_id"}]) or "paths" (local files or a folder, uploaded just before the job
        starts); optional "name" (a label: the batch's name, the dataset's name with "paths", and the download
        folder) and "slot" (default "input").

        A batch that fails or is cancelled does not stop the others (`stop_on_failure=True` starts no new ones
        after a failure; the running ones are still waited for). A job that cannot be uploaded or started gets
        status "error". When the server says the user's batch slots are full (429), the job waits and is tried
        again; that is not a failure. With `dest_dir`, a completed batch's files go to `dest_dir/<name or batch id>/`.
        `on_progress(summary)` is called after every change with {"total", "done", "failed", "running", "waiting",
        "result"}. Ctrl-C stops starting new jobs and re-raises; batches already started keep running on the server
        (their ids are printed). Returns one RunResult per job, in the order of `jobs`.
        """
        if not isinstance(max_active, int) or not 1 <= max_active <= MAX_ACTIVE_LIMIT:
            raise ValueError(f"max_active must be between 1 and {MAX_ACTIVE_LIMIT}, not {max_active!r}")
        job_list = [dict(job) for job in jobs]
        for job in job_list:
            if "pipeline" not in job or not ({"upload_id", "inputs", "paths"} & set(job)):
                raise ValueError(f"each job needs 'pipeline' and one of 'upload_id', 'inputs', 'paths': {job!r}")
        results = [RunResult(index=i, job=job, batch_id=None, status="waiting", error=None, batch=None, paths=[])
                   for i, job in enumerate(job_list)]
        waiting = list(range(len(job_list)))
        active: dict[int, str] = {}  # job index -> batch id
        stopped = False
        not_before = 0.0  # after a 429: when to try starting again

        def report(result: RunResult) -> None:
            if on_progress is None:
                return
            statuses = [r["status"] for r in results]
            on_progress({
                "total": len(results),
                "done": statuses.count("complete"),
                "failed": sum(statuses.count(s) for s in ("failed", "cancelled", "error")),
                "running": len(active),
                "waiting": len(waiting),
                "result": result,
            })

        def finish(i: int, status: str, error: str | None = None, batch: Any = None) -> None:
            nonlocal stopped
            result = results[i]
            result.update(status=status, error=error, batch=batch if batch is not None else result["batch"])
            if status == "complete" and dest_dir is not None:
                label = job_list[i].get("name") or result["batch_id"]
                try:
                    result["paths"] = self.download_artifacts(result["batch_id"], Path(dest_dir) / str(label))
                except Exception as exc:  # noqa: BLE001 -- the batch itself completed
                    result["error"] = f"download failed: {exc}"
            if status != "complete" and stop_on_failure:
                stopped = True
            report(result)

        def start(i: int) -> bool:
            """True if the job left the waiting list (started, or failed to start); False to try again later."""
            nonlocal not_before
            job = job_list[i]
            pipeline = job["pipeline"]
            pipeline_id = pipeline["id"] if isinstance(pipeline, dict) else pipeline
            try:
                if "inputs" in job:
                    inputs = job["inputs"]
                else:
                    upload_id = job.get("upload_id")
                    if upload_id is None:
                        upload_id = self.upload_files(job["paths"], name=job.get("name"))
                        job["upload_id"] = upload_id  # a retry after a 429 does not upload again
                    inputs = [{"slot": job.get("slot", "input"), "upload_id": upload_id}]
                batch = self.create_batch(pipeline_id, inputs, use_latest_version=use_latest, name=job.get("name"))
            except ForestSensAPIError as exc:
                if exc.status == 429:
                    wait_s = exc.retry_after if exc.retry_after is not None else DEFAULT_SLOT_WAIT_SECONDS
                    not_before = time.monotonic() + max(wait_s, 1.0)
                    return False
                finish(i, "error", str(exc))
                return True
            except Exception as exc:  # noqa: BLE001 -- one job's upload/start problem, not the others'
                finish(i, "error", f"{type(exc).__name__}: {exc}")
                return True
            results[i].update(batch_id=batch["id"], status="queued", batch=batch)
            active[i] = batch["id"]
            report(results[i])
            return True

        try:
            while waiting or active:
                # Start jobs while there is room (and no 429 pause, and not stopped).
                while waiting and len(active) < max_active and not stopped and time.monotonic() >= not_before:
                    i = waiting[0]
                    if start(i):
                        waiting.pop(0)
                    else:
                        break
                if stopped and not active:
                    break
                if not active and waiting and not stopped:
                    time.sleep(max(not_before - time.monotonic(), poll_interval))
                    continue
                time.sleep(poll_interval)
                for i, batch_id in list(active.items()):
                    try:
                        batch = self.get_batch(batch_id)
                    except ForestSensAPIError:
                        continue  # a passing API problem: ask again next round
                    status = batch.get("status")
                    if status in ("complete", "failed", "cancelled"):
                        del active[i]
                        error = None if status == "complete" else (batch.get("error_message") or status)
                        finish(i, status, error, batch)
                    elif status != results[i]["status"]:
                        results[i].update(status=status, batch=batch)
                        report(results[i])
        except KeyboardInterrupt:
            if active:
                print(f"\nstopped; {len(active)} batch(es) keep running on the server: {', '.join(active.values())}")
            raise
        for i in waiting:  # not started because of stop_on_failure
            results[i].update(status="error", error="not started (stop_on_failure)")
        return results

    # -- downloads ------------------------------------------------------------

    def download_artifacts(self, batch_id: str, dest_dir: str | os.PathLike[str]) -> list[str]:
        """Downloads every artifact produced by a batch into dest_dir,
        returns the local paths written. An artifact's download_url is
        either a live pre-authenticated Object Storage URL, or an API
        path (/v1/batches/.../download-all) that answers with one; both
        work. ArtifactOut carries no filename, so one is derived from
        output_type + format + a short id fragment (unchanged; the
        platform's downloads-revision gives files their real names).
        """
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)
        written: list[str] = []
        cursor = None
        while True:
            params = {"cursor": cursor} if cursor else None
            page = self._request_page(f"/v1/batches/{batch_id}/artifacts", params)
            for artifact in page["items"]:
                # ArtifactOut carries no filename or file extension --
                # `format` (e.g. "cog_tiff", "pmtiles", "laz") is used
                # as-is rather than guessed-mapped to a conventional
                # extension, so this never silently mislabels a file.
                local_path = dest / f"{artifact['output_type']}_{artifact['id'][:8]}.{artifact['format']}"
                self._download_one(self._artifact_url(artifact), local_path)
                written.append(str(local_path))
            cursor = page["next_cursor"]
            if not cursor:
                break
        return written

    # -- typed results ---------------------------------------------------------

    @staticmethod
    def _result_params(filters: dict[str, Any]) -> dict[str, Any]:
        params: dict[str, Any] = {}
        for key, value in filters.items():
            if value is None:
                continue
            if key == "class_":
                key = "class"
            if key == "bbox" and not isinstance(value, str):
                value = ",".join(str(v) for v in value)
            params[key] = value
        return params

    def _result_page(self, batch_id: str, table: str, filters: dict[str, Any]) -> tuple[list[Any], str | None]:
        if table not in RESULT_TABLES:
            raise ValueError(f"table must be one of {', '.join(RESULT_TABLES)}, not {table!r}")
        route, view = RESULT_TABLES[table]
        page = self._request_page(f"/v1/batches/{batch_id}/{route}", self._result_params(filters))
        return [view(row) for row in page["items"]], page["next_cursor"]

    def list_detections(
        self, batch_id: str, step_id: str | None = None, class_: str | None = None,
        confidence_min: float | None = None, bbox: Any = None, limit: int = RESULT_PAGE_SIZE,
        cursor: str | None = None,
    ) -> tuple[list[Detection], str | None]:
        """One page of a batch's detections, newest first. Returns (rows, next_cursor), like list_batches().
        `class_` keeps one class, `confidence_min` drops weaker ones, `bbox` is (min_lon, min_lat, max_lon, max_lat)
        or the same as a "minLon,minLat,maxLon,maxLat" string, `step_id` one step of a multi-step batch."""
        return self._result_page(batch_id, "detections", dict(
            step_id=step_id, class_=class_, confidence_min=confidence_min, bbox=bbox, limit=limit, cursor=cursor))

    def list_segments(
        self, batch_id: str, step_id: str | None = None, class_: str | None = None,
        confidence_min: float | None = None, bbox: Any = None, limit: int = RESULT_PAGE_SIZE,
        cursor: str | None = None,
    ) -> tuple[list[Segment], str | None]:
        """One page of a batch's segments (for example tree crowns); filters as list_detections()."""
        return self._result_page(batch_id, "segments", dict(
            step_id=step_id, class_=class_, confidence_min=confidence_min, bbox=bbox, limit=limit, cursor=cursor))

    def list_linear_features(
        self, batch_id: str, step_id: str | None = None, class_: str | None = None,
        confidence_min: float | None = None, bbox: Any = None, limit: int = RESULT_PAGE_SIZE,
        cursor: str | None = None,
    ) -> tuple[list[LinearFeature], str | None]:
        """One page of a batch's linear features (for example wheel ruts); filters as list_detections()."""
        return self._result_page(batch_id, "linear_features", dict(
            step_id=step_id, class_=class_, confidence_min=confidence_min, bbox=bbox, limit=limit, cursor=cursor))

    def list_tree_inventory(
        self, batch_id: str, step_id: str | None = None, species: str | None = None,
        dbh_min: float | None = None, dbh_max: float | None = None, limit: int = RESULT_PAGE_SIZE,
        cursor: str | None = None,
    ) -> tuple[list[TreeRecord], str | None]:
        """One page of a batch's tree inventory. `species`, `dbh_min`/`dbh_max` (cm) filter it."""
        return self._result_page(batch_id, "tree_inventory", dict(
            step_id=step_id, species=species, dbh_min=dbh_min, dbh_max=dbh_max, limit=limit, cursor=cursor))

    def iter_results(self, batch_id: str, table: str, **filters: Any) -> Iterator[Any]:
        """Every row of one result table, page by page (1000 rows a page). `table` is "detections", "segments",
        "linear_features" or "tree_inventory"; `filters` are those of the matching list_...() method."""
        filters.pop("cursor", None)
        filters.setdefault("limit", RESULT_PAGE_SIZE)
        cursor = None
        while True:
            rows, cursor = self._result_page(batch_id, table, {**filters, "cursor": cursor})
            yield from rows
            if not cursor:
                return

    def get_results(self, batch_id: str, table: str, **filters: Any) -> list[Any]:
        """All rows of one result table as a list (see iter_results())."""
        return list(self.iter_results(batch_id, table, **filters))

    def results_geojson(self, batch_id: str, table: str, **filters: Any) -> dict[str, Any]:
        """One result table as a GeoJSON FeatureCollection (WGS84): each row's `geom` is the geometry, everything
        else its properties. With geopandas: `geopandas.GeoDataFrame.from_features(fc["features"], crs="EPSG:4326")`."""
        features = []
        for row in self.iter_results(batch_id, table, **filters):
            properties = {k: v for k, v in row.items() if k != "geom"}
            features.append({"type": "Feature", "id": row.get("id"), "geometry": row.get("geom"),
                             "properties": properties})
        return {"type": "FeatureCollection", "features": features}

    def save_results(
        self, batch_id: str, dest_dir: str | os.PathLike[str], tables: list[str] | None = None
    ) -> list[str]:
        """Writes `<table>.geojson` into dest_dir for each result table the batch has rows in (or only `tables`),
        returns the paths written. Result files (orthophotos, point clouds) come from download_artifacts()."""
        summary = self.get_batch(batch_id).get("results") or {}
        wanted = tables if tables is not None else [t for t in RESULT_TABLES if (summary.get(t) or {}).get("count")]
        for table in wanted:
            if table not in RESULT_TABLES:
                raise ValueError(f"table must be one of {', '.join(RESULT_TABLES)}, not {table!r}")
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)
        written = []
        for table in wanted:
            path = dest / f"{table}.geojson"
            with path.open("w") as f:
                json.dump(self.results_geojson(batch_id, table), f)
            written.append(str(path))
        return written

    def _request_page(self, path: str, params: dict[str, Any] | None) -> dict[str, Any]:
        headers = {"X-Api-Key": self.api_key}
        for attempt in range(1, RATE_LIMIT_ATTEMPTS + 1):
            resp = self._session.get(self._url(path), headers=headers, params=params)
            if resp.status_code != 429 or attempt == RATE_LIMIT_ATTEMPTS:
                break
            # The per-key rate limit (paging a large result table can reach it): wait as told, then ask again.
            try:
                wait_s = float(resp.headers.get("Retry-After", 1))
            except (TypeError, ValueError):
                wait_s = 1.0
            time.sleep(min(max(wait_s, 0.5), 60))
        body = resp.json()
        if not resp.ok or body.get("error"):
            err = body.get("error") or {}
            raise ForestSensAPIError(
                resp.status_code, err.get("code", "unknown_error"), err.get("message", ""), retry_after=_retry_after(resp)
            )
        return {
            "items": body["data"],
            "next_cursor": (body.get("meta", {}).get("pagination") or {}).get("next_cursor"),
        }

    def _artifact_url(self, artifact: dict[str, Any]) -> str:
        """The artifact's pre-authenticated download link. Older servers put it in download_url directly; newer ones
        give an API path there, which answers (with the API key) {"download_url": <link>}."""
        url = artifact.get("download_url")
        if not url:
            raise ForestSensAPIError(200, "no_download_url", f"artifact {artifact.get('id')} has no download_url")
        if not url.startswith("/"):
            return url
        if url.startswith("/v1-key/"):
            url = "/v1/" + url[len("/v1-key/"):]
        if not url.startswith("/v1/"):
            raise ForestSensAPIError(200, "no_download_url", f"unexpected download path for artifact {artifact.get('id')}: {url!r}")
        reply = self._request("GET", url)
        link = reply.get("download_url") if isinstance(reply, dict) else None
        if not link or link.startswith("/"):
            raise ForestSensAPIError(200, "no_download_url", f"no download link for artifact {artifact.get('id')}: {reply!r}")
        return link

    def _download_one(self, url: str, local_path: Path) -> None:
        # A pre-authenticated link: requests.get, not the session, so no API key is ever sent to Object Storage.
        with requests.get(url, stream=True) as resp:
            resp.raise_for_status()
            with local_path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
