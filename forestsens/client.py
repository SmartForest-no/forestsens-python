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
import time
import warnings
from pathlib import Path
from typing import Any, Callable

import requests

from .errors import BatchFailedError, ForestSensAPIError, ForestSensNewerVersionWarning

# Must match forestsens/services/uploads.py's MULTIPART_CHUNK_SIZE_BYTES --
# kept in sync by hand (same as the frontend does), not derived at
# runtime. 16 MiB: real margin under the API Gateway's confirmed 20MB
# request-body ceiling, which every multipart chunk has to pass through
# since pre-authenticated requests don't support multipart at all.
MULTIPART_CHUNK_SIZE_BYTES = 16 * 1024 * 1024

MAX_ATTEMPTS = 3
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
            if error:
                raise ForestSensAPIError(
                    resp.status_code, error["code"], error["message"], error.get("details")
                )
            raise ForestSensAPIError(resp.status_code, "unknown_error", f"HTTP {resp.status_code}")
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
    ) -> str:
        """Creates a dataset from one or more local files and returns its
        upload_id. Sequential per file, on purpose -- simpler than the
        frontend's bounded-concurrency pool.
        Each file (or, for a large file, each chunk) is retried up to
        MAX_ATTEMPTS times with a short backoff before giving up.

        `on_progress`, if given, is called as `on_progress(filename, loaded_bytes, total_bytes)`:
        with 0 when a file starts, while it uploads (about once per percent; per chunk for large
        files), and with `total_bytes` when it is done. After a retry `loaded_bytes` may start
        over from a lower value. Default `None` is silent. Example:
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

        for file_out in created["files"]:
            local_path = file_by_name[file_out["filename"]]
            size = local_path.stat().st_size
            report = None
            if on_progress is not None:
                def report(loaded: int, name: str = local_path.name, total: int = size) -> None:
                    on_progress(name, loaded, total)

            if size <= MULTIPART_CHUNK_SIZE_BYTES:
                self._upload_small_file(file_out["upload_url"], local_path, report)
            else:
                self._upload_large_file(upload_id, file_out["id"], local_path, size, report)

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
        self, pipeline_id: str, inputs: list[dict[str, Any]], *, use_latest_version: bool = False
    ) -> Batch:
        """inputs: [{"slot": str, "upload_id": str}, ...] -- see
        list_pipelines()'s returned graph for a given pipeline's expected
        slot names.

        Versions: a pipeline's short id runs its newest published version; an older version's id or `short_id@N`
        runs exactly that version, and a ForestSensNewerVersionWarning says a newer one exists.
        `use_latest_version=True` runs the newest version whatever id is given. The returned batch's
        `pipeline_id` is the version that runs.
        """
        body: dict[str, Any] = {"pipeline_id": pipeline_id, "inputs": inputs}
        if use_latest_version:
            body["use_latest_version"] = True
        batch = Batch(self._post("/v1/batches", body))
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
        """Polls until the batch reaches status "complete" or "failed".
        Raises BatchFailedError (carrying the full batch dict, including
        per-step errors/log tails) on failure -- the "one call to get to
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
            if batch["status"] == "failed":
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

    # -- downloads ------------------------------------------------------------

    def download_artifacts(self, batch_id: str, dest_dir: str | os.PathLike[str]) -> list[str]:
        """Downloads every artifact produced by a batch into dest_dir,
        returns the local paths written. Each artifact's download_url is
        already a live pre-authenticated Object Storage URL (minted
        fresh per call) -- no separate "generate a download link" step.
        ArtifactOut carries no filename, so one is derived from
        output_type + format + a short id fragment.
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
                self._download_one(artifact["download_url"], local_path)
                written.append(str(local_path))
            cursor = page["next_cursor"]
            if not cursor:
                break
        return written

    def _request_page(self, path: str, params: dict[str, Any] | None) -> dict[str, Any]:
        headers = {"X-Api-Key": self.api_key}
        resp = self._session.get(self._url(path), headers=headers, params=params)
        body = resp.json()
        if not resp.ok or body.get("error"):
            err = body.get("error") or {}
            raise ForestSensAPIError(
                resp.status_code, err.get("code", "unknown_error"), err.get("message", "")
            )
        return {
            "items": body["data"],
            "next_cursor": (body.get("meta", {}).get("pagination") or {}).get("next_cursor"),
        }

    def _download_one(self, url: str, local_path: Path) -> None:
        with requests.get(url, stream=True) as resp:
            resp.raise_for_status()
            with local_path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
