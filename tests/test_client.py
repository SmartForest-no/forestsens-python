"""Mocks the HTTP layer (unittest.mock.patch), same testing philosophy
this repo's own tests already used before the rewrite -- pytest style,
pointed at the real v2 envelope/endpoint shapes instead of the legacy
API's.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from forestsens import Batch, BatchFailedError, Client, ForestSensAPIError, Pipeline


def make_client(**kwargs) -> Client:
    return Client(gateway_host="example.apigateway.oci.customer-oci.com", api_key="fs_test", **kwargs)


def envelope_response(data=None, error=None, status=200, pagination=None):
    resp = MagicMock()
    resp.status_code = status
    resp.ok = 200 <= status < 300
    body = {"data": data, "meta": {"request_id": "r1", "pagination": pagination}, "error": error}
    resp.json.return_value = body
    resp.content = json.dumps(body).encode()
    return resp


# -- config resolution -------------------------------------------------


def test_client_requires_gateway_host_and_api_key(tmp_path):
    empty_config = tmp_path / "config.json"
    empty_config.write_text("{}")
    with pytest.raises(ValueError):
        Client(config_path=empty_config)


def test_client_reads_config_file(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"gateway_host": "cfg.example.com", "api_key": "fs_cfg"}))
    client = Client(config_path=config)
    assert client.gateway_host == "cfg.example.com"
    assert client.api_key == "fs_cfg"


def test_explicit_args_override_config_file(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"gateway_host": "cfg.example.com", "api_key": "fs_cfg"}))
    client = Client(gateway_host="explicit.example.com", api_key="fs_explicit", config_path=config)
    assert client.gateway_host == "explicit.example.com"
    assert client.api_key == "fs_explicit"


def test_env_vars_override_config_file(tmp_path, monkeypatch):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"gateway_host": "cfg.example.com", "api_key": "fs_cfg"}))
    monkeypatch.setenv("FORESTSENS_GATEWAY_HOST", "env.example.com")
    monkeypatch.setenv("FORESTSENS_API_KEY", "fs_env")
    client = Client(config_path=config)
    assert client.gateway_host == "env.example.com"
    assert client.api_key == "fs_env"


# -- URL building / auth header -------------------------------------------------


def test_calls_go_through_v1_key_route_with_api_key_header():
    client = make_client()
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=[{"id": "p1"}])
        client.list_pipelines()
    called_url = mock_request.call_args.args[1]
    called_headers = mock_request.call_args.kwargs["headers"]
    assert called_url == "https://example.apigateway.oci.customer-oci.com/v1-key/pipelines"
    assert called_headers["X-Api-Key"] == "fs_test"


# -- error handling -------------------------------------------------


def test_raises_forestsens_api_error_on_envelope_error():
    client = make_client()
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(
            error={"code": "not_found", "message": "no such batch", "details": []}, status=404
        )
        with pytest.raises(ForestSensAPIError) as exc_info:
            client.get_batch("nonexistent")
    assert exc_info.value.code == "not_found"
    assert exc_info.value.status == 404


# -- uploads -------------------------------------------------


def test_upload_files_small_file_uses_put_to_par_no_auth_header(tmp_path):
    client = make_client()
    local_file = tmp_path / "flight1.jpg"
    local_file.write_bytes(b"tiny file contents")

    created = {
        "id": "upload-1",
        "files": [{"id": "file-1", "filename": "flight1.jpg", "upload_url": "https://par.example/flight1.jpg"}],
    }

    with patch("requests.Session.request") as mock_request, patch("requests.put") as mock_put:
        mock_request.side_effect = [
            envelope_response(data=created),  # POST /v1/uploads
            envelope_response(data={"id": "upload-1", "status": "ready"}),  # POST .../complete
        ]
        mock_put.return_value = MagicMock(ok=True, status_code=200)

        upload_id = client.upload_files([local_file])

    assert upload_id == "upload-1"
    mock_put.assert_called_once()
    assert mock_put.call_args.args[0] == "https://par.example/flight1.jpg"
    # PAR PUT never carries our own auth -- the URL is self-authenticating.
    assert "headers" not in mock_put.call_args.kwargs


def test_upload_files_accepts_a_single_file_path_not_wrapped_in_a_list(tmp_path):
    """Real footgun the old list-only signature had: passing a bare
    string/Path (instead of wrapping it in a list) used to iterate over
    its individual characters via `[Path(p) for p in paths]`.
    """
    client = make_client()
    local_file = tmp_path / "flight1.jpg"
    local_file.write_bytes(b"tiny file contents")

    created = {
        "id": "upload-1",
        "files": [{"id": "file-1", "filename": "flight1.jpg", "upload_url": "https://par.example/flight1.jpg"}],
    }

    with patch("requests.Session.request") as mock_request, patch("requests.put") as mock_put:
        mock_request.side_effect = [
            envelope_response(data=created),
            envelope_response(data={"id": "upload-1", "status": "ready"}),
        ]
        mock_put.return_value = MagicMock(ok=True, status_code=200)

        upload_id = client.upload_files(local_file)

    assert upload_id == "upload-1"
    sent_json = mock_request.call_args_list[0].kwargs["json"]
    assert sent_json["files"] == [{"filename": "flight1.jpg", "mime_type": "image/jpeg"}]


def test_upload_files_accepts_a_folder_and_uploads_its_files_non_recursively(tmp_path):
    client = make_client()
    (tmp_path / "a.jpg").write_bytes(b"a")
    (tmp_path / "b.jpg").write_bytes(b"b")
    (tmp_path / ".hidden.jpg").write_bytes(b"hidden")
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    (subdir / "c.jpg").write_bytes(b"c")

    created = {
        "id": "upload-1",
        "files": [
            {"id": "file-1", "filename": "a.jpg", "upload_url": "https://par.example/a.jpg"},
            {"id": "file-2", "filename": "b.jpg", "upload_url": "https://par.example/b.jpg"},
        ],
    }

    with patch("requests.Session.request") as mock_request, patch("requests.put") as mock_put:
        mock_request.side_effect = [
            envelope_response(data=created),
            envelope_response(data={"id": "upload-1", "status": "ready"}),
        ]
        mock_put.return_value = MagicMock(ok=True, status_code=200)

        client.upload_files(tmp_path)

    sent_json = mock_request.call_args_list[0].kwargs["json"]
    sent_filenames = [f["filename"] for f in sent_json["files"]]
    assert sent_filenames == ["a.jpg", "b.jpg"]  # sorted; hidden and subdir/ excluded


def test_upload_files_large_file_uses_resumable_multipart(tmp_path, monkeypatch):
    client = make_client()
    monkeypatch.setattr("forestsens.client.MULTIPART_CHUNK_SIZE_BYTES", 10)
    local_file = tmp_path / "big.tif"
    local_file.write_bytes(b"0123456789" * 3)  # 30 bytes -> 3 chunks of 10

    created = {
        "id": "upload-1",
        "files": [{"id": "file-1", "filename": "big.tif", "upload_url": "https://par.example/big.tif"}],
    }
    started = {
        "multipart_upload_id": "mp-1",
        "chunk_size_bytes": 10,
        "parts": [{"part_number": 1, "etag": "e1", "size_bytes": 10}],  # part 1 already landed
    }

    with patch("requests.Session.request") as mock_request, patch(
        "requests.Session.put"
    ) as mock_session_put:
        mock_request.side_effect = [
            envelope_response(data=created),  # POST /v1/uploads
            envelope_response(data=started),  # POST .../multipart/start
            envelope_response(data={"id": "upload-1", "status": "ready"}),  # POST .../multipart/commit is via _post too
            envelope_response(data={"id": "upload-1", "status": "ready"}),  # POST .../complete
        ]
        mock_session_put.return_value = envelope_response(
            data={"part_number": 2, "etag": "e2", "size_bytes": 10}
        )

        client.upload_files([local_file])

    # Only parts 2 and 3 uploaded -- part 1 was already reported as done.
    assert mock_session_put.call_count == 2


def test_upload_files_reports_progress_for_a_small_file(tmp_path):
    client = make_client()
    local_file = tmp_path / "flight1.jpg"
    local_file.write_bytes(b"x" * 1000)
    created = {
        "id": "upload-1",
        "files": [{"id": "file-1", "filename": "flight1.jpg", "upload_url": "https://par.example/flight1.jpg"}],
    }

    def fake_put(url, data):
        data.read()  # the real transport reads the body while sending it
        return MagicMock(ok=True, status_code=200)

    calls = []
    with patch("requests.Session.request") as mock_request, patch("requests.put", side_effect=fake_put):
        mock_request.side_effect = [
            envelope_response(data=created),
            envelope_response(data={"id": "upload-1", "status": "ready"}),
        ]
        client.upload_files([local_file], on_progress=lambda *a: calls.append(a))

    assert calls[0] == ("flight1.jpg", 0, 1000)
    assert calls[-1] == ("flight1.jpg", 1000, 1000)
    assert [c[1] for c in calls] == sorted(c[1] for c in calls)


def test_upload_files_reports_progress_per_chunk_and_starts_from_what_storage_has(tmp_path, monkeypatch):
    client = make_client()
    monkeypatch.setattr("forestsens.client.MULTIPART_CHUNK_SIZE_BYTES", 10)
    local_file = tmp_path / "big.tif"
    local_file.write_bytes(b"0123456789" * 3)
    created = {
        "id": "upload-1",
        "files": [{"id": "file-1", "filename": "big.tif", "upload_url": "https://par.example/big.tif"}],
    }
    started = {
        "multipart_upload_id": "mp-1",
        "chunk_size_bytes": 10,
        "parts": [{"part_number": 1, "etag": "e1", "size_bytes": 10}],
    }
    calls = []
    with patch("requests.Session.request") as mock_request, patch("requests.Session.put") as mock_put:
        mock_request.side_effect = [
            envelope_response(data=created),
            envelope_response(data=started),
            envelope_response(data={"id": "upload-1", "status": "ready"}),
            envelope_response(data={"id": "upload-1", "status": "ready"}),
        ]
        mock_put.return_value = envelope_response(data={"part_number": 2, "etag": "e2", "size_bytes": 10})
        client.upload_files([local_file], on_progress=lambda *a: calls.append(a))

    assert calls == [("big.tif", 10, 30), ("big.tif", 20, 30), ("big.tif", 30, 30)]


def test_upload_files_without_a_callback_stays_silent_and_unchanged(tmp_path):
    client = make_client()
    local_file = tmp_path / "a.jpg"
    local_file.write_bytes(b"abc")
    created = {"id": "u", "files": [{"id": "f", "filename": "a.jpg", "upload_url": "https://par.example/a"}]}
    with patch("requests.Session.request") as mock_request, patch("requests.put") as mock_put:
        mock_request.side_effect = [envelope_response(data=created), envelope_response(data={"id": "u"})]
        mock_put.return_value = MagicMock(ok=True, status_code=200)
        client.upload_files([local_file])
    # Without a callback the file object is passed straight to requests, as before.
    assert not hasattr(mock_put.call_args.kwargs["data"], "_report")


def test_run_passes_on_upload_progress_to_the_upload(tmp_path):
    client = make_client()
    local_file = tmp_path / "a.jpg"
    local_file.write_bytes(b"abc")
    callback = lambda name, loaded, total: None  # noqa: E731
    with patch.object(client, "upload_files", return_value="upload-1") as mock_upload, patch.object(
        client, "create_batch", return_value=Batch({"id": "b", "status": "queued"})
    ), patch.object(client, "wait_for_batch", return_value=Batch({"id": "b", "status": "complete"})), patch.object(
        client, "download_artifacts", return_value=[]
    ):
        client.run("p", local_file, tmp_path / "d", on_upload_progress=callback)
    mock_upload.assert_called_once_with(
        local_file, name=None, on_progress=callback, project=None, private=False
    )


# -- projects, visibility and sharing ----------------------------------

PROJECT_ID = "11111111-1111-1111-1111-111111111111"
OTHER_PROJECT_ID = "22222222-2222-2222-2222-222222222222"
ANNA_ID = "33333333-3333-3333-3333-333333333333"
BEN_ID = "44444444-4444-4444-4444-444444444444"
PROJECTS = [
    {"id": PROJECT_ID, "name": "Storm 2026", "dataset_count": 3, "can_manage": True},
    {"id": OTHER_PROJECT_ID, "name": "Wheel ruts", "dataset_count": 1, "can_manage": True},
]
MEMBERS = [
    {"user_id": ANNA_ID, "email": "anna@example.com", "display_name": "Anna", "is_me": False},
    {"user_id": BEN_ID, "email": "ben@example.com", "display_name": None, "is_me": True},
]


def test_projects_are_listed_created_renamed_and_deleted():
    client = make_client()
    with patch("requests.Session.request") as mock_request:
        mock_request.side_effect = [
            envelope_response(data=PROJECTS),
            envelope_response(data=PROJECTS[0]),
            envelope_response(data={**PROJECTS[0], "name": "Storm"}),
            envelope_response(data=None),
        ]
        projects = client.list_projects()
        created = client.create_project("Storm 2026")
        renamed = client.rename_project(created, "Storm")
        client.delete_project(PROJECT_ID)

    assert [p.name for p in projects] == ["Storm 2026", "Wheel ruts"]
    assert created.id == PROJECT_ID and renamed.name == "Storm"
    calls = [(c.args[0], c.args[1].split("/v1-key")[1]) for c in mock_request.call_args_list]
    assert calls == [
        ("GET", "/projects"),
        ("POST", "/projects"),
        ("PATCH", f"/projects/{PROJECT_ID}"),
        ("DELETE", f"/projects/{PROJECT_ID}"),
    ]


def test_a_project_can_be_given_as_object_id_or_name():
    client = make_client()
    with patch("requests.Session.request", return_value=envelope_response(data=PROJECTS)):
        assert client._project_id(PROJECTS[0]) == PROJECT_ID
        assert client._project_id(OTHER_PROJECT_ID) == OTHER_PROJECT_ID
        assert client._project_id("wheel RUTS") == OTHER_PROJECT_ID
        with pytest.raises(LookupError, match="Storm 2026"):
            client._project_id("nope")


def test_upload_files_sends_project_and_private():
    client = make_client()
    created = {"id": "u", "files": []}
    with patch("requests.Session.request") as mock_request, patch.object(
        client, "_resolve_paths", return_value=[]
    ):
        mock_request.side_effect = [envelope_response(data=created), envelope_response(data={"id": "u"})]
        client.upload_files(["x"], project=PROJECTS[0], private=True)
    body = mock_request.call_args_list[0].kwargs["json"]
    assert body["project_id"] == PROJECT_ID and body["visibility"] == "private"


def test_upload_files_default_sends_neither_project_nor_visibility():
    client = make_client()
    with patch("requests.Session.request") as mock_request, patch.object(
        client, "_resolve_paths", return_value=[]
    ):
        mock_request.side_effect = [envelope_response(data={"id": "u", "files": []}), envelope_response(data={})]
        client.upload_files(["x"])
    body = mock_request.call_args_list[0].kwargs["json"]
    assert "project_id" not in body and "visibility" not in body


def test_run_passes_project_and_private_to_the_upload(tmp_path):
    client = make_client()
    f = tmp_path / "a.jpg"
    f.write_bytes(b"abc")
    with patch.object(client, "upload_files", return_value="u") as mock_upload, patch.object(
        client, "create_batch", return_value=Batch({"id": "b", "status": "queued"})
    ), patch.object(client, "wait_for_batch", return_value=Batch({"id": "b", "status": "complete"})), patch.object(
        client, "download_artifacts", return_value=[]
    ):
        client.run("p", f, tmp_path / "d", project="Storm 2026", private=True)
    assert mock_upload.call_args.kwargs["project"] == "Storm 2026"
    assert mock_upload.call_args.kwargs["private"] is True


def test_list_uploads_filters_and_returns_the_next_cursor():
    client = make_client()
    upload = {"id": "u1", "name": "Flight", "visibility": "private", "project_id": PROJECT_ID}
    resp = MagicMock(ok=True, status_code=200)
    resp.json.return_value = {"data": [upload], "meta": {"pagination": {"next_cursor": "c2"}}, "error": None}
    with patch("requests.Session.get", return_value=resp) as mock_get, patch(
        "requests.Session.request", return_value=envelope_response(data=PROJECTS)
    ):
        uploads, cursor = client.list_uploads(project="Storm 2026", created_by="me")
        client.list_uploads(no_project=True)
    assert uploads[0].visibility == "private" and cursor == "c2"
    first, second = (c.kwargs["params"] for c in mock_get.call_args_list)
    assert first["project_id"] == PROJECT_ID and first["created_by"] == "me"
    assert second["project_id"] == "none"


def test_update_upload_only_sends_what_is_passed_and_none_removes_the_project():
    client = make_client()
    with patch("requests.Session.request", return_value=envelope_response(data={"id": "u"})) as mock_request:
        client.update_upload("u", private=True)
        client.update_upload("u", project=None)
        client.update_upload("u", name="New", project=PROJECT_ID, private=False)
    bodies = [c.kwargs["json"] for c in mock_request.call_args_list]
    assert bodies == [
        {"visibility": "private"},
        {"project_id": None},
        {"name": "New", "project_id": PROJECT_ID, "visibility": "account"},
    ]


def test_set_upload_shares_accepts_ids_members_and_emails_and_replaces_the_list():
    client = make_client()
    shared = [{"user_id": ANNA_ID, "email": "anna@example.com", "display_name": "Anna"}]
    with patch("requests.Session.request") as mock_request:
        mock_request.side_effect = [
            envelope_response(data=MEMBERS),  # e-mail lookup, once
            envelope_response(data=shared),
        ]
        result = client.set_upload_shares("u", [BEN_ID, "Anna@Example.com", MEMBERS[0]])
    put = mock_request.call_args_list[1]
    assert put.args[0] == "PUT" and put.args[1].endswith("/uploads/u/shares")
    assert put.kwargs["json"] == {"user_ids": [BEN_ID, ANNA_ID, ANNA_ID]}
    assert result[0].email == "anna@example.com" 


def test_set_upload_shares_rejects_an_unknown_email():
    client = make_client()
    with patch("requests.Session.request", return_value=envelope_response(data=MEMBERS)):
        with pytest.raises(LookupError, match="nobody@example.com"):
            client.set_upload_shares("u", ["nobody@example.com"])


def test_members_and_shares_are_listed():
    client = make_client()
    with patch("requests.Session.request") as mock_request:
        mock_request.side_effect = [envelope_response(data=MEMBERS), envelope_response(data=MEMBERS[:1])]
        members = client.list_members()
        shares = client.get_upload_shares("u")
    assert [m.email for m in members] == ["anna@example.com", "ben@example.com"]
    assert shares[0].user_id == ANNA_ID


# -- pipelines -------------------------------------------------


def test_list_pipelines_returns_a_simple_printable_view():
    client = make_client()
    raw = {
        "id": "p1",
        "account_id": None,
        "sens_id": "s1",
        "name": "Orthophoto (fast)",
        "description": "Fast orthomosaic",
        "graph": [{"step": "odm_orthomosaic_fast@1.0.0"}],
        "created_at": "2026-01-01T00:00:00Z",
    }
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=[raw])
        pipelines = client.list_pipelines()

    assert len(pipelines) == 1
    pipeline = pipelines[0]
    assert isinstance(pipeline, Pipeline)
    assert isinstance(pipeline, dict)  # every existing dict-based caller keeps working
    assert pipeline["id"] == "p1"  # bracket access
    assert pipeline.id == "p1" and pipeline.name == "Orthophoto (fast)"  # attribute access
    assert pipeline["graph"] == raw["graph"]  # full payload still reachable
    assert "graph" not in repr(pipeline) and "graph" not in str(pipeline)  # but not in the display
    assert json.loads(json.dumps(pipeline)) == raw  # still a real, serializable dict


def test_find_pipeline_returns_the_matching_one():
    client = make_client()
    raw = [{"id": "p1", "name": "Orthophoto (fast)"}, {"id": "p2", "name": "Wheel rut assessment"}]
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=raw)
        pipeline = client.find_pipeline("Wheel rut assessment")
    assert pipeline.id == "p2"


def test_find_pipeline_raises_with_available_names_when_not_found():
    client = make_client()
    raw = [{"id": "p1", "name": "Orthophoto (fast)"}]
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=raw)
        with pytest.raises(LookupError, match="Orthophoto \\(fast\\)"):
            client.find_pipeline("Nonexistent Pipeline")


def test_find_pipeline_raises_when_the_name_is_ambiguous():
    client = make_client()
    raw = [{"id": "p1", "name": "Dup"}, {"id": "p2", "name": "Dup"}]
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=raw)
        with pytest.raises(LookupError, match="matches more than one"):
            client.find_pipeline("Dup")


def test_find_pipeline_matches_a_case_insensitive_part_of_the_name():
    client = make_client()
    raw = [
        {"id": "a472746b-0000-0000-0000-000000000000", "name": "Canopy cover (DetecTree)"},
        {"id": "7c62f609-0000-0000-0000-000000000000", "name": "Individual tree crowns (detectree2)"},
    ]
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=raw)
        assert client.find_pipeline("canopy").id.startswith("a472746b")
        assert client.find_pipeline("DETECTREE2").id.startswith("7c62f609")


def test_find_pipeline_matches_the_start_of_the_id():
    client = make_client()
    raw = [
        {"id": "a472746b-0000-0000-0000-000000000000", "name": "Canopy cover (DetecTree)"},
        {"id": "7c62f609-0000-0000-0000-000000000000", "name": "Individual tree crowns (detectree2)"},
    ]
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=raw)
        assert client.find_pipeline("7c62f609").name == "Individual tree crowns (detectree2)"


def test_find_pipeline_matches_the_short_id_and_prints_it():
    client = make_client()
    raw = [
        {"id": "a472746b-0000-0000-0000-000000000000", "short_id": "k3x9", "name": "Canopy cover (DetecTree)"},
        {"id": "7c62f609-0000-0000-0000-000000000000", "short_id": "m7q2", "name": "Individual tree crowns (detectree2)"},
    ]
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=raw)
        pipeline = client.find_pipeline("K3X9")
    assert pipeline.name == "Canopy cover (DetecTree)"
    assert repr(pipeline) == "Pipeline(short_id='k3x9', name='Canopy cover (DetecTree)')"
    assert str(pipeline) == "Canopy cover (DetecTree) [k3x9]"


def test_find_pipeline_falls_back_to_the_uuid_prefix_when_there_is_no_short_id():
    client = make_client()
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(
            data=[{"id": "a472746b-0000-0000-0000-000000000000", "name": "Old"}]
        )
        pipeline = client.find_pipeline("a472746b")
    assert pipeline.short_id == "a472746b"


def test_find_pipeline_exact_name_beats_a_partial_match():
    client = make_client()
    raw = [
        {"id": "p1", "name": "Tree"},
        {"id": "p2", "name": "Tree species detector"},
    ]
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=raw)
        assert client.find_pipeline("Tree").id == "p1"


def test_find_pipeline_partial_match_that_hits_several_pipelines_lists_them():
    client = make_client()
    raw = [
        {"id": "p1", "name": "Orthophoto (fast)"},
        {"id": "p2", "name": "Orthophoto (highres)"},
    ]
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=raw)
        with pytest.raises(LookupError, match="Orthophoto \\(fast\\).*Orthophoto \\(highres\\)"):
            client.find_pipeline("ortho")


# -- batches -------------------------------------------------


def test_create_batch_posts_pipeline_and_inputs():
    client = make_client()
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data={"id": "batch-1", "status": "queued"})
        batch = client.create_batch("pipeline-1", [{"slot": "input", "upload_id": "upload-1"}])
    assert batch["id"] == "batch-1"
    sent_json = mock_request.call_args.kwargs["json"]
    assert sent_json == {"pipeline_id": "pipeline-1", "inputs": [{"slot": "input", "upload_id": "upload-1"}]}


def test_get_batch_and_create_batch_return_a_friendly_batch_view():
    client = make_client()
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(
            data={"id": "batch-1", "status": "running", "steps": [{"id": "s1"}]}
        )
        batch = client.get_batch("batch-1")

    assert isinstance(batch, Batch) and isinstance(batch, dict)
    assert batch.id == "batch-1" and batch.status == "running"  # attribute access
    assert batch["steps"] == [{"id": "s1"}]  # full payload still reachable
    assert repr(batch) == "Batch(id='batch-1', status='running')"
    assert str(batch) == "batch-1 (running)"


def test_wait_for_batch_calls_on_progress_once_per_status_change():
    client = make_client()
    seen: list[str] = []
    with patch("requests.Session.request") as mock_request, patch("time.sleep"):
        mock_request.side_effect = [
            envelope_response(data={"id": "batch-1", "status": "queued"}),
            envelope_response(data={"id": "batch-1", "status": "running"}),
            envelope_response(data={"id": "batch-1", "status": "running"}),  # no change -- no callback
            envelope_response(data={"id": "batch-1", "status": "complete"}),
        ]
        client.wait_for_batch("batch-1", poll_interval=0.01, on_progress=lambda b: seen.append(b["status"]))
    assert seen == ["queued", "running", "complete"]


def test_wait_for_batch_polls_until_complete():
    """Real bug, found 2026-09-15: this test's own mock used to fabricate
    "done" as the terminal status -- a value the real API never actually
    returns (Batch.status is only ever "queued"/"running"/"complete"/
    "failed") -- so it validated a status this API doesn't produce, not
    the real contract, and passed while wait_for_batch was completely
    broken for real use.
    """
    client = make_client()
    with patch("requests.Session.request") as mock_request, patch("time.sleep") as mock_sleep:
        mock_request.side_effect = [
            envelope_response(data={"id": "batch-1", "status": "running"}),
            envelope_response(data={"id": "batch-1", "status": "running"}),
            envelope_response(data={"id": "batch-1", "status": "complete"}),
        ]
        batch = client.wait_for_batch("batch-1", poll_interval=0.01)
    assert batch["status"] == "complete"
    assert mock_sleep.call_count == 2


def test_wait_for_batch_raises_on_failure():
    client = make_client()
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(
            data={"id": "batch-1", "status": "failed", "error_message": "step 2 blew up", "steps": []}
        )
        with pytest.raises(BatchFailedError) as exc_info:
            client.wait_for_batch("batch-1", poll_interval=0.01)
    assert exc_info.value.message == "step 2 blew up"
    assert exc_info.value.batch["status"] == "failed"


def test_list_batches_returns_batches_and_the_next_cursor():
    client = make_client()
    with patch("requests.Session.get") as mock_get:
        mock_get.return_value = envelope_response(
            data=[{"id": "b1", "status": "complete"}, {"id": "b2", "status": "running"}],
            pagination={"next_cursor": "c2"},
        )
        batches, next_cursor = client.list_batches(limit=2)
    assert [b.id for b in batches] == ["b1", "b2"]
    assert next_cursor == "c2"
    sent_params = mock_get.call_args.kwargs["params"]
    assert sent_params == {"sort": "created", "direction": "desc", "limit": 2}  # None fields omitted


def test_list_batches_passes_through_every_filter():
    client = make_client()
    with patch("requests.Session.get") as mock_get:
        mock_get.return_value = envelope_response(data=[], pagination={"next_cursor": None})
        client.list_batches(status="failed", upload_id="u1", created_by="me", cursor="c1")
    sent_params = mock_get.call_args.kwargs["params"]
    assert sent_params == {
        "status": "failed",
        "upload_id": "u1",
        "created_by": "me",
        "sort": "created",
        "direction": "desc",
        "limit": 20,
        "cursor": "c1",
    }


# -- run() convenience -------------------------------------------------


def test_run_uploads_creates_a_batch_waits_and_downloads_in_one_call(tmp_path):
    client = make_client()
    local_file = tmp_path / "flight1.jpg"
    local_file.write_bytes(b"tiny file contents")
    dest_dir = tmp_path / "downloads"

    with patch.object(client, "upload_files", return_value="upload-1") as mock_upload, patch.object(
        client, "create_batch", return_value=Batch({"id": "batch-1", "status": "queued"})
    ) as mock_create, patch.object(
        client, "wait_for_batch", return_value=Batch({"id": "batch-1", "status": "complete"})
    ) as mock_wait, patch.object(
        client, "download_artifacts", return_value=["downloads/a.tif"]
    ) as mock_download:
        paths = client.run("pipeline-1", local_file, dest_dir, on_progress=print)

    assert paths == ["downloads/a.tif"]
    mock_upload.assert_called_once_with(
        local_file, name=None, on_progress=None, project=None, private=False
    )
    mock_create.assert_called_once_with("pipeline-1", [{"slot": "input", "upload_id": "upload-1"}])
    mock_wait.assert_called_once_with("batch-1", poll_interval=5.0, timeout=None, on_progress=print)
    mock_download.assert_called_once_with("batch-1", dest_dir)


def test_run_accepts_a_pipeline_dict_as_well_as_a_bare_id():
    client = make_client()
    with patch.object(client, "upload_files", return_value="upload-1"), patch.object(
        client, "create_batch", return_value=Batch({"id": "batch-1", "status": "queued"})
    ) as mock_create, patch.object(
        client, "wait_for_batch", return_value=Batch({"id": "batch-1", "status": "complete"})
    ), patch.object(client, "download_artifacts", return_value=[]):
        client.run(Pipeline({"id": "pipeline-1", "name": "Orthophoto (fast)"}), "f.jpg", "dest")
    mock_create.assert_called_once_with("pipeline-1", [{"slot": "input", "upload_id": "upload-1"}])


# -- downloads -------------------------------------------------


def test_download_artifacts_streams_each_artifact_to_dest_dir(tmp_path):
    client = make_client()
    artifacts = [
        {"id": "artifact-1234abcd", "output_type": "results_zip", "format": "zip", "download_url": "https://par.example/a1"},
    ]

    with patch("requests.Session.get") as mock_session_get, patch("requests.get") as mock_get:
        mock_session_get.return_value = envelope_response(data=artifacts, pagination={"next_cursor": None})
        download_resp = MagicMock()
        download_resp.__enter__.return_value = download_resp
        download_resp.raise_for_status.return_value = None
        download_resp.iter_content.return_value = [b"file", b"bytes"]
        mock_get.return_value = download_resp

        written = client.download_artifacts("batch-1", tmp_path)

    assert len(written) == 1
    local_path = tmp_path / "results_zip_artifact.zip"
    assert local_path.exists()
    assert local_path.read_bytes() == b"filebytes"


def test_gateway_host_may_be_given_with_a_scheme_and_trailing_slash():
    for given in ("h.example.com", "https://h.example.com", "https://h.example.com/", "HTTPS://h.example.com/"):
        client = Client(gateway_host=given, api_key="fs_x")
        assert client.gateway_host == "h.example.com"
        assert client._url("/v1/pipelines") == "https://h.example.com/v1-key/pipelines"


# -- pipeline versions ---------------------------------------------------------


def test_create_batch_sends_use_latest_version_only_when_asked():
    client = make_client()
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data={"id": "batch-1", "status": "queued"})
        client.create_batch("pipeline-1", [{"slot": "input", "upload_id": "u"}], use_latest_version=True)
    assert mock_request.call_args.kwargs["json"]["use_latest_version"] is True


def test_create_batch_warns_when_a_newer_version_exists():
    import warnings

    from forestsens import ForestSensNewerVersionWarning

    client = make_client()
    newer = {"id": "p-2", "short_id": "k3x9", "version": 2, "name": "Canopy"}
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(
            data={"id": "batch-1", "status": "queued", "pipeline_id": "p-1", "newer_pipeline_version": newer}
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            batch = client.create_batch("p-1", [{"slot": "input", "upload_id": "u"}])
    assert batch["id"] == "batch-1"
    assert any(issubclass(w.category, ForestSensNewerVersionWarning) and "k3x9" in str(w.message) for w in caught)


def test_no_warning_on_the_newest_version():
    import warnings

    client = make_client()
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data={"id": "b", "status": "queued", "newer_pipeline_version": None})
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            client.create_batch("p", [{"slot": "input", "upload_id": "u"}])


def test_run_passes_use_latest(tmp_path):
    client = make_client()
    with patch.object(client, "upload_files", return_value="upload-1"), patch.object(
        client, "create_batch", return_value=Batch({"id": "batch-1"})
    ) as create, patch.object(client, "wait_for_batch", return_value=Batch({"id": "batch-1"})), patch.object(
        client, "download_artifacts", return_value=[]
    ):
        client.run("pipeline-1", str(tmp_path), tmp_path, use_latest=True)
    create.assert_called_once_with("pipeline-1", [{"slot": "input", "upload_id": "upload-1"}], use_latest_version=True)


def test_list_pipelines_flags_and_at_n_lookup():
    client = make_client()
    rows = [
        {"id": "p-1", "short_id": "k3x9", "name": "Canopy", "version": 1},
        {"id": "p-2", "short_id": "k3x9@2", "name": "Canopy", "version": 2},
    ]
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=rows)
        client.list_pipelines(include_retired=True, include_drafts=True)
        assert mock_request.call_args.kwargs["params"] == {"include_retired": "true", "include_drafts": "true"}
        found = client.find_pipeline("k3x9@2")
        assert mock_request.call_args.kwargs["params"] == {"include_retired": "true"}
    assert found["id"] == "p-2" and repr(found) == "Pipeline(short_id='k3x9@2', name='Canopy', version=2)"


def test_find_pipeline_at_1_is_the_first_version():
    client = make_client()
    rows = [
        {"id": "p-1", "short_id": "k3x9", "name": "Canopy", "version": 1},
        {"id": "p-2", "short_id": "k3x9@2", "name": "Canopy", "version": 2},
    ]
    with patch("requests.Session.request") as mock_request:
        mock_request.return_value = envelope_response(data=rows)
        assert client.find_pipeline("k3x9@1")["id"] == "p-1"


# -- parallel uploads ----------------------------------------------------


def _created_for(names):
    return {
        "id": "upload-1",
        "files": [
            {"id": f"file-{i}", "filename": n, "upload_url": f"https://par.example/{n}"} for i, n in enumerate(names)
        ],
    }


def test_upload_files_sends_small_files_in_parallel(tmp_path):
    import threading
    import time as _time

    names = [f"img{i}.jpg" for i in range(8)]
    for n in names:
        (tmp_path / n).write_bytes(b"x" * 10)
    active, peak, lock = [0], [0], threading.Lock()

    def slow_put(url, data=None, **kwargs):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        _time.sleep(0.05)
        with lock:
            active[0] -= 1
        return MagicMock(ok=True, status_code=200)

    with patch("requests.Session.request") as mock_request, patch("requests.put", side_effect=slow_put) as mock_put:
        mock_request.side_effect = [envelope_response(data=_created_for(names)), envelope_response(data={"id": "upload-1"})]
        make_client().upload_files(tmp_path, concurrency=4)

    assert mock_put.call_count == 8
    assert peak[0] == 4  # never more than `concurrency` at once, and really in parallel
    assert mock_request.call_args_list[-1].args[1].endswith("/uploads/upload-1/complete")


def test_upload_files_concurrency_one_is_one_after_another(tmp_path):
    import threading

    names = ["a.jpg", "b.jpg", "c.jpg"]
    for n in names:
        (tmp_path / n).write_bytes(b"x")
    active, peak, lock = [0], [0], threading.Lock()

    def put(url, data=None, **kwargs):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        with lock:
            active[0] -= 1
        return MagicMock(ok=True, status_code=200)

    with patch("requests.Session.request") as mock_request, patch("requests.put", side_effect=put):
        mock_request.side_effect = [envelope_response(data=_created_for(names)), envelope_response(data={"id": "upload-1"})]
        make_client().upload_files(tmp_path, concurrency=1)
    assert peak[0] == 1


def test_a_file_that_keeps_failing_raises_and_the_dataset_is_not_completed(tmp_path, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)  # skip the retry backoff
    names = ["good.jpg", "bad.jpg"]
    for n in names:
        (tmp_path / n).write_bytes(b"x")

    def put(url, data=None, **kwargs):
        return MagicMock(ok=not url.endswith("bad.jpg"), status_code=200 if not url.endswith("bad.jpg") else 500)

    with patch("requests.Session.request") as mock_request, patch("requests.put", side_effect=put):
        mock_request.side_effect = [envelope_response(data=_created_for(names))]
        with pytest.raises(ForestSensAPIError):
            make_client().upload_files(tmp_path)
    assert mock_request.call_count == 1  # created, never completed


def test_progress_callbacks_from_parallel_uploads_never_overlap(tmp_path):
    import threading
    import time as _time

    names = [f"img{i}.jpg" for i in range(6)]
    for n in names:
        (tmp_path / n).write_bytes(b"x" * 1000)
    inside, overlapped, seen = [0], [False], set()

    def on_progress(name, loaded, total):
        inside[0] += 1
        if inside[0] > 1:
            overlapped[0] = True
        _time.sleep(0.005)
        seen.add(name)
        inside[0] -= 1

    def put(url, data=None, **kwargs):
        while data.read(256):
            pass
        return MagicMock(ok=True, status_code=200)

    with patch("requests.Session.request") as mock_request, patch("requests.put", side_effect=put):
        mock_request.side_effect = [envelope_response(data=_created_for(names)), envelope_response(data={"id": "upload-1"})]
        make_client().upload_files(tmp_path, on_progress=on_progress)
    assert not overlapped[0] and seen == set(names)
