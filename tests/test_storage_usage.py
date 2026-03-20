"""Tests for storage_usage.py"""

import json
import os
import socket
import stat
import textwrap
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

from storage_usage import (
    Base,
    FileRecord,
    PrefixRecord,
    UserRecord,
    _is_under_complete_prefix,
    _upsert_file,
    _upsert_prefix,
    _upsert_user,
    apply_nice_ionice,
    build_parser,
    file_kind,
    format_size,
    format_tree_report,
    generate_directory_tree,
    generate_summary_html,
    load_complete_prefixes,
    main,
    resolve_uid,
    scan,
    serve_db,
    update_prefix_sizes,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine():
    """In-memory SQLite engine for testing."""
    eng = sa.create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture()
def session(engine):
    with Session(engine) as s:
        yield s


@pytest.fixture()
def tmp_tree(tmp_path):
    """Create a small directory tree owned by the current process.

    Layout::

        tmp_path/
            a.txt   (10 bytes)
            sub/
                b.txt  (20 bytes)
                c.txt  (30 bytes)
            empty_dir/
    """
    (tmp_path / "a.txt").write_bytes(b"0" * 10)
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.txt").write_bytes(b"0" * 20)
    (sub / "c.txt").write_bytes(b"0" * 30)
    (tmp_path / "empty_dir").mkdir()
    return tmp_path


# ---------------------------------------------------------------------------
# Unit tests – helpers
# ---------------------------------------------------------------------------


class TestFormatSize:
    def test_bytes(self):
        assert format_size(512) == "512.0 B"

    def test_kilobytes(self):
        assert format_size(1024) == "1.0 KB"

    def test_megabytes(self):
        assert format_size(1024 * 1024) == "1.0 MB"

    def test_zero(self):
        assert format_size(0) == "0.0 B"

    def test_fractional_kilobytes(self):
        # 1536 bytes = 1.5 KB — integer division would lose the fraction
        assert format_size(1536) == "1.5 KB"


class TestFileKind:
    def test_regular_file(self, tmp_path):
        f = tmp_path / "f.txt"
        f.write_text("hello")
        mode = f.stat().st_mode
        assert file_kind(mode) == "file"

    def test_directory(self, tmp_path):
        mode = tmp_path.stat().st_mode
        assert file_kind(mode) == "directory"

    def test_symlink(self, tmp_path):
        target = tmp_path / "target.txt"
        target.write_text("x")
        link = tmp_path / "link"
        link.symlink_to(target)
        mode = link.lstat().st_mode
        assert file_kind(mode) == "symlink"


class TestIsUnderCompletePrefix:
    def test_exact_match(self):
        assert _is_under_complete_prefix("/a/b", {"/a/b"})

    def test_nested(self):
        assert _is_under_complete_prefix("/a/b/c", {"/a/b"})

    def test_no_match(self):
        assert not _is_under_complete_prefix("/a/b2", {"/a/b"})

    def test_empty_set(self):
        assert not _is_under_complete_prefix("/a/b", set())


class TestResolveUid:
    def test_no_user_returns_none(self):
        assert resolve_uid(None) is None

    def test_unknown_user_exits(self):
        with pytest.raises(SystemExit):
            resolve_uid("__nonexistent_user_xyz__")


# ---------------------------------------------------------------------------
# Unit tests – database helpers
# ---------------------------------------------------------------------------

HOST = "testhost.example.com"


class TestUpsertFile:
    def test_insert(self, session):
        uid = os.getuid()
        _upsert_user(session, uid)
        _upsert_file(session, HOST, "/tmp/x.txt", 100, "file", uid)
        session.commit()
        rec = session.get(FileRecord, (HOST, "/tmp/x.txt"))
        assert rec is not None
        assert rec.size == 100
        assert rec.kind == "file"
        assert rec.host_id == HOST
        assert rec.uid == uid

    def test_update(self, session):
        uid = os.getuid()
        _upsert_user(session, uid)
        _upsert_file(session, HOST, "/tmp/x.txt", 100, "file", uid)
        session.commit()
        _upsert_file(session, HOST, "/tmp/x.txt", 200, "file", uid)
        session.commit()
        rec = session.get(FileRecord, (HOST, "/tmp/x.txt"))
        assert rec.size == 200


class TestUpsertUser:
    def test_insert(self, session):
        uid = os.getuid()
        _upsert_user(session, uid)
        session.commit()
        rec = session.get(UserRecord, uid)
        assert rec is not None
        assert rec.uid == uid
        assert rec.username  # non-empty

    def test_insert_unknown_uid_uses_str(self, session):
        _upsert_user(session, 999999)
        session.commit()
        rec = session.get(UserRecord, 999999)
        assert rec is not None
        assert rec.username == "999999"

    def test_idempotent(self, session):
        uid = os.getuid()
        _upsert_user(session, uid)
        _upsert_user(session, uid)
        session.commit()
        count = session.query(UserRecord).filter(UserRecord.uid == uid).count()
        assert count == 1



    def test_insert(self, session):
        _upsert_prefix(session, HOST, "/tmp", 500, complete=True)
        session.commit()
        rec = session.get(PrefixRecord, (HOST, "/tmp"))
        assert rec is not None
        assert rec.complete is True
        assert rec.host_id == HOST

    def test_update(self, session):
        _upsert_prefix(session, HOST, "/tmp", 500, complete=False)
        session.commit()
        _upsert_prefix(session, HOST, "/tmp", 999, complete=True)
        session.commit()
        rec = session.get(PrefixRecord, (HOST, "/tmp"))
        assert rec.size == 999
        assert rec.complete is True


class TestLoadCompletePrefixes:
    def test_empty(self, session):
        assert load_complete_prefixes(session, HOST) == set()

    def test_returns_only_complete(self, session):
        session.add(PrefixRecord(host_id=HOST, prefix="/a", size=0, complete=True))
        session.add(PrefixRecord(host_id=HOST, prefix="/b", size=0, complete=False))
        session.commit()
        result = load_complete_prefixes(session, HOST)
        assert result == {"/a"}

    def test_filters_by_host(self, session):
        session.add(PrefixRecord(host_id="other.host", prefix="/a", size=0, complete=True))
        session.add(PrefixRecord(host_id=HOST, prefix="/b", size=0, complete=True))
        session.commit()
        result = load_complete_prefixes(session, HOST)
        assert result == {"/b"}


# ---------------------------------------------------------------------------
# Integration tests – scan
# ---------------------------------------------------------------------------


class TestScan:
    def test_basic_scan(self, tmp_tree, session):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())

        files = session.query(FileRecord).all()
        paths = {f.path for f in files}
        host_ids = {f.host_id for f in files}

        assert str(tmp_tree / "a.txt") in paths
        assert str(tmp_tree / "sub" / "b.txt") in paths
        assert str(tmp_tree / "sub" / "c.txt") in paths
        assert host_ids == {HOST}

    def test_prefix_marked_complete(self, tmp_tree, session):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())

        root_prefix = session.get(PrefixRecord, (HOST, str(tmp_tree)))
        assert root_prefix is not None
        assert root_prefix.complete is True

    def test_skips_complete_prefix(self, tmp_tree, session):
        uid = os.getuid()
        sub = str(tmp_tree / "sub")

        # Mark the sub-directory as already complete.
        session.add(PrefixRecord(host_id=HOST, prefix=sub, size=50, complete=True))
        session.commit()

        complete = load_complete_prefixes(session, HOST)
        scan(tmp_tree, uid, HOST, session, complete)

        # Files inside the skipped prefix must NOT have been (re-)inserted.
        b = session.get(FileRecord, (HOST, str(tmp_tree / "sub" / "b.txt")))
        assert b is None

        # Files outside should still be scanned.
        a = session.get(FileRecord, (HOST, str(tmp_tree / "a.txt")))
        assert a is not None

    def test_wrong_uid_excluded(self, tmp_tree, session):
        # Use a UID that doesn't match anything on disk.
        scan(tmp_tree, uid=999999, host_id=HOST, session=session, complete_prefixes=set())
        files = session.query(FileRecord).all()
        assert files == []

    def test_scan_all_when_uid_is_none(self, tmp_tree, session):
        # uid=None should scan all accessible files regardless of owner.
        scan(tmp_tree, uid=None, host_id=HOST, session=session, complete_prefixes=set())
        files = session.query(FileRecord).all()
        paths = {f.path for f in files}
        assert str(tmp_tree / "a.txt") in paths
        assert str(tmp_tree / "sub" / "b.txt") in paths
        assert str(tmp_tree / "sub" / "c.txt") in paths
        # Each file record should have a uid and a corresponding user row.
        for rec in files:
            user = session.get(UserRecord, rec.uid)
            assert user is not None

    def test_users_inserted_during_scan(self, tmp_tree, session):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())
        user = session.get(UserRecord, uid)
        assert user is not None
        assert user.uid == uid
        assert user.username  # non-empty string

    def test_update_prefix_sizes(self, tmp_tree, session):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())
        update_prefix_sizes(session, HOST)

        root_rec = session.get(PrefixRecord, (HOST, str(tmp_tree)))
        # Recursive size should be >= the sum of all files under root
        assert root_rec.size >= 10 + 20 + 30  # a.txt + b.txt + c.txt


# ---------------------------------------------------------------------------
# Integration tests – HTML summary
# ---------------------------------------------------------------------------


class TestGenerateSummaryHtml:
    def test_creates_file(self, tmp_tree, session, tmp_path):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())
        update_prefix_sizes(session, HOST)

        out = tmp_path / "summary.html"
        generate_summary_html(session, out)

        assert out.exists()
        content = out.read_text()
        assert "Storage Usage Summary" in content
        assert "Total files" in content

    def test_contains_file_paths(self, tmp_tree, session, tmp_path):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())

        out = tmp_path / "summary.html"
        generate_summary_html(session, out)
        content = out.read_text()
        assert "a.txt" in content

    def test_contains_host_id(self, tmp_tree, session, tmp_path):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())

        out = tmp_path / "summary.html"
        generate_summary_html(session, out)
        content = out.read_text()
        assert HOST in content


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------


class TestCLI:
    def test_scan_via_main(self, tmp_tree, tmp_path):
        db_url = f"sqlite:///{tmp_path / 'test.db'}"
        main([str(tmp_tree), "--db", db_url])

        engine = sa.create_engine(db_url)
        with Session(engine) as s:
            files = s.query(FileRecord).all()
        assert len(files) > 0

    def test_users_stored_in_db(self, tmp_tree, tmp_path):
        db_url = f"sqlite:///{tmp_path / 'test.db'}"
        main([str(tmp_tree), "--db", db_url])

        engine = sa.create_engine(db_url)
        with Session(engine) as s:
            users = s.query(UserRecord).all()
        assert len(users) > 0
        for u in users:
            assert u.uid is not None
            assert u.username  # non-empty

    def test_host_id_stored(self, tmp_tree, tmp_path):
        db_url = f"sqlite:///{tmp_path / 'test.db'}"
        main([str(tmp_tree), "--db", db_url])

        engine = sa.create_engine(db_url)
        with Session(engine) as s:
            files = s.query(FileRecord).all()
        fqdn = socket.getfqdn()
        assert all(f.host_id == fqdn for f in files)

    def test_resolved_path_stored(self, tmp_tree, tmp_path):
        db_url = f"sqlite:///{tmp_path / 'test.db'}"
        main([str(tmp_tree), "--db", db_url])

        engine = sa.create_engine(db_url)
        expected_root = str(tmp_tree.resolve())
        with Session(engine) as s:
            prefixes = s.query(PrefixRecord).all()
        assert any(p.prefix == expected_root for p in prefixes)

    def test_summary_html_created(self, tmp_tree, tmp_path):
        db_url = f"sqlite:///{tmp_path / 'test.db'}"
        html_out = tmp_path / "summary.html"
        main([str(tmp_tree), "--db", db_url, "--summary-html", str(html_out)])
        assert html_out.exists()

    def test_parser_defaults(self):
        parser = build_parser()
        args = parser.parse_args(["/some/path"])
        assert args.path == Path("/some/path")
        assert args.user is None
        assert args.db == "sqlite:///storage_usage.db"
        assert args.summary_html is None
        assert args.serve is False
        assert args.port == 8080
        assert args.nice is None
        assert args.ionice_class is None
        assert args.ionice_level is None

    def test_serve_flag_parsed(self):
        parser = build_parser()
        args = parser.parse_args(["--serve", "--port", "9090"])
        assert args.serve is True
        assert args.port == 9090
        assert args.path is None

    def test_missing_path_without_serve_errors(self, capsys):
        with pytest.raises(SystemExit):
            main([])
        captured = capsys.readouterr()
        assert "PATH" in captured.err


# ---------------------------------------------------------------------------
# nice / ionice
# ---------------------------------------------------------------------------


class TestApplyNiceIonice:
    def test_no_op_when_nothing_set(self):
        # Should return without exec-ing.
        apply_nice_ionice(None, None, None)  # no exception

    def test_no_op_when_already_niced(self, monkeypatch):
        monkeypatch.setenv("_STORAGE_USAGE_NICED", "1")
        apply_nice_ionice(19, 3, None)  # should NOT exec

    def test_execvp_called_for_nice(self, monkeypatch):
        monkeypatch.delenv("_STORAGE_USAGE_NICED", raising=False)
        captured = {}

        def fake_execvp(file, args):
            captured["file"] = file
            captured["args"] = args
            raise SystemExit(0)  # stop execution

        monkeypatch.setattr(os, "execvp", fake_execvp)
        with pytest.raises(SystemExit):
            apply_nice_ionice(10, None, None)

        assert captured["file"] == "nice"
        assert "-n10" in captured["args"]

    def test_execvp_called_for_ionice(self, monkeypatch):
        monkeypatch.delenv("_STORAGE_USAGE_NICED", raising=False)
        captured = {}

        def fake_execvp(file, args):
            captured["file"] = file
            captured["args"] = args
            raise SystemExit(0)

        monkeypatch.setattr(os, "execvp", fake_execvp)
        with pytest.raises(SystemExit):
            apply_nice_ionice(None, 3, None)

        assert captured["file"] == "ionice"
        assert "-c3" in captured["args"]

    def test_execvp_ionice_and_nice(self, monkeypatch):
        monkeypatch.delenv("_STORAGE_USAGE_NICED", raising=False)
        captured = {}

        def fake_execvp(file, args):
            captured["file"] = file
            captured["args"] = args
            raise SystemExit(0)

        monkeypatch.setattr(os, "execvp", fake_execvp)
        with pytest.raises(SystemExit):
            apply_nice_ionice(5, 2, 4)

        # ionice comes first
        assert captured["file"] == "ionice"
        assert "-c2" in captured["args"]
        assert "-n4" in captured["args"]
        # nice follows
        assert "nice" in captured["args"]
        assert "-n5" in captured["args"]


# ---------------------------------------------------------------------------
# Web server tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def served_engine(tmp_tree, tmp_path):
    """Engine with scanned data, served on a random port; yields (engine, port)."""
    eng = sa.create_engine(f"sqlite:///{tmp_path / 'serve.db'}")
    Base.metadata.create_all(eng)
    with Session(eng) as s:
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, s, set())
        update_prefix_sizes(s, HOST)
    return eng


def _find_free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def live_server(served_engine):
    """Start serve_db in a daemon thread; yield the port; shut down after test."""
    port = _find_free_port()
    from http.server import HTTPServer

    t = threading.Thread(target=serve_db, args=(served_engine, port), daemon=True)
    t.start()
    # Wait for the server to be ready.
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://localhost:{port}/")
            break
        except Exception:
            time.sleep(0.05)
    yield port


class TestServeDB:
    def test_index_returns_html(self, live_server):
        resp = urllib.request.urlopen(f"http://localhost:{live_server}/")
        assert resp.status == 200
        content_type = resp.headers.get("Content-Type", "")
        assert "text/html" in content_type
        body = resp.read().decode()
        assert "Storage Usage" in body
        assert "Files" in body
        assert "Directories" in body

    def test_api_files_returns_json(self, live_server):
        resp = urllib.request.urlopen(f"http://localhost:{live_server}/api/files")
        assert resp.status == 200
        data = json.loads(resp.read())
        assert "rows" in data
        assert "total" in data
        assert "page" in data
        assert "total_pages" in data
        assert data["total"] > 0
        row = data["rows"][0]
        assert "host_id" in row
        assert "path" in row
        assert "kind" in row
        assert "size" in row

    def test_api_files_ordered_by_size_desc(self, live_server):
        resp = urllib.request.urlopen(f"http://localhost:{live_server}/api/files")
        data = json.loads(resp.read())
        sizes = [r["size"] for r in data["rows"]]
        assert sizes == sorted(sizes, reverse=True)

    def test_api_files_host_filter(self, live_server):
        resp = urllib.request.urlopen(
            f"http://localhost:{live_server}/api/files?host={HOST}"
        )
        data = json.loads(resp.read())
        assert data["total"] > 0
        assert all(HOST in r["host_id"] for r in data["rows"])

    def test_api_files_host_filter_no_match(self, live_server):
        resp = urllib.request.urlopen(
            f"http://localhost:{live_server}/api/files?host=__no_such_host__"
        )
        data = json.loads(resp.read())
        assert data["total"] == 0
        assert data["rows"] == []

    def test_api_files_path_filter(self, live_server):
        resp = urllib.request.urlopen(
            f"http://localhost:{live_server}/api/files?path=a.txt"
        )
        data = json.loads(resp.read())
        assert data["total"] >= 1
        assert all("a.txt" in r["path"] for r in data["rows"])

    def test_api_files_kind_filter(self, live_server):
        resp = urllib.request.urlopen(
            f"http://localhost:{live_server}/api/files?kind=file"
        )
        data = json.loads(resp.read())
        assert data["total"] > 0
        assert all("file" in r["kind"] for r in data["rows"])

    def test_api_files_min_size_filter(self, live_server):
        # c.txt is 30 bytes, b.txt is 20, a.txt is 10.  min_size=25 => only c.txt.
        resp = urllib.request.urlopen(
            f"http://localhost:{live_server}/api/files?min_size=25"
        )
        data = json.loads(resp.read())
        assert all(r["size"] >= 25 for r in data["rows"])

    def test_api_files_pagination(self, live_server):
        resp1 = urllib.request.urlopen(
            f"http://localhost:{live_server}/api/files?per_page=1&page=1"
        )
        resp2 = urllib.request.urlopen(
            f"http://localhost:{live_server}/api/files?per_page=1&page=2"
        )
        d1 = json.loads(resp1.read())
        d2 = json.loads(resp2.read())
        assert len(d1["rows"]) == 1
        assert len(d2["rows"]) == 1
        # Different records on different pages
        assert d1["rows"][0]["path"] != d2["rows"][0]["path"]
        assert d1["total_pages"] >= 2

    def test_api_dirs_returns_json(self, live_server):
        resp = urllib.request.urlopen(f"http://localhost:{live_server}/api/dirs")
        assert resp.status == 200
        data = json.loads(resp.read())
        assert "rows" in data
        assert data["total"] > 0
        row = data["rows"][0]
        assert "host_id" in row
        assert "path" in row
        assert "size" in row
        assert "complete" in row

    def test_api_dirs_ordered_by_size_desc(self, live_server):
        resp = urllib.request.urlopen(f"http://localhost:{live_server}/api/dirs")
        data = json.loads(resp.read())
        sizes = [r["size"] for r in data["rows"]]
        assert sizes == sorted(sizes, reverse=True)

    def test_api_dirs_min_size_filter(self, live_server):
        resp = urllib.request.urlopen(
            f"http://localhost:{live_server}/api/dirs?min_size=1"
        )
        data = json.loads(resp.read())
        assert all(r["size"] >= 1 for r in data["rows"])

    def test_api_dirs_path_filter(self, live_server):
        resp = urllib.request.urlopen(
            f"http://localhost:{live_server}/api/dirs?path=sub"
        )
        data = json.loads(resp.read())
        assert all("sub" in r["path"] for r in data["rows"])

    def test_api_404(self, live_server):
        try:
            urllib.request.urlopen(f"http://localhost:{live_server}/no/such/path")
            assert False, "Expected 404"
        except urllib.error.HTTPError as exc:
            assert exc.code == 404

    def test_html_contains_filter_inputs(self, live_server):
        resp = urllib.request.urlopen(f"http://localhost:{live_server}/")
        body = resp.read().decode()
        assert 'type="range"' in body
        assert "substring" in body


# ---------------------------------------------------------------------------
# Directory tree tests
# ---------------------------------------------------------------------------


class TestGenerateDirectoryTree:
    def test_returns_none_when_no_data(self, session):
        result = generate_directory_tree(session, HOST, max_depth=3)
        assert result is None

    def test_returns_none_for_negative_depth(self, tmp_tree, session):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())
        update_prefix_sizes(session, HOST)
        result = generate_directory_tree(session, HOST, max_depth=-1)
        assert result is None

    def test_basic_tree_all_users(self, tmp_tree, session):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())
        update_prefix_sizes(session, HOST)

        tree = generate_directory_tree(session, HOST, max_depth=3)
        assert tree is not None
        assert tree["path"] == str(tmp_tree)
        assert tree["size"] >= 10 + 20 + 30  # a.txt + b.txt + c.txt
        assert tree["depth"] == 0

    def test_tree_has_children(self, tmp_tree, session):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())
        update_prefix_sizes(session, HOST)

        tree = generate_directory_tree(session, HOST, max_depth=3)
        assert tree is not None
        # The "sub" directory should appear as a child.
        child_paths = [c["path"] for c in tree["children"]]
        assert str(tmp_tree / "sub") in child_paths

    def test_children_sorted_by_size_desc(self, tmp_tree, session):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())
        update_prefix_sizes(session, HOST)

        tree = generate_directory_tree(session, HOST, max_depth=3)
        assert tree is not None
        sizes = [c["size"] for c in tree["children"]]
        assert sizes == sorted(sizes, reverse=True)

    def test_depth_zero_no_children(self, tmp_tree, session):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())
        update_prefix_sizes(session, HOST)

        tree = generate_directory_tree(session, HOST, max_depth=0)
        assert tree is not None
        assert tree["children"] == []

    def test_tree_with_uid_filter(self, tmp_tree, session):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())
        update_prefix_sizes(session, HOST)

        tree = generate_directory_tree(session, HOST, max_depth=3, uid=uid)
        assert tree is not None
        assert tree["size"] >= 10 + 20 + 30

    def test_tree_with_nonexistent_uid_returns_none_or_zero(self, tmp_tree, session):
        scan(tmp_tree, uid=None, host_id=HOST, session=session, complete_prefixes=set())
        update_prefix_sizes(session, HOST)

        # uid=999999 owns no files, so the tree should either be None or have size 0.
        tree = generate_directory_tree(session, HOST, max_depth=3, uid=999999)
        assert tree is None or tree["size"] == 0

    def test_formatted_size_present(self, tmp_tree, session):
        uid = os.getuid()
        scan(tmp_tree, uid, HOST, session, set())
        update_prefix_sizes(session, HOST)

        tree = generate_directory_tree(session, HOST, max_depth=3)
        assert tree is not None
        assert "formatted_size" in tree
        assert isinstance(tree["formatted_size"], str)


class TestFormatTreeReport:
    def _make_tree(self) -> dict:
        return {
            "path": "/data",
            "size": 160 * 1024**3,
            "formatted_size": "160.0 GB",
            "depth": 0,
            "children": [
                {
                    "path": "/data/subdir1",
                    "size": 80 * 1024**3,
                    "formatted_size": "80.0 GB",
                    "depth": 1,
                    "children": [],
                },
                {
                    "path": "/data/subdir2",
                    "size": 80 * 1024**3,
                    "formatted_size": "80.0 GB",
                    "depth": 1,
                    "children": [],
                },
            ],
        }

    def test_no_data_message(self):
        assert format_tree_report(None) == "No data available."

    def test_all_users_header(self):
        report = format_tree_report(self._make_tree())
        assert "All Users" in report

    def test_username_in_header(self):
        report = format_tree_report(self._make_tree(), username="alice")
        assert "User: alice" in report

    def test_root_path_in_report(self):
        report = format_tree_report(self._make_tree())
        assert "/data" in report

    def test_child_paths_in_report(self):
        report = format_tree_report(self._make_tree())
        assert "/data/subdir1" in report
        assert "/data/subdir2" in report

    def test_sizes_in_report(self):
        report = format_tree_report(self._make_tree())
        assert "160.0 GB" in report
        assert "80.0 GB" in report

    def test_children_indented(self):
        report = format_tree_report(self._make_tree())
        lines = report.splitlines()
        child_lines = [l for l in lines if "subdir1" in l or "subdir2" in l]
        assert all(l.startswith("    -") for l in child_lines)


class TestDirectoryTreeCLI:
    def test_directory_tree_flag_parsed(self):
        parser = build_parser()
        args = parser.parse_args(["/some/path", "--directory-tree"])
        assert args.directory_tree is True

    def test_depth_default(self):
        parser = build_parser()
        args = parser.parse_args(["/some/path"])
        assert args.depth == 3

    def test_depth_custom(self):
        parser = build_parser()
        args = parser.parse_args(["/some/path", "--directory-tree", "--depth", "5"])
        assert args.depth == 5

    def test_directory_tree_printed_after_scan(self, tmp_tree, tmp_path, capsys):
        db_url = f"sqlite:///{tmp_path / 'test.db'}"
        main([str(tmp_tree), "--db", db_url, "--directory-tree", "--depth", "2"])
        captured = capsys.readouterr()
        assert str(tmp_tree) in captured.out
        # Formatted size should appear somewhere in the output.
        assert any(unit in captured.out for unit in ("B", "KB", "MB", "GB", "TB"))

    def test_directory_tree_standalone_no_path(self, tmp_tree, tmp_path, capsys):
        db_url = f"sqlite:///{tmp_path / 'test.db'}"
        # First scan to populate the database.
        main([str(tmp_tree), "--db", db_url])
        capsys.readouterr()  # discard scan output
        # Now query the existing database without re-scanning.
        main(["--db", db_url, "--directory-tree", "--depth", "2"])
        captured = capsys.readouterr()
        assert str(tmp_tree) in captured.out

    def test_missing_path_still_errors_without_directory_tree(self, capsys):
        with pytest.raises(SystemExit):
            main([])
        captured = capsys.readouterr()
        assert "PATH" in captured.err
