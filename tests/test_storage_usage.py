"""Tests for storage_usage.py"""

import os
import socket
import stat
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

from storage_usage import (
    Base,
    FileRecord,
    PrefixRecord,
    _is_under_complete_prefix,
    _upsert_file,
    _upsert_prefix,
    apply_nice_ionice,
    build_parser,
    file_kind,
    format_size,
    generate_summary_html,
    load_complete_prefixes,
    main,
    resolve_uid,
    scan,
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
    def test_current_user(self):
        assert resolve_uid(None) == os.getuid()

    def test_unknown_user_exits(self):
        with pytest.raises(SystemExit):
            resolve_uid("__nonexistent_user_xyz__")


# ---------------------------------------------------------------------------
# Unit tests – database helpers
# ---------------------------------------------------------------------------

HOST = "testhost.example.com"


class TestUpsertFile:
    def test_insert(self, session):
        _upsert_file(session, HOST, "/tmp/x.txt", 100, "file")
        session.commit()
        rec = session.get(FileRecord, (HOST, "/tmp/x.txt"))
        assert rec is not None
        assert rec.size == 100
        assert rec.kind == "file"
        assert rec.host_id == HOST

    def test_update(self, session):
        _upsert_file(session, HOST, "/tmp/x.txt", 100, "file")
        session.commit()
        _upsert_file(session, HOST, "/tmp/x.txt", 200, "file")
        session.commit()
        rec = session.get(FileRecord, (HOST, "/tmp/x.txt"))
        assert rec.size == 200


class TestUpsertPrefix:
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
        assert args.nice is None
        assert args.ionice_class is None
        assert args.ionice_level is None


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
