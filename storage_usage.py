#!/usr/bin/env python3
"""
storage_usage.py – Scan file-system storage and build a summary database.

Usage
-----
storage_usage.py [--user USER] [--db SQLALCHEMY_URL]
                 [--summary-html FILE]
                 [--nice N] [--ionice-class {1,2,3}] [--ionice-level {0..7}]
                 PATH

storage_usage.py --serve [--port PORT] [--db SQLALCHEMY_URL]
"""

import argparse
import json
import os
import pwd
import socket
import stat
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional, Set
from urllib.parse import parse_qs, urlparse

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class UserRecord(Base):
    """One row per unique user (uid/username pair) encountered during scanning."""

    __tablename__ = "users"

    uid: Mapped[int] = mapped_column(sa.Integer, primary_key=True, nullable=False)
    username: Mapped[str] = mapped_column(sa.String, nullable=False)


class FileRecord(Base):
    """One row per file-system entry belonging to the scanned user."""

    __tablename__ = "files"

    host_id: Mapped[str] = mapped_column(sa.String, primary_key=True)
    path: Mapped[str] = mapped_column(sa.String, primary_key=True)
    size: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    kind: Mapped[str] = mapped_column(sa.String, nullable=False)
    uid: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey("users.uid"), nullable=False
    )


class PrefixRecord(Base):
    """One row per directory prefix encountered during the scan."""

    __tablename__ = "prefixes"

    host_id: Mapped[str] = mapped_column(sa.String, primary_key=True)
    prefix: Mapped[str] = mapped_column(sa.String, primary_key=True)
    size: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=0)
    complete: Mapped[bool] = mapped_column(
        sa.Boolean, nullable=False, default=False
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def resolve_uid(user: Optional[str]) -> Optional[int]:
    """Return the UID for *user*, or *None* when no user is specified.

    When *user* is ``None`` the caller should scan all accessible files
    without restricting by ownership.
    """
    if user is None:
        return None
    try:
        return pwd.getpwnam(user).pw_uid
    except KeyError:
        sys.exit(f"Error: unknown user '{user}'")


def file_kind(mode: int) -> str:
    """Return a human-readable file-type string from a stat mode."""
    if stat.S_ISREG(mode):
        return "file"
    if stat.S_ISLNK(mode):
        return "symlink"
    if stat.S_ISDIR(mode):
        return "directory"
    if stat.S_ISBLK(mode):
        return "block"
    if stat.S_ISCHR(mode):
        return "char"
    if stat.S_ISFIFO(mode):
        return "fifo"
    if stat.S_ISSOCK(mode):
        return "socket"
    return "other"


def format_size(size: float) -> str:
    """Return *size* bytes as a human-readable string."""
    fsize = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(fsize) < 1024:
            return f"{fsize:.1f} {unit}"
        fsize /= 1024
    return f"{fsize:.1f} EB"


# ---------------------------------------------------------------------------
# nice / ionice self-re-exec
# ---------------------------------------------------------------------------

_NICED_ENV = "_STORAGE_USAGE_NICED"


def apply_nice_ionice(
    nice: Optional[int],
    ionice_class: Optional[int],
    ionice_level: Optional[int],
) -> None:
    """Re-exec the current process under nice/ionice when requested.

    A sentinel environment variable prevents infinite recursion.
    """
    if os.environ.get(_NICED_ENV):
        return
    if nice is None and ionice_class is None:
        return

    cmd: list[str] = []

    if ionice_class is not None:
        cmd += ["ionice", f"-c{ionice_class}"]
        if ionice_level is not None:
            cmd += [f"-n{ionice_level}"]
        cmd += ["--"]

    if nice is not None:
        cmd += ["nice", f"-n{nice}"]

    cmd += [sys.executable] + sys.argv

    os.environ[_NICED_ENV] = "1"
    os.execvp(cmd[0], cmd)  # replaces the current process


# ---------------------------------------------------------------------------
# Scan
# ---------------------------------------------------------------------------


def load_complete_prefixes(session: Session, host_id: str) -> Set[str]:
    """Return the set of prefix strings already marked *complete* in the DB."""
    rows = (
        session.query(PrefixRecord.prefix)
        .filter(
            PrefixRecord.host_id == host_id,
            PrefixRecord.complete.is_(True),
        )
        .all()
    )
    return {row.prefix for row in rows}


def _is_under_complete_prefix(path: str, complete: Set[str]) -> bool:
    """Return True if *path* equals or is nested under any complete prefix."""
    for prefix in complete:
        if path == prefix or path.startswith(prefix + os.sep):
            return True
    return False


def scan(
    root: Path,
    uid: Optional[int],
    host_id: str,
    session: Session,
    complete_prefixes: Set[str],
) -> None:
    """Scan *root* recursively and persist file/prefix records.

    Directories already marked *complete* in the database are skipped so that
    an interrupted scan can be resumed efficiently.

    When *uid* is given, only file-system entries owned by that uid are stored
    in the ``files`` table.  When *uid* is ``None``, all accessible entries are
    stored and users are inserted into the ``users`` table as they are
    encountered.

    Every directory that is actually entered is recorded in the ``prefixes``
    table with the **direct** size of entries found inside it; the aggregate
    (recursive) sizes are computed afterwards by :func:`update_prefix_sizes`.
    """
    dirs_to_visit: list[Path] = [root]

    while dirs_to_visit:
        current = dirs_to_visit.pop()
        current_str = str(current)

        if _is_under_complete_prefix(current_str, complete_prefixes):
            continue

        try:
            entries = list(os.scandir(current))
        except OSError:
            # Permission denied or similar – record the prefix as incomplete
            _upsert_prefix(session, host_id, current_str, 0, complete=False)
            session.commit()
            continue

        direct_size = 0

        for entry in entries:
            try:
                st = entry.stat(follow_symlinks=False)
            except OSError:
                continue

            if entry.is_dir(follow_symlinks=False):
                dirs_to_visit.append(Path(entry.path))

            if uid is not None and st.st_uid != uid:
                continue

            kind = file_kind(st.st_mode)
            size = st.st_size
            direct_size += size
            _upsert_user(session, st.st_uid)
            _upsert_file(session, host_id, entry.path, size, kind, st.st_uid)

        _upsert_prefix(session, host_id, current_str, direct_size, complete=True)
        session.commit()


def _upsert_user(session: Session, uid: int) -> None:
    if session.get(UserRecord, uid) is not None:
        return
    try:
        username = pwd.getpwuid(uid).pw_name
    except KeyError:
        username = str(uid)
    session.add(UserRecord(uid=uid, username=username))


def _upsert_file(
    session: Session, host_id: str, path: str, size: int, kind: str, uid: int
) -> None:
    existing = session.get(FileRecord, (host_id, path))
    if existing is not None:
        existing.size = size
        existing.kind = kind
        existing.uid = uid
    else:
        session.add(
            FileRecord(host_id=host_id, path=path, size=size, kind=kind, uid=uid)
        )


def _upsert_prefix(
    session: Session, host_id: str, prefix: str, size: int, *, complete: bool
) -> None:
    existing = session.get(PrefixRecord, (host_id, prefix))
    if existing is not None:
        existing.size = size
        existing.complete = complete
    else:
        session.add(
            PrefixRecord(host_id=host_id, prefix=prefix, size=size, complete=complete)
        )


def update_prefix_sizes(session: Session, host_id: str) -> None:
    """Set each prefix's ``size`` to the **recursive** total of owned files.

    After scanning, the ``prefixes.size`` column holds only the *direct* bytes
    found in each directory.  This function recomputes it as the recursive sum
    of all ``files`` entries whose path falls under each prefix.
    """
    # Build a mapping prefix -> total recursive size in Python.
    # For very large datasets a pure-SQL approach would be preferable, but
    # this implementation is straightforward and correct for all SQL backends.
    prefixes = (
        session.query(PrefixRecord)
        .filter(PrefixRecord.host_id == host_id)
        .all()
    )
    files = (
        session.query(FileRecord.path, FileRecord.size)
        .filter(FileRecord.host_id == host_id)
        .all()
    )

    for prefix_rec in prefixes:
        p = prefix_rec.prefix + os.sep
        total = sum(size for path, size in files if path.startswith(p))
        prefix_rec.size = total

    session.commit()


# ---------------------------------------------------------------------------
# Directory tree report
# ---------------------------------------------------------------------------


def generate_directory_tree(
    session: Session,
    host_id: str,
    max_depth: int,
    uid: Optional[int] = None,
) -> Optional[dict]:
    """Generate a hierarchical tree of directories with disk usage up to *max_depth*.

    Uses :class:`PrefixRecord` as an indexed lookup source for efficient
    queries.  When *uid* is provided, only files owned by that user are
    included; otherwise all users are aggregated.

    Returns a nested dict of the form::

        {
            "path": "/scan/root",
            "size": 160000000000,
            "formatted_size": "160.0 GB",
            "depth": 0,
            "children": [
                {"path": "/scan/root/subdir1", "size": 80000000000, ...},
                ...
            ]
        }

    Returns ``None`` when no complete prefixes exist for the host.
    """
    if max_depth < 0:
        return None

    # Load all complete prefix records for this host.
    prefixes = (
        session.query(PrefixRecord)
        .filter(PrefixRecord.host_id == host_id, PrefixRecord.complete.is_(True))
        .all()
    )

    if not prefixes:
        return None

    prefix_map: dict[str, PrefixRecord] = {p.prefix: p for p in prefixes}
    all_prefix_paths: set[str] = set(prefix_map.keys())

    # Find root paths: complete prefixes that are not nested under any other
    # complete prefix.
    roots = sorted(
        p
        for p in all_prefix_paths
        if not any(
            p.startswith(other + os.sep)
            for other in all_prefix_paths
            if p != other
        )
    )

    if not roots:
        return None

    # When filtering by user, pre-compute recursive sizes from FileRecord.
    # Walk each file's directory hierarchy once to accumulate sizes per prefix
    # in O(n * d) time, where n is the number of files and d is the path depth.
    user_prefix_sizes: Optional[dict[str, int]] = None
    if uid is not None:
        files = (
            session.query(FileRecord.path, FileRecord.size)
            .filter(FileRecord.host_id == host_id, FileRecord.uid == uid)
            .all()
        )
        user_prefix_sizes = {p: 0 for p in prefix_map}
        for fpath, size in files:
            current = os.path.dirname(fpath)
            while current:
                if current in user_prefix_sizes:
                    user_prefix_sizes[current] += size
                parent = os.path.dirname(current)
                if parent == current:  # reached the filesystem root
                    break
                current = parent

    def _get_size(prefix_path: str) -> int:
        if user_prefix_sizes is not None:
            return user_prefix_sizes.get(prefix_path, 0)
        rec = prefix_map.get(prefix_path)
        return rec.size if rec is not None else 0

    def _build_node(prefix_path: str, current_depth: int) -> Optional[dict]:
        total_size = _get_size(prefix_path)

        children: list[dict] = []
        if current_depth < max_depth:
            seen: set[str] = set()
            prefix_with_sep = prefix_path + os.sep
            for p in all_prefix_paths:
                if not p.startswith(prefix_with_sep):
                    continue
                relative = p[len(prefix_with_sep):]
                # Direct child: no separator in the relative portion.
                child_path = prefix_with_sep + relative.split(os.sep)[0]
                if child_path not in seen and child_path in all_prefix_paths:
                    seen.add(child_path)
                    child_node = _build_node(child_path, current_depth + 1)
                    if child_node is not None:
                        children.append(child_node)

        if total_size == 0 and not children:
            return None

        return {
            "path": prefix_path,
            "size": total_size,
            "formatted_size": format_size(total_size),
            "depth": current_depth,
            "children": sorted(children, key=lambda x: x["size"], reverse=True),
        }

    if len(roots) == 1:
        return _build_node(roots[0], 0)

    # Multiple scan roots: wrap them in a virtual top-level node.
    root_nodes = [n for r in roots if (n := _build_node(r, 0)) is not None]
    if not root_nodes:
        return None
    total = sum(n["size"] for n in root_nodes)
    return {
        "path": os.sep,
        "size": total,
        "formatted_size": format_size(total),
        "depth": 0,
        "children": sorted(root_nodes, key=lambda x: x["size"], reverse=True),
    }


def format_tree_report(
    tree: Optional[dict],
    username: Optional[str] = None,
) -> str:
    """Format a directory tree dict as a human-readable text report.

    The tree is the structure returned by :func:`generate_directory_tree`.
    Pass *username* to include a header line identifying the user.
    """
    if tree is None:
        return "No data available."

    lines: list[str] = []
    if username:
        lines.append(f"User: {username}\n")
    else:
        lines.append("All Users\n")

    def _add_lines(node: dict, indent: int = 0) -> None:
        indent_str = "    " * indent + ("- " if indent > 0 else "")
        lines.append(f"{indent_str}{node['path']:<50} {node['formatted_size']:>12}")
        for child in node["children"]:
            _add_lines(child, indent + 1)

    _add_lines(tree)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# HTML summary
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Storage Usage Summary</title>
  <style>
    body { font-family: sans-serif; margin: 2em; }
    h1   { color: #333; }
    h2   { color: #555; margin-top: 2em; }
    table { border-collapse: collapse; width: 100%; margin-top: 0.5em; }
    th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; }
    th     { background: #eee; }
    tr:nth-child(even) { background: #f9f9f9; }
    .num   { text-align: right; }
    .check { text-align: center; }
  </style>
</head>
<body>
<h1>Storage Usage Summary</h1>
<p>Total files: <strong>{{ total_files | format_int }}</strong></p>
<p>Total size: <strong>{{ total_size | format_size }}</strong></p>

<h2>Largest Files (top {{ largest_files | length }})</h2>
<table>
  <tr><th>Host</th><th>Path</th><th>Kind</th><th class="num">Size</th></tr>
  {% for f in largest_files %}
  <tr>
    <td>{{ f.host_id }}</td>
    <td>{{ f.path }}</td>
    <td>{{ f.kind }}</td>
    <td class="num">{{ f.size | format_size }}</td>
  </tr>
  {% endfor %}
</table>

<h2>Largest Directory Prefixes (top {{ largest_prefixes | length }})</h2>
<table>
  <tr><th>Host</th><th>Prefix</th><th class="num">Size</th><th class="check">Complete</th></tr>
  {% for p in largest_prefixes %}
  <tr>
    <td>{{ p.host_id }}</td>
    <td>{{ p.prefix }}</td>
    <td class="num">{{ p.size | format_size }}</td>
    <td class="check">{{ "✓" if p.complete else "✗" }}</td>
  </tr>
  {% endfor %}
</table>
</body>
</html>
"""


def generate_summary_html(session: Session, output: Path, top_n: int = 20) -> None:
    """Write an HTML summary of the database to *output*."""
    from jinja2 import Environment

    total_files: int = session.query(sa.func.count(FileRecord.path)).scalar() or 0
    total_size: int = session.query(sa.func.sum(FileRecord.size)).scalar() or 0

    largest_files = (
        session.query(FileRecord)
        .order_by(FileRecord.size.desc())
        .limit(top_n)
        .all()
    )
    largest_prefixes = (
        session.query(PrefixRecord)
        .order_by(PrefixRecord.size.desc())
        .limit(top_n)
        .all()
    )

    env = Environment(autoescape=True)
    env.filters["format_size"] = format_size
    env.filters["format_int"] = lambda n: f"{n:,}"

    tmpl = env.from_string(_HTML_TEMPLATE)
    html = tmpl.render(
        total_files=total_files,
        total_size=total_size,
        largest_files=largest_files,
        largest_prefixes=largest_prefixes,
    )

    output.write_text(html, encoding="utf-8")
    print(f"Summary written to {output}")


# ---------------------------------------------------------------------------
# Web server
# ---------------------------------------------------------------------------

_SERVE_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Storage Usage</title>
  <style>
    body { font-family: sans-serif; margin: 2em; max-width: 1400px; }
    h1   { color: #333; }
    h2   { color: #555; margin-top: 2em; }
    table { border-collapse: collapse; width: 100%; margin-top: 0.5em; }
    th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; word-break: break-all; }
    th     { background: #eee; }
    tr:nth-child(even) { background: #f9f9f9; }
    .num   { text-align: right; white-space: nowrap; }
    .check { text-align: center; }
    .filters { display: flex; gap: 1.5em; flex-wrap: wrap; margin-bottom: 1em; align-items: flex-end; }
    .filters label { display: flex; flex-direction: column; font-size: 0.85em;
                     font-weight: bold; gap: 4px; color: #555; }
    .filters input[type=text] { padding: 5px 8px; border: 1px solid #ccc;
                                 border-radius: 3px; font-size: 1em; min-width: 160px; }
    .filters input[type=range] { width: 180px; cursor: pointer; }
    .size-label { font-weight: normal; font-size: 0.9em; color: #333; }
    .pagination { display: flex; gap: 0.5em; margin-top: 1em; align-items: center; }
    .pagination button { padding: 5px 14px; cursor: pointer; border: 1px solid #ccc;
                         background: #fff; border-radius: 3px; font-size: 0.9em; }
    .pagination button:disabled { opacity: 0.35; cursor: default; }
    .page-info { color: #555; font-size: 0.9em; }
    .empty { color: #888; font-style: italic; }
  </style>
</head>
<body>
<h1>Storage Usage</h1>

<h2>Files</h2>
<div class="filters">
  <label>Host
    <input type="text" id="f-host" placeholder="substring\u2026">
  </label>
  <label>Path
    <input type="text" id="f-path" placeholder="substring\u2026">
  </label>
  <label>Kind
    <input type="text" id="f-kind" placeholder="substring\u2026">
  </label>
  <label>Min Size: <span class="size-label" id="f-size-lbl">0 B</span>
    <input type="range" id="f-size" min="0" max="6" value="0" step="1">
  </label>
</div>
<table>
  <thead>
    <tr><th>Host</th><th>Path</th><th>Kind</th><th class="num">Size</th></tr>
  </thead>
  <tbody id="f-tbody">
    <tr><td colspan="4" class="empty">Loading\u2026</td></tr>
  </tbody>
</table>
<div class="pagination">
  <button id="f-prev">&#8592; Prev</button>
  <span class="page-info" id="f-info"></span>
  <button id="f-next">Next &#8594;</button>
</div>

<h2>Directories</h2>
<div class="filters">
  <label>Host
    <input type="text" id="d-host" placeholder="substring\u2026">
  </label>
  <label>Path
    <input type="text" id="d-path" placeholder="substring\u2026">
  </label>
  <label>Min Size: <span class="size-label" id="d-size-lbl">0 B</span>
    <input type="range" id="d-size" min="0" max="6" value="0" step="1">
  </label>
</div>
<table>
  <thead>
    <tr><th>Host</th><th>Path</th><th class="num">Size</th><th class="check">Complete</th></tr>
  </thead>
  <tbody id="d-tbody">
    <tr><td colspan="4" class="empty">Loading\u2026</td></tr>
  </tbody>
</table>
<div class="pagination">
  <button id="d-prev">&#8592; Prev</button>
  <span class="page-info" id="d-info"></span>
  <button id="d-next">Next &#8594;</button>
</div>

<script>
// Size thresholds: 0 B, 1 KB, 1 MB, 1 GB, 1 TB, 1 PB, 1 EB
const THRESHOLDS = [0, 1024, 1048576, 1073741824, 1099511627776,
                    1125899906842624, 1152921504606846976];
const THRESHOLD_LABELS = ['0 B', '1 KB', '1 MB', '1 GB', '1 TB', '1 PB', '1 EB'];

function formatSize(b) {
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB'];
  let v = Number(b);
  for (const u of units) {
    if (Math.abs(v) < 1024) return v.toFixed(1) + '\u00a0' + u;
    v /= 1024;
  }
  return v.toFixed(1) + '\u00a0EB';
}

function esc(s) {
  return String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

const filesState = { page: 1, host: '', path: '', kind: '', minSizeIdx: 0 };
const dirsState  = { page: 1, host: '', path: '', minSizeIdx: 0 };

async function fetchFiles() {
  const s = filesState;
  const p = new URLSearchParams({
    page: s.page, per_page: 20,
    host: s.host, path: s.path, kind: s.kind,
    min_size: THRESHOLDS[s.minSizeIdx],
  });
  const resp = await fetch('/api/files?' + p);
  const data = await resp.json();
  const tbody = document.getElementById('f-tbody');
  if (!data.rows.length) {
    tbody.innerHTML = '<tr><td colspan="4" class="empty">No records match the current filters.</td></tr>';
  } else {
    tbody.innerHTML = data.rows.map(r =>
      '<tr>' +
      '<td>' + esc(r.host_id) + '</td>' +
      '<td>' + esc(r.path) + '</td>' +
      '<td>' + esc(r.kind) + '</td>' +
      '<td class="num">' + formatSize(r.size) + '</td>' +
      '</tr>'
    ).join('');
  }
  document.getElementById('f-info').textContent =
    'Page\u00a0' + data.page + '\u00a0/\u00a0' + data.total_pages +
    '\u2002\u2014\u2002' + data.total.toLocaleString() + '\u00a0records';
  document.getElementById('f-prev').disabled = data.page <= 1;
  document.getElementById('f-next').disabled = data.page >= data.total_pages;
}

async function fetchDirs() {
  const s = dirsState;
  const p = new URLSearchParams({
    page: s.page, per_page: 20,
    host: s.host, path: s.path,
    min_size: THRESHOLDS[s.minSizeIdx],
  });
  const resp = await fetch('/api/dirs?' + p);
  const data = await resp.json();
  const tbody = document.getElementById('d-tbody');
  if (!data.rows.length) {
    tbody.innerHTML = '<tr><td colspan="4" class="empty">No records match the current filters.</td></tr>';
  } else {
    tbody.innerHTML = data.rows.map(r =>
      '<tr>' +
      '<td>' + esc(r.host_id) + '</td>' +
      '<td>' + esc(r.path) + '</td>' +
      '<td class="num">' + formatSize(r.size) + '</td>' +
      '<td class="check">' + (r.complete ? '\u2713' : '\u2717') + '</td>' +
      '</tr>'
    ).join('');
  }
  document.getElementById('d-info').textContent =
    'Page\u00a0' + data.page + '\u00a0/\u00a0' + data.total_pages +
    '\u2002\u2014\u2002' + data.total.toLocaleString() + '\u00a0records';
  document.getElementById('d-prev').disabled = data.page <= 1;
  document.getElementById('d-next').disabled = data.page >= data.total_pages;
}

function debounce(fn, ms) {
  let t;
  return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

// Files filters
['f-host', 'f-path', 'f-kind'].forEach(id => {
  const key = id.slice(2); // 'host', 'path', 'kind'
  document.getElementById(id).addEventListener('input', debounce(e => {
    filesState[key] = e.target.value;
    filesState.page = 1;
    fetchFiles();
  }, 300));
});
document.getElementById('f-size').addEventListener('input', e => {
  filesState.minSizeIdx = +e.target.value;
  filesState.page = 1;
  document.getElementById('f-size-lbl').textContent = THRESHOLD_LABELS[filesState.minSizeIdx];
  fetchFiles();
});
document.getElementById('f-prev').addEventListener('click', () => { filesState.page--; fetchFiles(); });
document.getElementById('f-next').addEventListener('click', () => { filesState.page++; fetchFiles(); });

// Dirs filters
['d-host', 'd-path'].forEach(id => {
  const key = id.slice(2);
  document.getElementById(id).addEventListener('input', debounce(e => {
    dirsState[key] = e.target.value;
    dirsState.page = 1;
    fetchDirs();
  }, 300));
});
document.getElementById('d-size').addEventListener('input', e => {
  dirsState.minSizeIdx = +e.target.value;
  dirsState.page = 1;
  document.getElementById('d-size-lbl').textContent = THRESHOLD_LABELS[dirsState.minSizeIdx];
  fetchDirs();
});
document.getElementById('d-prev').addEventListener('click', () => { dirsState.page--; fetchDirs(); });
document.getElementById('d-next').addEventListener('click', () => { dirsState.page++; fetchDirs(); });

// Initial load
fetchFiles();
fetchDirs();
</script>
</body>
</html>
"""


def serve_db(engine: sa.Engine, port: int) -> None:
    """Start an HTTP server that provides a live web UI over *engine*.

    Routes
    ------
    GET /              – HTML single-page application
    GET /api/files     – JSON: paginated file records (filterable)
    GET /api/dirs      – JSON: paginated prefix records (filterable)

    Query parameters for both API endpoints
    ----------------------------------------
    page      – 1-based page number (default 1)
    per_page  – records per page (default 20, max 100)
    host      – substring filter on host_id
    path      – substring filter on path / prefix
    min_size  – minimum size in bytes (integer; 0 = no filter)

    Additional parameter for /api/files
    ------------------------------------
    kind      – substring filter on kind
    """

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # type: ignore[override]
            """Print requests to stdout instead of stderr (default behaviour)."""
            print(f"[{self.address_string()}] {fmt % args}")

        def _send_json(self, obj: dict, status: int = 200) -> None:
            body = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, html: str) -> None:
            body = html.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)

            def first(key: str, default: str = "") -> str:
                return qs.get(key, [default])[0]

            if parsed.path == "/":
                self._send_html(_SERVE_HTML)
                return

            if parsed.path in ("/api/files", "/api/dirs"):
                try:
                    page = max(1, int(first("page", "1")))
                    per_page = max(1, min(100, int(first("per_page", "20"))))
                    min_size = int(first("min_size", "0"))
                except ValueError:
                    self._send_json({"error": "invalid query parameter"}, 400)
                    return

                host_filter = first("host")
                path_filter = first("path")

                with Session(engine) as session:
                    if parsed.path == "/api/files":
                        kind_filter = first("kind")
                        q = session.query(FileRecord)
                        if host_filter:
                            q = q.filter(FileRecord.host_id.contains(host_filter))
                        if path_filter:
                            q = q.filter(FileRecord.path.contains(path_filter))
                        if kind_filter:
                            q = q.filter(FileRecord.kind.contains(kind_filter))
                        if min_size > 0:
                            q = q.filter(FileRecord.size >= min_size)
                        q = q.order_by(FileRecord.size.desc())
                        total: int = q.count()
                        rows_f = q.offset((page - 1) * per_page).limit(per_page).all()
                        total_pages = max(1, (total + per_page - 1) // per_page)
                        self._send_json({
                            "page": page,
                            "total_pages": total_pages,
                            "total": total,
                            "rows": [
                                {
                                    "host_id": r.host_id,
                                    "path": r.path,
                                    "kind": r.kind,
                                    "size": r.size,
                                }
                                for r in rows_f
                            ],
                        })
                    else:  # /api/dirs
                        q2 = session.query(PrefixRecord)
                        if host_filter:
                            q2 = q2.filter(PrefixRecord.host_id.contains(host_filter))
                        if path_filter:
                            q2 = q2.filter(PrefixRecord.prefix.contains(path_filter))
                        if min_size > 0:
                            q2 = q2.filter(PrefixRecord.size >= min_size)
                        q2 = q2.order_by(PrefixRecord.size.desc())
                        total2: int = q2.count()
                        rows_d = q2.offset((page - 1) * per_page).limit(per_page).all()
                        total_pages2 = max(1, (total2 + per_page - 1) // per_page)
                        self._send_json({
                            "page": page,
                            "total_pages": total_pages2,
                            "total": total2,
                            "rows": [
                                {
                                    "host_id": r.host_id,
                                    "path": r.prefix,
                                    "size": r.size,
                                    "complete": r.complete,
                                }
                                for r in rows_d
                            ],
                        })
                return

            self.send_response(404)
            self.end_headers()

    httpd = HTTPServer(("", port), _Handler)
    print(f"Serving on http://localhost:{port}/ — press Ctrl+C to stop")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="storage_usage.py",
        description=(
            "Scan a directory tree and build a database of storage usage "
            "for files owned by a given user, or serve a live web UI over "
            "an existing database."
        ),
    )
    parser.add_argument(
        "path",
        metavar="PATH",
        type=Path,
        nargs="?",
        default=None,
        help="Root path to scan (required unless --serve is given).",
    )
    parser.add_argument(
        "--user",
        default=None,
        metavar="USER",
        help="Only include files owned by USER (default: No user).",
    )
    parser.add_argument(
        "--db",
        default="sqlite:///storage_usage.db",
        metavar="URL",
        help=(
            "SQLAlchemy database URL "
            "(default: sqlite:///storage_usage.db)."
        ),
    )
    parser.add_argument(
        "--summary-html",
        metavar="FILE",
        type=Path,
        default=None,
        help="Write an HTML summary of the database to FILE.",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        default=False,
        help=(
            "Start a web UI that browses the database instead of scanning. "
            "PATH is not required in this mode."
        ),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        metavar="PORT",
        help="Port for the --serve web UI (default: 8080).",
    )
    parser.add_argument(
        "--nice",
        type=int,
        default=None,
        metavar="N",
        help="Run with the given nice(1) priority adjustment (0–19).",
    )
    parser.add_argument(
        "--ionice-class",
        type=int,
        choices=[1, 2, 3],
        default=None,
        metavar="{1,2,3}",
        help=(
            "ionice(1) scheduling class: "
            "1=realtime, 2=best-effort, 3=idle."
        ),
    )
    parser.add_argument(
        "--ionice-level",
        type=int,
        choices=range(8),
        default=None,
        metavar="{0..7}",
        help="ionice(1) priority level within the class (0–7).",
    )
    parser.add_argument(
        "--directory-tree",
        action="store_true",
        default=False,
        help=(
            "Print a hierarchical directory tree with disk usage to stdout. "
            "Can be combined with a scan or used standalone against an "
            "existing database (PATH is then optional)."
        ),
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=3,
        metavar="N",
        help="Maximum directory depth for --directory-tree (default: 3).",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    engine = sa.create_engine(args.db)
    Base.metadata.create_all(engine)

    if args.serve:
        serve_db(engine, args.port)
        return

    # Scan mode – PATH is required unless only --directory-tree is requested.
    if args.path is None and not args.directory_tree:
        parser.error("PATH is required unless --serve or --directory-tree is given")

    uid = resolve_uid(args.user)
    host_id = socket.getfqdn()

    with Session(engine) as session:
        if args.path is not None:
            # Re-exec under nice/ionice before doing any real work.
            apply_nice_ionice(args.nice, args.ionice_class, args.ionice_level)

            # Resolve to an absolute realpath so that stored paths are canonical.
            root = args.path.resolve()

            complete_prefixes = load_complete_prefixes(session, host_id)

            print(
                f"Scanning {root!s} "
                + (
                    f"for files owned by uid={uid} on {host_id}"
                    if uid is not None
                    else f"for all accessible files on {host_id}"
                )
                + (
                    f" (skipping {len(complete_prefixes)} complete prefix(es))"
                    if complete_prefixes
                    else ""
                )
            )

            scan(root, uid, host_id, session, complete_prefixes)
            update_prefix_sizes(session, host_id)

            print("Scan complete.")

            if args.summary_html is not None:
                generate_summary_html(session, args.summary_html)

        if args.directory_tree:
            tree = generate_directory_tree(session, host_id, args.depth, uid)
            print(format_tree_report(tree, args.user))


if __name__ == "__main__":
    main()
