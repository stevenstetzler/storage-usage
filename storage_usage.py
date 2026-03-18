#!/usr/bin/env python3
"""
storage_usage.py – Scan file-system storage and build a summary database.

Usage
-----
storage_usage.py [--user USER] [--db SQLALCHEMY_URL]
                 [--summary-html FILE]
                 [--nice N] [--ionice-class {1,2,3}] [--ionice-level {0..7}]
                 PATH
"""

import argparse
import os
import pwd
import stat
import sys
from pathlib import Path
from typing import Optional, Set

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class FileRecord(Base):
    """One row per file-system entry belonging to the scanned user."""

    __tablename__ = "files"

    path: Mapped[str] = mapped_column(sa.String, primary_key=True)
    size: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    kind: Mapped[str] = mapped_column(sa.String, nullable=False)


class PrefixRecord(Base):
    """One row per directory prefix encountered during the scan."""

    __tablename__ = "prefixes"

    prefix: Mapped[str] = mapped_column(sa.String, primary_key=True)
    size: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=0)
    complete: Mapped[bool] = mapped_column(
        sa.Boolean, nullable=False, default=False
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def resolve_uid(user: Optional[str]) -> int:
    """Return the UID for *user*, or the current process's UID when None."""
    if user is None:
        return os.getuid()
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


def load_complete_prefixes(session: Session) -> Set[str]:
    """Return the set of prefix strings already marked *complete* in the DB."""
    rows = (
        session.query(PrefixRecord.prefix)
        .filter(PrefixRecord.complete.is_(True))
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
    uid: int,
    session: Session,
    complete_prefixes: Set[str],
) -> None:
    """Scan *root* recursively and persist file/prefix records.

    Directories already marked *complete* in the database are skipped so that
    an interrupted scan can be resumed efficiently.

    Only file-system entries owned by *uid* are stored in the ``files`` table.
    Every directory that is actually entered is recorded in the ``prefixes``
    table with the **direct** size of user-owned entries found inside it; the
    aggregate (recursive) sizes are computed afterwards by
    :func:`update_prefix_sizes`.
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
            _upsert_prefix(session, current_str, 0, complete=False)
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

            if st.st_uid != uid:
                continue

            kind = file_kind(st.st_mode)
            size = st.st_size
            direct_size += size
            _upsert_file(session, entry.path, size, kind)

        _upsert_prefix(session, current_str, direct_size, complete=True)
        session.commit()


def _upsert_file(session: Session, path: str, size: int, kind: str) -> None:
    existing = session.get(FileRecord, path)
    if existing is not None:
        existing.size = size
        existing.kind = kind
    else:
        session.add(FileRecord(path=path, size=size, kind=kind))


def _upsert_prefix(
    session: Session, prefix: str, size: int, *, complete: bool
) -> None:
    existing = session.get(PrefixRecord, prefix)
    if existing is not None:
        existing.size = size
        existing.complete = complete
    else:
        session.add(PrefixRecord(prefix=prefix, size=size, complete=complete))


def update_prefix_sizes(session: Session) -> None:
    """Set each prefix's ``size`` to the **recursive** total of owned files.

    After scanning, the ``prefixes.size`` column holds only the *direct* bytes
    found in each directory.  This function recomputes it as the recursive sum
    of all ``files`` entries whose path falls under each prefix.
    """
    # Build a mapping prefix -> total recursive size in Python.
    # For very large datasets a pure-SQL approach would be preferable, but
    # this implementation is straightforward and correct for all SQL backends.
    prefixes = session.query(PrefixRecord).all()
    files = session.query(FileRecord.path, FileRecord.size).all()

    for prefix_rec in prefixes:
        p = prefix_rec.prefix + os.sep
        total = sum(size for path, size in files if path.startswith(p))
        prefix_rec.size = total

    session.commit()


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
  <tr><th>Path</th><th>Kind</th><th class="num">Size</th></tr>
  {% for f in largest_files %}
  <tr>
    <td>{{ f.path }}</td>
    <td>{{ f.kind }}</td>
    <td class="num">{{ f.size | format_size }}</td>
  </tr>
  {% endfor %}
</table>

<h2>Largest Directory Prefixes (top {{ largest_prefixes | length }})</h2>
<table>
  <tr><th>Prefix</th><th class="num">Size</th><th class="check">Complete</th></tr>
  {% for p in largest_prefixes %}
  <tr>
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
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="storage_usage.py",
        description=(
            "Scan a directory tree and build a database of storage usage "
            "for files owned by a given user."
        ),
    )
    parser.add_argument(
        "path",
        metavar="PATH",
        type=Path,
        help="Root path to scan.",
    )
    parser.add_argument(
        "--user",
        default=None,
        metavar="USER",
        help="Only include files owned by USER (default: current user).",
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
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Re-exec under nice/ionice before doing any real work.
    apply_nice_ionice(args.nice, args.ionice_class, args.ionice_level)

    uid = resolve_uid(args.user)

    engine = sa.create_engine(args.db)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        complete_prefixes = load_complete_prefixes(session)

        print(
            f"Scanning {args.path!s} "
            f"for files owned by uid={uid}"
            + (
                f" (skipping {len(complete_prefixes)} complete prefix(es))"
                if complete_prefixes
                else ""
            )
        )

        scan(args.path, uid, session, complete_prefixes)
        update_prefix_sizes(session)

        print("Scan complete.")

        if args.summary_html is not None:
            generate_summary_html(session, args.summary_html)


if __name__ == "__main__":
    main()
