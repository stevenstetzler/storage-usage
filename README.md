# storage-usage

A command-line tool that recursively scans a directory tree, records the storage used by each file owned by a given user into a database, and optionally serves a live web UI to browse and filter those results.

---

## Features

- Scans any path on the file system and records per-file sizes together with aggregate directory sizes.
- Filters results by file owner (defaults to the current user).
- Stores data in any SQLAlchemy-supported database (defaults to a local SQLite file).
- Resumes interrupted scans automatically – directories already fully scanned are skipped.
- Runs with reduced CPU and I/O priority via `nice`/`ionice` to avoid disturbing other workloads.
- Generates a standalone HTML summary report of the database contents.
- Serves a browser-based web UI with pagination and filtering over any previously built database.

---

## Requirements

- Python 3.10 or later
- The packages listed in `requirements.txt` (`Jinja2`, `SQLAlchemy`)
- `ionice` and `nice` (optional; only needed if you use the `--ionice-class` / `--nice` flags)

---

## Installation

Install directly from GitHub using pip:

```bash
pip install git+https://github.com/stevenstetzler/storage-usage.git
```

This installs the `storage-usage` command and all required dependencies.

### Development installation

Clone the repository and install in editable mode:

```bash
git clone https://github.com/stevenstetzler/storage-usage.git
cd storage-usage
pip install -e .
```

---

## Usage

```
storage-usage [--user USER] [--db URL]
              [--summary-html FILE]
              [--nice N] [--ionice-class {1,2,3}] [--ionice-level {0..7}]
              PATH

storage-usage --serve [--port PORT] [--db URL]
```

### Scan mode

Scan a directory tree and persist the results to a database:

```bash
storage-usage /path/to/scan
```

Scan a path for files owned by a specific user and write results to a named database:

```bash
storage-usage --user alice --db sqlite:///alice.db /home/alice
```

Generate an HTML summary after scanning:

```bash
storage-usage /data --summary-html report.html
```

Run the scan at reduced priority so it does not affect other processes:

```bash
storage-usage --nice 19 --ionice-class 3 /data
```

### Serve mode

Start the web UI to browse a previously built database:

```bash
storage-usage --serve
```

Use a specific database and port:

```bash
storage-usage --serve --db sqlite:///alice.db --port 9090
```

Open `http://localhost:8080/` (or whichever port you chose) in your browser.

---

## Options

| Option | Description |
|---|---|
| `PATH` | Root directory to scan (required in scan mode). |
| `--user USER` | Only count files owned by `USER`. Defaults to the current user. |
| `--db URL` | SQLAlchemy database URL. Defaults to `sqlite:///storage_usage.db`. |
| `--summary-html FILE` | After scanning, write a standalone HTML summary to `FILE`. |
| `--serve` | Start the web UI instead of scanning. `PATH` is not required. |
| `--port PORT` | Port for the web UI (default: `8080`). |
| `--nice N` | Run the scan under `nice -n N` (0–19). Higher values mean lower CPU priority. |
| `--ionice-class {1,2,3}` | I/O scheduling class: `1` = realtime (requires root), `2` = best-effort, `3` = idle. |
| `--ionice-level {0..7}` | Priority level within the chosen I/O class (0 = highest, 7 = lowest). |

---

## Web UI API

When running in `--serve` mode the server exposes two JSON endpoints in addition to the HTML UI:

| Endpoint | Description |
|---|---|
| `GET /` | Single-page HTML application. |
| `GET /api/files` | Paginated list of individual file records. |
| `GET /api/dirs` | Paginated list of directory prefix records. |

Both API endpoints accept the following query parameters:

| Parameter | Description |
|---|---|
| `page` | 1-based page number (default `1`). |
| `per_page` | Records per page (default `20`, max `100`). |
| `host` | Substring filter on the host identifier. |
| `path` | Substring filter on the file path or directory prefix. |
| `min_size` | Minimum file size in bytes (default: no minimum). |

`/api/files` additionally accepts:

| Parameter | Description |
|---|---|
| `kind` | Substring filter on the file type (`file`, `symlink`, `directory`, …). |

---

## Running the tests

```bash
pip install pytest
pytest tests/
```

---

## License

See [LICENSE](LICENSE).