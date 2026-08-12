# News Weaver

An automated ETL pipeline that collects news from RSS feeds and web sources,
extracts structured content, and stores it in a local data warehouse.

```
┌─────────────┐     scrape     ┌─────────────┐    transform    ┌─────────────┐
│   Sources   │ ─────────────► │  Extractor  │ ──────────────► │ Transformer │
│ (DB table)  │                │  (fetcher)  │                 │  (parser)   │
└─────────────┘                └─────────────┘                 └──────┬──────┘
                                                                       │ POST
                                                               ┌───────▼──────┐
                                                               │  Loader API  │
                                                               │  (FastAPI)   │
                                                               └───────┬──────┘
                                                                       │ store
                                                               ┌───────▼──────┐
                                                               │   data.db    │
                                                               │ (warehouse)  │
                                                               └──────────────┘
```

---

## Table of Contents

1. [Architecture](#architecture)
2. [Requirements](#requirements)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [First-time Setup](#first-time-setup)
6. [Running the Pipeline](#running-the-pipeline)
7. [Scheduling with Cron](#scheduling-with-cron)
8. [Managing Sources](#managing-sources)
9. [Development](#development)
10. [Project Structure](#project-structure)

---

## Architecture

News Weaver is composed of four loosely coupled components that each run as
separate processes:

| Component | Entry-point | Role |
|-----------|-------------|------|
| **Extractor** | `src/news_weaver/extractor.py` | Fetches a single source by ID and saves raw content to `scraped_data/` |
| **Transformer** | `src/news_weaver/transformer.py` | Reads up to 50 unprocessed files from the pipeline DB and posts structured data to the Loader API |
| **Loader API** | `src/news_weaver/loader.py` | FastAPI service that validates and stores articles, documents, images, and spreadsheets in `data.db` |
| **Manager** | `src/news_weaver/manager.py` | Reads source schedules from the pipeline DB and writes cron jobs to the user's crontab |

**Two databases** are used:

- `pipeline.db` — tracks sources and the scrape status of each downloaded file.
- `data.db` — the data warehouse where structured content is stored.

Both are SQLite files whose locations are set in `config.yaml`.

---

## Requirements

| Dependency | Notes |
|------------|-------|
| Python ≥ 3.11 | |
| [`uv`](https://docs.astral.sh/uv/) | Package manager and task runner |
| Tesseract OCR | Required only for image content extraction |
| cron (system) | Required for automated scheduling |

### Installing `uv`

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Installing Tesseract (optional — needed for image OCR)

```bash
# Debian / Ubuntu
sudo apt install tesseract-ocr

# macOS (Homebrew)
brew install tesseract
```

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/DanielBerns/news_weaver.git
cd news_weaver

# 2. Create the virtual environment and install all dependencies
uv sync

# 3. Verify the installation
uv run python -c "import news_weaver; print('OK')"
```

---

## Configuration

All non-secret settings live in `config.yaml` at the project root. The file
is read relative to the **current working directory**, so always run commands
from the project root.

```yaml
# config.yaml

database:
  data_db_url: "sqlite:///data.db"         # data warehouse
  pipeline_db_url: "sqlite:///pipeline.db" # pipeline tracking DB

api:
  host: "127.0.0.1"   # Loader API bind address
  port: 8000          # Loader API port
  # secret_key is NOT stored here — see below

logging:
  file: "pipeline.log"  # relative to project root
  level: "INFO"         # DEBUG | INFO | WARNING | ERROR

system:
  project_root: "."           # resolved to absolute path at runtime
  uv_path: "uv"               # full path if uv is not on $PATH
  scraped_data_dir: "scraped_data"  # staging directory for raw downloads
```

### Secret key (required)

The API secret key **must** be provided as an environment variable. It is
never stored in `config.yaml`.

```bash
export NEWS_WEAVER_SECRET_KEY="change-me-to-a-long-random-string"
```

Add this line to your shell profile (`~/.bashrc`, `~/.zshrc`, etc.) or to
a `.env` file that you source before running the pipeline. All four components
read the key from the same environment variable at startup.

### Alternative config path

If you need to keep `config.yaml` somewhere other than the project root, use
either of the following mechanisms:

```bash
# Option A: environment variable
export CONFIG_FILE=/etc/news_weaver/config.yaml

# Option B: CLI flag (per-command override)
uv run src/news_weaver/extractor.py --config /path/to/config.yaml --source_id 1
```

---

## First-time Setup

Run the following commands once, in order, from the project root:

### Step 1 — Initialise the databases

```bash
NEWS_WEAVER_SECRET_KEY="your-secret" uv run init-db
```

This command:
1. Creates all tables in `pipeline.db` and `data.db` if they don't exist.
2. Adds a default seed source (Hacker News RSS) if it isn't already present.

Expected output:

```
Added source: https://news.ycombinator.com/rss
```

### Step 2 — Start the Loader API

The Loader API must be running before the Transformer can post data.
Open a dedicated terminal (or run it as a background service):

```bash
NEWS_WEAVER_SECRET_KEY="your-secret" uv run uvicorn news_weaver.loader:app \
    --host 127.0.0.1 \
    --port 8000
```

You can verify it is running:

```bash
curl -s http://127.0.0.1:8000/docs   # opens Swagger UI
```

### Step 3 — Register cron schedules

```bash
NEWS_WEAVER_SECRET_KEY="your-secret" uv run src/news_weaver/manager.py
```

The Manager reads every source from `pipeline.db`, writes bash wrapper scripts
(`run_extractor.sh`, `run_transformer.sh`) to the project root, and inserts
the corresponding cron jobs into the current user's crontab.

Verify the crontab was written:

```bash
crontab -l | grep ETL_PIPELINE
```

---

## Running the Pipeline

### Manually — run the Extractor for a specific source

```bash
# Replace 1 with the source ID from the pipeline DB
NEWS_WEAVER_SECRET_KEY="your-secret" \
    uv run src/news_weaver/extractor.py --source_id 1
```

The Extractor:
- Fetches the URL for the given source ID.
- Saves the raw response body to `scraped_data/<id>_<timestamp>_<filename>`.
- Records a `ScrapedFile` row in `pipeline.db` with status `SCRAPED`.

### Manually — run the Transformer

```bash
NEWS_WEAVER_SECRET_KEY="your-secret" \
    uv run src/news_weaver/transformer.py
```

The Transformer:
- Picks up to 50 files in `SCRAPED` or `LOAD_FAILED` status.
- Parses HTML (BeautifulSoup), images (Tesseract OCR), or generic documents.
- Posts the structured payload to the Loader API.
- Updates the file status to `PROCESSED_SUCCESSFULLY` or `TRANSFORM_FAILED`.

### Checking logs

All components write JSON-structured log lines to the file configured under
`logging.file` (default: `pipeline.log`):

```bash
tail -f pipeline.log | python -m json.tool
```

---

## Scheduling with Cron

Running the Manager once is sufficient to keep cron schedules in sync:

```bash
NEWS_WEAVER_SECRET_KEY="your-secret" uv run src/news_weaver/manager.py
```

The Manager creates **one extractor cron job per source**, using the
`schedule` field (standard cron expression) stored in the `sources` table,
and one transformer job that runs **every 5 minutes**.

> **Important**: Re-run the Manager whenever you add, remove, or change a
> source's schedule. It removes all previously auto-generated jobs and writes
> fresh ones, so it is safe to run repeatedly.

### Example generated crontab entries

```
*/30 * * * * /path/to/run_extractor.sh --source_id 1 >> /path/to/pipeline.log 2>&1
*/5  * * * * /path/to/run_transformer.sh >> /path/to/pipeline.log 2>&1
```

---

## Managing Sources

Sources are stored in the `sources` table of `pipeline.db`. Use any SQLite
client to manage them, or add a short script.

### Add a new source via SQLite CLI

```bash
sqlite3 pipeline.db "
INSERT INTO sources (url, source_type, schedule)
VALUES ('https://feeds.arstechnica.com/arstechnica/index', 'rss', '0 * * * *');
"
```

| Column | Type | Description |
|--------|------|-------------|
| `url` | TEXT UNIQUE | Full URL of the source |
| `source_type` | TEXT | `rss`, `website`, or `http` |
| `schedule` | TEXT | Standard cron expression (e.g. `*/30 * * * *`) |
| `last_scraped_at` | DATETIME | Set automatically by the Extractor |

After adding a source, re-run the Manager to register its cron job.

### Supported source types

| Type | Behaviour |
|------|-----------|
| `rss` / `website` / `http` / `https` | HTTP GET; content saved as-is |

---

## Development

### Install dev dependencies

```bash
uv sync --dev
```

### Run tests

```bash
NEWS_WEAVER_SECRET_KEY=test-secret uv run pytest tests/ -v
```

### Lint

```bash
uv run ruff check src/ tests/
```

### Type check

```bash
uv run mypy src/
```

### Auto-fix lint issues

```bash
uv run ruff check --fix src/ tests/
```

---

## Project Structure

```
news_weaver/
├── config.yaml                  # Runtime configuration (no secrets)
├── pyproject.toml               # Project metadata, dependencies, QA config
├── pipeline.db                  # Pipeline tracking DB (auto-created)
├── data.db                      # Data warehouse (auto-created)
├── pipeline.log                 # Structured JSON log (auto-created)
├── scraped_data/                # Raw downloaded files (auto-created)
├── run_extractor.sh             # Cron wrapper (auto-generated by Manager)
├── run_transformer.sh           # Cron wrapper (auto-generated by Manager)
│
├── src/news_weaver/
│   ├── common/
│   │   ├── config.py            # Pydantic AppConfig, lazy singleton, logging
│   │   ├── database.py          # SQLAlchemy 2.0 engine/session (lazy init)
│   │   └── models.py            # ORM models: Source, ScrapedFile
│   ├── cli/
│   │   └── init_db.py           # `init-db` entry-point
│   ├── extractor.py             # Fetch + stage raw content
│   ├── transformer.py           # Parse + POST to Loader API
│   ├── loader.py                # FastAPI data warehouse API
│   └── manager.py               # Crontab synchronisation
│
└── tests/
    ├── test_config.py           # Configuration validation tests
    ├── test_models.py           # ORM model tests (in-memory SQLite)
    └── test_loader_api.py       # FastAPI endpoint integration tests
```

---

## Loader API Reference

The Loader API runs at `http://<host>:<port>` (default `http://127.0.0.1:8000`).
All write endpoints require the header `X-API-Key: <NEWS_WEAVER_SECRET_KEY>`.

Interactive documentation is available at `http://127.0.0.1:8000/docs`.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/articles` | Store an HTML article |
| `POST` | `/documents` | Store a generic document |
| `POST` | `/images` | Store an image with OCR text |
| `POST` | `/spreadsheets` | Store spreadsheet data as JSON |

All endpoints return `{"status": "success"}` on `201 Created`, or
`{"status": "exists"}` if the `source_file_id` was already stored.

---

## License

MIT — see [LICENSE](LICENSE).
