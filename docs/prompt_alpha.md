# **Resilient ETL Pipeline (Revised)**

## **1. Project Overview**

This document outlines the requirements for developing a complete ETL (Extract, Transform, Load) pipeline in Python. The system is designed to aggregate information from remote (web) and local (filesystem) sources, process the ingested content (including OCR and object detection), and load the structured results into a local database via a REST API.

The architecture consists of four decoupled components: **Manager** (Scheduler), **Extractor** (Ingestion), **Transformer** (Processor), and **Loader** (API & Storage).

## **2. Core Requirements**

* **Language:** Python 3.13
* **Key Libraries:** * **Core:** `SQLAlchemy` (ORM), `Pydantic` (Validation), `python-crontab` (Scheduling), `PyYAML` (Config).
    * **Web/API:** `FastAPI`, `uvicorn`, `httpx` (Async Client).
    * **Processing:** `pillow` (Image processing), `pytesseract` (OCR), `openpyxl` (Excel), `beautifulsoup4` (HTML).
* **Code Style:** Adherence to PEP 8. Use `ruff` for linting and formatting.
* **Testing:** Use `pytest`. High test coverage required for parsers and API validation.
* **Dependency Management:** Use `uv`. A `pyproject.toml` is required.

## **3. Global Requirements**

### **3.1. Unified Configuration**

All components must read from a centralized `config.yaml`.
**Required Settings:**
* **Database:** Connection strings for `pipeline.db` (Control) and `data.db` (Storage).
* **API:** Loader host (e.g., `127.0.0.1`), port, and `secret_key` (for component authentication).
* **Storage:** `scraped_data_dir` (absolute path for raw files).
* **Logging:** Log file path, rotation policy, and log level.
* **System:** Absolute path to the python interpreter (for cron jobs) and project root.

### **3.2. Structured & Concurrent Logging**

* **File:** `pipeline.log`
* **Concurrency:** Since multiple components runs simultaneously, use a concurrency-safe logging approach (e.g., `concurrent-log-handler` or `QueueHandler`) to prevent file corruption.
* **Format:** JSON Lines (JSONL).
    * Fields: `timestamp` (ISO 8601), `level`, `component`, `message`, `context` (dict containing `source_id`, `file_path`, etc.).

## **4. Component Breakdown**

### **4.1. Component 1: The Manager (Scheduler)**

A CLI tool (`manager.py`) that translates the database schedule into system cron jobs. 

**Functionality:**
1.  **Source Sync:** Queries `pipeline.db` (table: `sources`) for active sources.
2.  **Extractor Scheduling:** Generates a cron job for each source to run different bash scripts (where the execution of `extractor.py` is done using the uv package manager, , using the appropiate config file as argument) at the specified schedule.
3.  **Transformer Scheduling:** automatically generates a **fixed interval cron job** (e.g., every 5 minutes) to run a bash script (where the execution of `transformer.py` is done using the package manager, using the appropiate config file as argument). This ensures that files downloaded by the Extractor are processed regularly without manual intervention.
4.  **Safe Update:** Uses a unique marker comment (e.g., `# ETL_PIPELINE_MANAGED`) to clean and update only its own jobs in the user's crontab, preserving other system jobs.

### **4.2. Component 2: The Extractor (Ingestion)**

A CLI tool (`extractor.py`) triggered by Cron to fetch data.

**Control Database (`pipeline.db`):**
* **`sources` table:**
    * `id` (PK), `uri` (Unique - supports `http://`, `https://`, and `file://`), `type` (RSS, WEB, LOCAL), `schedule` (Cron str), `last_scraped_at`.
* **`scraped_files` table:**
    * `id` (PK), `source_id` (FK), `local_path`, `filename`, `mimetype`, `scraped_at`, `status` (ENUM: `SCRAPED`, `PROCESSING`, `PROCESSED`, `FAILED`), `retry_count` (int), `notes`.

**Functionality:**
1.  **Input:** Accepts `source_id` as an argument.
2.  **Protocol Support:**
    * **HTTP/HTTPS:** Downloads content. Handles user-agents and timeouts.
    * **FILE:** Copies files from a local directory if they are new/modified.
3.  **Output:** Saves raw files to `scraped_data_dir` using a collision-resistant naming convention (e.g., `{source_id}_{timestamp}_{filename}`).
4.  **Resilience:**
    * **Transient Errors (Network/Timeout):** Does not update `last_scraped_at` (allowing cron to retry naturally) or logs a "warning" state.
    * **Fatal Errors (404/403):** Logs error to `notes` and marks extraction as failed.

### **4.3. Component 3: The Transformer (Processor)**

A CLI tool (`transformer.py`) that processes raw files into structured data.

**Functionality:**
1.  **Batch Processing:** Queries `scraped_files` for records with status `SCRAPED`.
2.  **Content Extraction:**
    * **HTML/RSS:** Extracts metadata (author, date) and main content (stripping nav/ads).
    * **PDF/DOCX:** Extracts full text.
    * **XLSX:** Converts the active sheet to a JSON-compatible list of dicts.
    * **Images:**
        * **OCR:** Uses `pytesseract` to extract text from the image.
        * **Object Detection:** (Optional) Uses a pre-trained model (e.g., standard libraries or API) to detect objects. Returns a list of labels (e.g., `["car", "person"]`).
        * **Metadata:** Extracts EXIF data (GPS, Date, Camera).
3.  **Loading:** Sends the processed payload to the **Loader API**.
4.  **State Management:**
    * Success: Updates status to `PROCESSED`.
    * Failure: Updates status to `FAILED` and logs the exception in `notes`.

### **4.4. Component 4: The Loader (API & Storage)**

A `FastAPI` server (`loader.py`) managing the final data warehouse (`data.db`).

**Storage Database (`data.db`):**
* **`articles`:** `id`, `source_file_id` (Unique), `url`, `title`, `content` (Text), `language`, `ingested_at`.
* **`documents`:** `id`, `source_file_id` (Unique), `url`, `filename`, `content` (Text), `ingested_at`.
* **`spreadsheets`:** `id`, `source_file_id` (Unique), `url`, `filename`, `data_json` (Text/JSON), `ingested_at`.
* **`images`:** * `id` (PK)
    * `source_file_id` (Unique, FK to pipeline.db ref)
    * `url` (Text)
    * `extracted_text` (Text) — **Added**
    * `detected_objects` (Text/JSON) — **Added** (Stores list of detected tags/boxes)
    * `meta_data` (Text/JSON) — **Added** (Stores technical metadata)
    * `ingested_at` (Datetime)

**API Endpoints:**
* **Auth:** `X-API-Key` header required.
* **Endpoints:**
    * `POST /articles`
    * `POST /documents`
    * `POST /spreadsheets`
    * `POST /images`
        * **Payload:** `{ "source_file_id": int, "url": str, "mimetype": str, "extracted_text": str, "detected_objects": list[str], "metadata": dict }`

## **5. Deliverables**

1.  **Source Code:** `manager.py`, `extractor.py`, `transformer.py`, `loader.py`.
2.  **Configuration:** `config.yaml` template.
3.  **Environment:** `pyproject.toml` fully configured with `uv`.
4.  **Tests:** `pytest` suite with:
    * Unit tests for parsers (HTML, Image, Excel).
    * Integration tests for the Loader API.
5.  **Documentation:** `README.md` covering:
    * Architecture diagram (text-based).
    * Setup instructions (database init, crontab sync).
    * Specific instructions on installing system dependencies (e.g., `tesseract-ocr` binary for Linux).
    
