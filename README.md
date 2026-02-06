# news_weaver

Weawing a web of news

A robust, modular ETL (Extract, Transform, Load) system designed to aggregate information from diverse sources (Web, RSS, Local Files), process it (including OCR and Object Detection), and store it in a centralized data warehouse via a REST API.

## 🏗 Architecture

The system is composed of four decoupled components:

1.  **Manager (`manager.py`):** The conductor. Reads schedules from the database and synchronizes the system `crontab`. It generates bash wrappers to ensure consistent execution environments.
2.  **Extractor (`extractor.py`):** The gatherer. Fetches raw content (HTML, PDF, Images) from configured sources and saves it to a local staging area.
3.  **Transformer (`transformer.py`):** The processor. Detects new raw files, extracts text (using OCR/NLP), formats data, and pushes it to the Loader.
4.  **Loader (`loader.py`):** The destination. A FastAPI server that validates incoming data and stores it in the final `data.db`.

## 🚀 Prerequisites

* **Python:** 3.14+
* **Package Manager:** [uv](https://github.com/astral-sh/uv) (Recommended for speed and reliability).
* **System Libraries:**
    * `tesseract-ocr` (Required for image processing/OCR).

### Install System Dependencies
**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install tesseract-ocr
