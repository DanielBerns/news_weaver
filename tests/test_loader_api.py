"""Tests for the FastAPI Loader API endpoints."""
from pathlib import Path
from typing import Generator

import pytest
import yaml
from fastapi.testclient import TestClient

import news_weaver.common.config as config_module
from news_weaver.common.config import read_config

SECRET = "test-api-secret"
AUTH_HEADERS = {"X-API-Key": SECRET}
BAD_HEADERS = {"X-API-Key": "wrong-key"}


@pytest.fixture(autouse=True)
def setup_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator:
    """Provide a valid in-memory config and reset the singleton."""
    config_module._config = None
    monkeypatch.setenv("NEWS_WEAVER_SECRET_KEY", SECRET)
    data = {
        "database": {
            "data_db_url": "sqlite:///:memory:",
            "pipeline_db_url": "sqlite:///:memory:",
        },
        "api": {"host": "127.0.0.1", "port": 9000},
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(data))
    config_module._config = read_config(config_path=cfg_file)
    yield
    config_module._config = None


@pytest.fixture()
def client() -> Generator[TestClient, None, None]:
    """Create a fresh FastAPI TestClient backed by an in-memory DB."""
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool

    from news_weaver.loader import Base, app, reset_loader_engine

    # Reset and wire a StaticPool in-memory engine
    reset_loader_engine()
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    # Monkey-patch the loader's engine so the app uses our test engine
    import news_weaver.loader as loader_module

    loader_module._engine = engine
    loader_module._SessionLocal = None  # force session factory rebuild

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with TestClient(app) as c:
        yield c

    Base.metadata.drop_all(engine)
    reset_loader_engine()


# ---------------------------------------------------------------------------
# /articles
# ---------------------------------------------------------------------------


def test_create_article_returns_201(client: TestClient) -> None:
    resp = client.post(
        "/articles",
        json={"source_file_id": 1, "url": "https://example.com", "content": "Hello"},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 201
    assert resp.json()["status"] == "success"


def test_create_article_duplicate_returns_exists(client: TestClient) -> None:
    payload = {
        "source_file_id": 2,
        "url": "https://example.com",
        "content": "Hello",
    }
    client.post("/articles", json=payload, headers=AUTH_HEADERS)
    resp = client.post("/articles", json=payload, headers=AUTH_HEADERS)
    assert resp.json()["status"] == "exists"


def test_create_article_invalid_key_returns_403(client: TestClient) -> None:
    resp = client.post(
        "/articles",
        json={"source_file_id": 3, "url": "https://example.com", "content": "Hi"},
        headers=BAD_HEADERS,
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# /documents
# ---------------------------------------------------------------------------


def test_create_document_returns_201(client: TestClient) -> None:
    resp = client.post(
        "/documents",
        json={
            "source_file_id": 10,
            "url": "https://example.com/doc.pdf",
            "filename": "doc.pdf",
            "mimetype": "application/pdf",
            "content": "document text",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 201
    assert resp.json()["status"] == "success"


# ---------------------------------------------------------------------------
# /images
# ---------------------------------------------------------------------------


def test_create_image_returns_201(client: TestClient) -> None:
    resp = client.post(
        "/images",
        json={
            "source_file_id": 20,
            "url": "https://example.com/img.png",
            "mimetype": "image/png",
            "extracted_text": "some text",
            "detected_objects": [],
            "image_metadata": {},
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 201
    assert resp.json()["status"] == "success"


# ---------------------------------------------------------------------------
# /spreadsheets
# ---------------------------------------------------------------------------


def test_create_spreadsheet_returns_201(client: TestClient) -> None:
    resp = client.post(
        "/spreadsheets",
        json={
            "source_file_id": 30,
            "url": "https://example.com/data.xlsx",
            "filename": "data.xlsx",
            "mimetype": (
                "application/vnd.openxmlformats-officedocument"
                ".spreadsheetml.sheet"
            ),
            "data_json": [{"col1": "val1", "col2": "val2"}],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 201
    assert resp.json()["status"] == "success"
