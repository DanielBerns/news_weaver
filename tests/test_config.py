"""Tests for configuration loading and validation."""
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

# Reset the singleton between tests
import news_weaver.common.config as config_module
from news_weaver.common.config import AppConfig, read_config


@pytest.fixture(autouse=True)
def reset_config_singleton():
    """Reset the lazy config singleton before each test."""
    config_module._config = None
    yield
    config_module._config = None


@pytest.fixture()
def minimal_config_yaml(tmp_path: Path) -> Path:
    """Write a minimal valid config.yaml to a temp directory."""
    data = {
        "database": {
            "data_db_url": "sqlite:///test_data.db",
            "pipeline_db_url": "sqlite:///test_pipeline.db",
        },
        "api": {
            "host": "127.0.0.1",
            "port": 9000,
        },
    }
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(data))
    return config_file


def test_read_config_raises_when_file_missing(tmp_path: Path) -> None:
    """read_config() must raise FileNotFoundError for a missing file."""
    missing = tmp_path / "nonexistent.yaml"
    with pytest.raises(FileNotFoundError, match="not found"):
        read_config(config_path=missing)


def test_read_config_validates_correct_config(
    minimal_config_yaml: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """read_config() returns a valid AppConfig for a well-formed YAML."""
    monkeypatch.setenv("NEWS_WEAVER_SECRET_KEY", "test-secret")
    cfg = read_config(config_path=minimal_config_yaml)
    assert isinstance(cfg, AppConfig)
    assert cfg.database.data_db_url == "sqlite:///test_data.db"
    assert cfg.api.host == "127.0.0.1"
    assert cfg.api.port == 9000
    assert cfg.api.secret_key == "test-secret"


def test_read_config_raises_when_secret_key_missing(
    minimal_config_yaml: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AppConfig validation must fail when NEWS_WEAVER_SECRET_KEY is not set."""
    monkeypatch.delenv("NEWS_WEAVER_SECRET_KEY", raising=False)
    with pytest.raises(ValidationError, match="secret key"):
        read_config(config_path=minimal_config_yaml)


def test_read_config_raises_on_invalid_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """read_config() must raise ValidationError when required keys are absent."""
    monkeypatch.setenv("NEWS_WEAVER_SECRET_KEY", "test-secret")
    bad_config = tmp_path / "bad.yaml"
    bad_config.write_text("logging:\n  level: DEBUG\n")  # missing 'database' section
    with pytest.raises(ValidationError):
        read_config(config_path=bad_config)


def test_project_root_path_is_absolute(
    minimal_config_yaml: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AppConfig.project_root_path must return an absolute Path."""
    monkeypatch.setenv("NEWS_WEAVER_SECRET_KEY", "test-secret")
    cfg = read_config(config_path=minimal_config_yaml)
    assert cfg.project_root_path.is_absolute()
