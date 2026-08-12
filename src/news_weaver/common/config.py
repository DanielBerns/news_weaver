"""Configuration management for news_weaver.

Path resolution precedence:
  1. --config CLI argument
  2. CONFIG_FILE environment variable
  3. Default: config.yaml in the current working directory

The API secret_key is NOT stored in config.yaml.
It is read from the NEWS_WEAVER_SECRET_KEY environment variable.
"""
import argparse
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, ValidationError, model_validator

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class DatabaseSettings(BaseModel):
    data_db_url: str
    pipeline_db_url: str


class ApiSettings(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8000
    # secret_key is injected from env; it must NOT appear in config.yaml
    secret_key: str = ""

    @model_validator(mode="after")
    def load_secret_from_env(self) -> "ApiSettings":
        env_key = os.environ.get("NEWS_WEAVER_SECRET_KEY", "")
        if env_key:
            self.secret_key = env_key
        elif not self.secret_key:
            raise ValueError(
                "API secret key is not configured. "
                "Set the NEWS_WEAVER_SECRET_KEY environment variable."
            )
        return self


class LoggingSettings(BaseModel):
    file: str = "pipeline.log"
    level: str = "INFO"


class SystemSettings(BaseModel):
    project_root: str = "."
    uv_path: str = "uv"
    scraped_data_dir: str = "scraped_data"


class AppConfig(BaseModel):
    database: DatabaseSettings
    api: ApiSettings
    logging: LoggingSettings = LoggingSettings()
    system: SystemSettings = SystemSettings()

    @property
    def project_root_path(self) -> Path:
        """Return the project root as an absolute Path."""
        return Path(self.system.project_root).resolve()

    @property
    def scraped_data_path(self) -> Path:
        """Return the scraped_data directory as an absolute Path."""
        p = Path(self.system.scraped_data_dir)
        return p if p.is_absolute() else self.project_root_path / p

    @property
    def log_file_path(self) -> Path:
        """Return the log file as an absolute Path."""
        p = Path(self.logging.file)
        return p if p.is_absolute() else self.project_root_path / p


# ---------------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------------


def resolve_config_path() -> Path:
    """Resolve the configuration file path.

    Precedence:
      1. --config CLI argument
      2. CONFIG_FILE environment variable
      3. Default: config.yaml in CWD
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", help="Path to the YAML configuration file")
    args, _ = parser.parse_known_args()

    if args.config:
        return Path(args.config)
    if env_path := os.environ.get("CONFIG_FILE"):
        return Path(env_path)
    return Path("config.yaml")


def read_config(config_path: Optional[Path] = None) -> AppConfig:
    """Read, parse, and validate the YAML configuration file.

    Args:
        config_path: Override the resolved path (useful in tests).

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        ValidationError: If the configuration is invalid.
    """
    path = config_path if config_path is not None else resolve_config_path()
    if not path.exists():
        raise FileNotFoundError(
            f"Configuration file not found at '{path}'. "
            "Use --config to specify a path or set the CONFIG_FILE env var."
        )
    with path.open("r") as fh:
        raw: dict[str, object] = yaml.safe_load(fh) or {}
    try:
        return AppConfig.model_validate(raw)
    except ValidationError:
        raise


# ---------------------------------------------------------------------------
# Lazy singleton
# ---------------------------------------------------------------------------

_config: Optional[AppConfig] = None


def get_config(config_path: Optional[Path] = None) -> AppConfig:
    """Return the singleton AppConfig, initialising it on first call."""
    global _config
    if _config is None:
        _config = read_config(config_path)
    return _config


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def setup_logger(name: str) -> logging.Logger:
    """Return a configured logger with a RotatingFileHandler and StreamHandler.

    Uses the global AppConfig singleton. Safe to call multiple times with the
    same name (subsequent calls return the cached Logger instance).
    """
    cfg = get_config()
    log_level = getattr(logging, cfg.logging.level.upper(), logging.INFO)

    fmt = (
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s",'
        ' "component": "%(name)s", "message": "%(message)s"}'
    )

    logger = logging.getLogger(name)
    if logger.handlers:
        # Already configured — return existing logger.
        return logger

    logger.setLevel(log_level)

    # File handler with rotation (10 MB per file, keep 5 backups)
    log_path = cfg.log_file_path
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(fmt))

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(fmt))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
