import logging
import os
import sys
import yaml

def load_config(config_path: str = "config.yaml") -> dict:
    # Look for config in root if running from src
    if not os.path.exists(config_path):
        # Fallback check for common project structures
        up_one = os.path.join("..", config_path)
        if os.path.exists(up_one):
            config_path = up_one
        else:
            print(f"Error: {config_path} not found.")
            sys.exit(1)

    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# Global Config Object
CONFIG = load_config()

def setup_logger(name: str):
    """Returns a configured logger instance."""
    logging.basicConfig(
        filename=CONFIG["logging"]["file"],
        level=getattr(logging, CONFIG["logging"]["level"].upper(), logging.INFO),
        format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "%(name)s", "message": "%(message)s"}'
    )
    return logging.getLogger(name)
