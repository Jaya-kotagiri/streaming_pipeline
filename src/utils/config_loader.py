"""
Config loader for the FileChange Stream Toolkit.

Loads config/config.yaml and applies environment variable overrides of the
form FCSTREAM_<SECTION>_<KEY> (case-insensitive on the env var name lookup).

Example:
    export FCSTREAM_KAFKA_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092
    export FCSTREAM_DATABASE_PROD_PASSWORD=supersecret
"""

import os
from pathlib import Path
from typing import Any, Dict

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
ENV_PREFIX = "FCSTREAM_"


def _apply_env_overrides(config: Dict[str, Any], prefix: list) -> Dict[str, Any]:
    """Recursively walk the config dict and replace values if a matching
    environment variable is set."""
    for key, value in config.items():
        path = prefix + [key]
        if isinstance(value, dict):
            _apply_env_overrides(value, path)
        else:
            env_var = ENV_PREFIX + "_".join(p.upper() for p in path)
            if env_var in os.environ:
                raw = os.environ[env_var]
                config[key] = _coerce(raw, value)
    return config


def _coerce(raw: str, original: Any) -> Any:
    """Cast the env var string to match the type of the original yaml value."""
    if isinstance(original, bool):
        return raw.lower() in ("1", "true", "yes", "on")
    if isinstance(original, int):
        try:
            return int(raw)
        except ValueError:
            return raw
    if isinstance(original, float):
        try:
            return float(raw)
        except ValueError:
            return raw
    return raw


def load_config(path: str = None) -> Dict[str, Any]:
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return _apply_env_overrides(config, [])


if __name__ == "__main__":
    import json

    cfg = load_config()
    print(json.dumps(cfg, indent=2))
