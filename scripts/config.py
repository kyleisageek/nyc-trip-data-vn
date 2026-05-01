"""Load config.yaml and resolve ${ENV_VAR} placeholders from environment."""

import os
import re
from pathlib import Path

import yaml
from dotenv import load_dotenv


def _resolve_env_vars(value):
    """Replace ${ENV_VAR} patterns with actual environment variable values."""
    if isinstance(value, str):
        def replacer(match):
            var_name = match.group(1)
            return os.environ.get(var_name, "")
        return re.sub(r"\$\{(\w+)\}", replacer, value)
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def load_config(config_path: str | None = None) -> dict:
    """Load config.yaml, resolve env vars, return config dict."""
    # Load .env from project root
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")

    if config_path is None:
        config_path = str(project_root / "config.yaml")

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    return _resolve_env_vars(raw)
