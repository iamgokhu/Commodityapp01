import yaml
from pathlib import Path
from typing import Dict, Any


def load_config(config_path: str) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def get_data_sources_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return config.get("data_sources", {})


def get_rate_limiting_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return config.get("rate_limiting", {})