# Configuration loader
import yaml

def load_config(file_path: str) -> dict:
    """Load YAML config file."""
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

def load_schema(schema_path: str = "config/schema.yml") -> dict:
    """Load warehouse schema."""
    return load_config(schema_path)
