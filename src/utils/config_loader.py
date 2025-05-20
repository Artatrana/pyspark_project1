import os
import yaml

def load_config(path: str):
    base_dir = os.path.dirname(os.path.abspath(os.path.join(__file__, "../../")))  # get to project root
    config_path = os.path.join(base_dir, path)  # use the provided relative path

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    with open(config_path, 'r') as file:
        return yaml.safe_load(file)
