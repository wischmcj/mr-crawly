from __future__ import annotations

import logging
import logging.config
import os

import toml
import yaml

cwd = os.getcwd()
print(f"Current working directory: {cwd}")
# Read in environment variables, set defaults if not present
loc = os.path.dirname(__file__)
print(f"mr-crawly installed at {loc}")

config_file = os.environ.get("MRCRAWLYCONFIG", f"{loc}/config.toml")
log_config = os.environ.get("MRCRAWLY_LOG_CONFIG", f"{loc}/logging_config.yml")

try:
    with open(log_config) as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
except Exception as error:
    print(f"Error loading log config {error}")

log = logging.getLogger(__name__)

try:
    with open(config_file) as f:
        config = toml.load(f)
        root_dir = config["directories"]["root_dir"]
        test_input_dir = config["directories"]["test_input_dir"]
except Exception as e:
    log.error(f"Error loading configuration variables from {config_file}: {e}")
    raise e
