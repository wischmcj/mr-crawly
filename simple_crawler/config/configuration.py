from __future__ import annotations

import logging
import logging.config
import os
import sys

import toml
import yaml

cwd = os.getcwd()
loc = os.path.dirname(os.path.dirname(__file__))
sys.path.append(loc)
# sys.path.append(os.path.dirname(cwd))

config_file = os.environ.get("MRCRAWLYCONFIG", f"{loc}/config/config.toml")
log_config = os.environ.get("MRCRAWLY_LOG_CONFIG", f"{loc}/config/logging_config.yml")


def _load_console_log():
    with open(log_config) as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)


def get_logger(logger_name: str, log_file: str = None, log_level: int = logging.INFO):
    """
    Returns a logger with at least the default console handler.
    If log_file is provided, it will either:
     - add a file handler to the logger if one doesn't exist, or
     - repoint the file handler to a new file
    Likewise, if the log level is provided, it will update the log level of both handlers.
    """
    logger = logging.getLogger(logger_name)
    return_logger = logger
    has_file_handler = any(
        isinstance(handler, logging.FileHandler) for handler in logger.handlers
    )
    has_console_handler = any(
        isinstance(handler, logging.StreamHandler) for handler in logger.handlers
    )
    if not has_console_handler:
        _load_console_log()
        return_logger = logging.getLogger(logger_name)
        for handler in return_logger.handlers:
            handler.setLevel(log_level)
    if log_file is not None:
        if has_file_handler:
            file_handler = [
                x for x in logger.handlers if isinstance(x, logging.FileHandler)
            ][0]
            if file_handler.stream.name != log_file:
                file_handler.stream = open(log_file, "w")
            if file_handler.level != log_level:
                file_handler.setLevel(log_level)
            return return_logger
        else:
            file_handler = logging.FileHandler(log_file, "w")
            file_handler.setLevel(log_level)
            return_logger.addHandler(file_handler)
            return return_logger
    else:
        return return_logger


def get_config(**kwargs):
    """
    Returns the config values set in the yaml file as a dict.
    If kwargs are provided these will be added to the config dict,
        overwriting any existing values with the same key.
    """
    try:
        with open(config_file) as f:
            config = toml.load(f)
        for key, value in kwargs.items():
            if key in config:
                config[key] = value
            else:
                raise ValueError(f"Invalid configuration key: {key}")
        return config
    except Exception as e:
        print(f"Error loading configuration variables from {config_file}: {e}")
        raise e
