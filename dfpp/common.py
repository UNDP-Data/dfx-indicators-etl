"""
A pyhon module to hold common (between stages) functions for the DFP pipeline
"""

import logging
import re
from configparser import ConfigParser, RawConfigParser
from urllib.parse import urlparse

import tldextract

logger = logging.getLogger(__name__)


def cfg2dict(config_object=None):
    """
    Converts a config object to dict
    :param config_object:
    :return: dict
    """
    output_dict = {}
    sections = config_object.sections()
    for section in sections:
        items = config_object.items(section)
        output_dict[section] = dict(items)
    return output_dict


def dict2cfg(dict_obj=None):
    """
    Converts a dict to a config object
    :param dict_obj:
    :return: ConfigParser object
    """
    config = RawConfigParser()
    for section, items in dict_obj.items():
        config.add_section(section)
        for key, value in items.items():
            config.set(section, key, value)
    return config


async def read_indicator(storage_manager=None, indicator_blob_rel_path=None):
    """
    Read the configuration file of a DFP indicator stored in blob_rel_path
    :param storage_manager:
    :param indicator_blob_rel_path:
    :return: the configuration file as a dict
    """

    logger.info(f"Downloading config for indicator {indicator_blob_rel_path}")

    indicator_cfg_file_exists = await storage_manager.check_blob_exists(
        blob_name=indicator_blob_rel_path
    )
    if not indicator_cfg_file_exists:
        raise FileNotFoundError(
            f"Indicator configuration file {indicator_blob_rel_path} does not exist"
        )
    file_in_bytes = await storage_manager.read_blob(path=indicator_blob_rel_path)

    indicator_parser = ConfigParser(interpolation=None)
    indicator_parser.read_string(file_in_bytes.decode("utf-8"))

    cfg_dict = cfg2dict(indicator_parser)

    assert (
        "indicator" in cfg_dict
    ), f"Indicator config file  {indicator_blob_rel_path} is invalid"
    assert cfg_dict, f"Indicator config file  {indicator_blob_rel_path} is invalid"

    return cfg_dict


def get_domain_from_url(url: str | None) -> str | None:
    """
    Extract the domain (registered domain) from a given URL.
    """
    if url is None:
        return None
    return tldextract.extract(url).registered_domain


def get_netloc_from_url(url: str | None) -> str | None:
    """
    Extract the network location (netloc) from a given URL.
    """
    if url is None:
        return None
    return " ".join(urlparse(url).netloc.split("/")[:2])


def snake_casify(s: str) -> str:
    """
    Convert a string to snake_case
    """
    # Replace any dots, spaces, or non-alphanumeric characters with underscores
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s)

    # Insert an underscore before any uppercase letter that is followed by a lowercase letter or another uppercase letter
    s = re.sub(r"(?<!^)(?=[A-Z])", "_", s)

    # Convert the whole string to lowercase
    return s.lower().strip("_")
