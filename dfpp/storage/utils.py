import ast
import configparser

__all__ = [
    "UnescapedConfigParser",
    "flatten_dict_config",
]


class UnescapedConfigParser(configparser.RawConfigParser):
    """
    An extension of the RawConfigParser that does not escape values when reading from a file.
    """

    def get(self, section, option, **kwargs):
        """
        Get the value of an option.
        :param section: The section of the config file.
        :param option: The option to get the value of.
        :param kwargs: The keyword arguments.
        :return:
        """
        value = super().get(section, option, **kwargs)
        try:
            return value.encode().decode("unicode_escape")
        except AttributeError:
            return value


def flatten_dict_config(config_dict: dict) -> dict:
    """unnest section from config is section is source or indicator"""
    config = {}
    for k, v in config_dict.items():
        if k not in ["source", "indicator"]:
            config[k] = v
        else:
            config.update(v)
    return config
