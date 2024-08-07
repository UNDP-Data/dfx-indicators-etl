import ast
import configparser

__all__ = [
    "UnescapedConfigParser",
    "validate_src_cfg",
]

MANDATORY_SOURCE_COLUMNS = "id", "url", "save_as"


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


def validate_src_cfg(cfg_dict=None, cfg_file_path=None):
    """
    Validate a source config file
    :param cfg_dict:
    :param cfg_file_path:
    :return:
    """
    assert cfg_dict is not None, f"Invalid source config {cfg_dict}"
    assert cfg_dict != {}, f"Invalid source config {cfg_dict}"

    for k in MANDATORY_SOURCE_COLUMNS:
        v = cfg_dict[k]
        try:
            v_parsed = ast.literal_eval(v)
            message = f"{k} key {cfg_file_path} needs to be a valid string. Current value is {v}"
            assert v_parsed is not None, message
        except AssertionError:
            raise
        except Exception as e:
            pass

        message = f"{cfg_file_path} needs to contain {k} key"
        assert k in cfg_dict, message

        message = f"{cfg_file_path}'s {k} key needs to be a string. Current value is {type(v)}"
        assert isinstance(v, str), message

        message = f"{cfg_file_path}'s {k} key needs to be a valid string. Current value is {v}"
        assert v, message
