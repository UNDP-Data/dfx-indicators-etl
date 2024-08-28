import configparser

__all__ = [
    "UnescapedConfigParser"
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
