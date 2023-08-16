class ConfigError(Exception):
    pass


class DFPSourceError(Exception):
    pass


class TransformationError(Exception):
    pass


class TransformationWarning(Warning):
    pass


class PublishError(Exception):
    pass


class SourceDoesNotExist(Exception):
    pass


class AggregationError(Exception):
    pass
