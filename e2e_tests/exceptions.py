from neptune.new.exceptions import NeptuneException


class ConfigurationException(NeptuneException):
    def __init__(self, msg):
        super().__init__(msg)


class MissingEnvironmentVariable(ConfigurationException):
    def __init__(self, missing_variable):
        msg = f"Missing '{missing_variable}' in env configuration"
        super().__init__(msg)
