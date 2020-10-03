#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from neptune import envs


class NeptuneException(Exception):
    BASE_MESSAGE = """\n
Neptune team here, seems like there are some problems, sorry.
No worries thought, in most cases you can fix them by:
    - Finding an answer in the docs -> https://docs.neptune.ai
    - Getting help from the team (we are friendly) -> https://docs.neptune.ai/getting-started/getting-help.html
    """
    pass


class Uninitialized(NeptuneException):
    def __init__(self):
        message = """This particular error is about:
    - Problem -> You must initialize `neptune` object before you start logging to it. 
    - Solution -> Run `neptune.init()`. Remember to specify the `project_qualified_name' and 'api_token'. 
    - Relevant docs page -> https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html
    """
        super(Uninitialized, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class FileNotFound(NeptuneException):
    def __init__(self, path):
        message = """This particular error is about:
    - Problem -> File {} doesn't exist.
    - Solution - > Make sure that the path to the file is correct. 
    - Relevant docs page -> https://docs.neptune.ai/logging-and-managing-experiment-results/index.html
        """.format(path)
        super(FileNotFound, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class NotAFile(NeptuneException):
    def __init__(self, path):
        message = """This particular error is about:
    - Problem -> Path {} is not a file.
    - Solution - > Check if the path you specified is a file and not a directory. 
    - Relevant docs page -> https://docs.neptune.ai/logging-and-managing-experiment-results/index.html
            """.format(path)
        super(NotAFile, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class NotADirectory(NeptuneException):
    def __init__(self, path):
        message = """This particular error is about:
    - Problem -> Path {} is not a directory.
    - Solution - > Check if the path you specified is a directory and not a file. 
    - Relevant docs page -> https://docs.neptune.ai/logging-and-managing-experiment-results/index.html
                """.format(path)
        super(NotADirectory, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class InvalidNotebookPath(NeptuneException):
    def __init__(self, path):
        message = """This particular error is about:
    - Problem -> File {} is not a valid notebook.
    - Solution - > Check if the file you chose ends with .ipynb.
    - Relevant docs page -> https://docs.neptune.ai/keep-track-of-jupyter-notebooks/index.html#managing-notebooks-in-neptune-using-the-cli
        """.format(path)
        super(InvalidNotebookPath, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class InvalidChannelX(NeptuneException):
    def __init__(self, x):
        message = """This particular error is about:
    - Problem -> Invalid channel X-coordinate: '{}'. The sequence of X-coordinates must be strictly increasing.
    - Solution - > Make sure that you don't log multiple values of a metric at the same step x. You can log explicitly with neptune.log_metric('metric_name', x=step, y=metric_value).
    - Relevant docs page -> https://docs.neptune.ai/api-reference/neptune/experiments/index.html?highlight=log_metric#neptune.experiments.Experiment.log_metric
        """.format(x)
        super(InvalidChannelX, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class NoChannelValue(NeptuneException):
    def __init__(self):
        message = """This particular error is about:
    - Problem -> No channel value provided.
    - Solution - > You need to log some values before you fetch them. You can do that with `neptune.log_metric()`. 
    - Relevant docs page -> https://docs.neptune.ai/logging-and-managing-experiment-results/logging-experiment-data/index.html
        """
        super(NoChannelValue, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class LibraryNotInstalled(NeptuneException):
    def __init__(self, library):
        message = """This particular error is about:
    - Problem -> Library {0} is not installed.
    - Solution - > You need to install the library first. Running `pip install {0}` will do the trick. 
    - Relevant docs page -> https://docs.neptune.ai/getting-started/installation/index.html
        """.format(library)
        super(LibraryNotInstalled, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class InvalidChannelValue(NeptuneException):
    def __init__(self, expected_type, actual_type):
        message = """This particular error is about:
    - Problem -> Invalid channel value type. Expected: {expected}, actual: {actual}.
    - Solution - > You need to log objects with appropriate function. 
                    - `neptune.log_metric()` for metrics, 
                    - `neptune.log_image()` for images,
                    - `neptune.log_text()` for text,
                    - `neptune.log_artifact()` for files,
    - Relevant docs page -> https://docs.neptune.ai/logging-and-managing-experiment-results/logging-experiment-data/what-can-you-log-to-experiments.html
         """.format(expected=expected_type, actual=actual_type)
        super(InvalidChannelValue, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class NoExperimentContext(NeptuneException):
    def __init__(self):
        message = """This particular error is about:
    - Problem -> Unable to find current active experiment. 
    - Solution - > You need to create an experiment before you log things. Run `neptune.create_experiment()`. 
    - Relevant docs page -> https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html
        """
        super(NoExperimentContext, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class MissingApiToken(NeptuneException):
    def __init__(self):
        message = """This particular error is about:
    - Problem -> Missing API token.
    - Solution - > Use "{}" environment variable or pass it as an argument to neptune.init.
    - Relevant docs page -> https://docs.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html
            """.format(envs.API_TOKEN_ENV_NAME)
        super(MissingApiToken, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class MissingProjectQualifiedName(NeptuneException):
    def __init__(self):
        message = """This particular error is about:
    - Problem -> Missing project qualified name.
    - Solution - > Use "{}" environment variable or pass it as a `project_qualified_name` argument.
    - Relevant docs page -> https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html
        """.format(envs.PROJECT_ENV_NAME)
        super(MissingProjectQualifiedName, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class IncorrectProjectQualifiedName(NeptuneException):
    def __init__(self, project_qualified_name):
        message = """This particular error is about:
    - Problem -> Incorrect project qualified name "{}". 
    - Solution - > Should be in format "workspace/project_name".
    - Relevant docs pages: 
        - Quickstart -> https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html
        - Projects in Neptune -> https://docs.neptune.ai/workspace-project-and-user-management/projects/index.html
        """.format(project_qualified_name)
        super(IncorrectProjectQualifiedName, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class InvalidNeptuneBackend(NeptuneException):
    def __init__(self, provided_backend_name):
        message = """This particular error is about:
    - Problem -> Unknown {0} "{1}". e.g. using {0}=offline allows you to run your code without logging anything to Neptune.
    - Solution - > Specify the correct backend when you initialize neptune or via environment variable. You can choose:
        - Hosted Backed -> neptune.init(backend=HostedNeptuneBackend(proxies=...)) or `export NEPTUNE_BACKEND=hosted`
        - Offline Backed -> neptune.init(backend=OfflineBackend(proxies=...)) or `export NEPTUNE_BACKEND=offline`
    - Relevant docs page -> https://docs.neptune.ai/api-reference/neptune/index.html#neptune.init
            """.format(envs.BACKEND, provided_backend_name)
        super(InvalidNeptuneBackend, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class DeprecatedApiToken(NeptuneException):
    def __init__(self, app_url):
        message = """This particular error is about:
        - Problem -> Your API token is deprecated.
        - Solution - > Please visit {} to get a new one.
        - Relevant docs page -> https://docs.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html
            """.format(app_url)
        super(DeprecatedApiToken, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class CannotResolveHostname(NeptuneException):
    def __init__(self, host):
        message = """This particular error is about:
        - Problem -> Cannot resolve hostname {}.
        - Solution - > Please contact Neptune support.
        - Relevant docs page -> https://docs.neptune.ai/getting-started/getting-help.html
            """.format(host)
        super(CannotResolveHostname, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))


class UnsupportedClientVersion(NeptuneException):
    def __init__(self, version, minVersion, maxVersion):
        message = """This particular error is about:
        - Problem -> This client version ({}) is not supported.
        - Solution - > Please install neptune-client{}. Simply run `pip install neptune-client --upgrade`. 
        - Relevant docs page -> https://docs.neptune.ai/getting-started/getting-help.html
            """.format(
            version,
            "==" + str(maxVersion) if maxVersion else ">=" + str(minVersion)
        )
        super(UnsupportedClientVersion, self).__init__('\n'.join([NeptuneException.BASE_MESSAGE, message]))
