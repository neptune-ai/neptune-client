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
import os
import threading

from neptune import envs, projects, experiments
from neptune.exceptions import MissingProjectQualifiedName, Uninitialized
from neptune.sessions import Session

session = None
project = None

__lock = threading.RLock()


def init(project_qualified_name=None, api_token=None, proxies=None):
    if project_qualified_name is None:
        project_qualified_name = os.getenv(envs.PROJECT_ENV_NAME)

    # pylint: disable=global-statement
    with __lock:
        global session, project

        session = Session(api_token=api_token, proxies=proxies)

        if project_qualified_name is None:
            raise MissingProjectQualifiedName()

        project = session.get_project(project_qualified_name)

        return project


def set_project(project_qualified_name):
    # pylint: disable=global-statement
    with __lock:
        global session, project

        if session is None:
            session = init(project_qualified_name=project_qualified_name)
        else:
            project = session.get_project(project_qualified_name)

        return project


def create_experiment(name=None,
                      description=None,
                      params=None,
                      properties=None,
                      tags=None,
                      upload_source_files=None,
                      abort_callback=None,
                      upload_stdout=True,
                      upload_stderr=True,
                      send_hardware_metrics=True,
                      run_monitoring_thread=True,
                      handle_uncaught_exceptions=True,
                      git_info=None,
                      hostname=None):
    # pylint: disable=global-statement
    global project
    if project is None:
        raise Uninitialized()

    return project.create_experiment(
        name=name,
        description=description,
        params=params,
        properties=properties,
        tags=tags,
        upload_source_files=upload_source_files,
        abort_callback=abort_callback,
        upload_stdout=upload_stdout,
        upload_stderr=upload_stderr,
        send_hardware_metrics=send_hardware_metrics,
        run_monitoring_thread=run_monitoring_thread,
        handle_uncaught_exceptions=handle_uncaught_exceptions,
        git_info=git_info,
        hostname=hostname
    )


get_experiment = experiments.get_current_experiment


def append_tag(tag):
    get_experiment().append_tag(tag)


def remove_tag(tag):
    get_experiment().remove_tag(tag)


def set_property(key, value):
    get_experiment().set_property(key, value)


def remove_property(key):
    get_experiment().remove_property(key)


def send_metric(channel_name, x, y=None, timestamp=None):
    return get_experiment().send_metric(channel_name, x, y, timestamp)


def send_text(channel_name, x, y=None, timestamp=None):
    return get_experiment().send_text(channel_name, x, y, timestamp)


def send_image(channel_name, x, y=None, name=None, description=None, timestamp=None):
    return get_experiment().send_image(channel_name, x, y, name, description, timestamp)


def send_artifact(artifact):
    return get_experiment().send_artifact(artifact)


def stop(traceback=None):
    get_experiment().stop(traceback)
