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
from neptune import projects, experiments
from neptune.internal.structs.stack import Stack
from neptune.sessions import Session

session = None
project = None


def init(api_token=None, project_qualified_name=None):
    # pylint: disable=global-statement
    global session, project

    session = Session(api_token=api_token)

    if project_qualified_name is not None:
        project = session.get_project(project_qualified_name)
    else:
        project = session.get_default_project()

    return session


def set_project(project_qualified_name):
    # pylint: disable=global-statement
    global session, project

    if session is None:
        session = init(project_qualified_name=project_qualified_name)
    else:
        project = session.get_project(project_qualified_name)

    return project


def create_experiment(name="Untitled",
                      description=None,
                      params=None,
                      properties=None,
                      tags=None,
                      upload_source_files=None,
                      abort_callback=None,
                      send_hardware_metrics=True,
                      run_monitoring_thread=True,
                      handle_uncaught_exceptions=True):
    # pylint: disable=global-statement
    global project
    return experiments.push_new_experiment(
        project.create_experiment(
            name=name,
            description=description,
            params=params,
            properties=properties,
            tags=tags,
            upload_source_files=upload_source_files,
            abort_callback=abort_callback,
            send_hardware_metrics=send_hardware_metrics,
            run_monitoring_thread=run_monitoring_thread,
            handle_uncaught_exceptions=handle_uncaught_exceptions
        )
    )


def get_experiment():
    return experiments.current_experiment


def send_metric(channel_name, x, y=None):
    return get_experiment().send_metric(channel_name, x, y)


def send_text(channel_name, x, y=None):
    return get_experiment().send_text(channel_name, x, y)


def send_image(channel_name, x, y=None, name=None, description=None):
    return get_experiment().send_image(channel_name, x, y, name, description)


def send_artifact(artifact):
    return get_experiment().send_artifact(artifact)


def stop(traceback=None):
    get_experiment().stop(traceback)
