#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
__all__ = ["init_run"]

from typing import (
    List,
    Optional,
    Union,
)

from neptune.internal.init.parameters import DEFAULT_FLUSH_PERIOD
from neptune.metadata_containers import Run


def init_run(
    with_id: Optional[str] = None,
    *,
    project: Optional[str] = None,
    api_token: Optional[str] = None,
    custom_run_id: Optional[str] = None,
    mode: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[Union[List[str], str]] = None,
    source_files: Optional[Union[List[str], str]] = None,
    capture_stdout: Optional[bool] = None,
    capture_stderr: Optional[bool] = None,
    capture_hardware_metrics: Optional[bool] = None,
    fail_on_exception: bool = True,
    monitoring_namespace: Optional[str] = None,
    flush_period: float = DEFAULT_FLUSH_PERIOD,
    proxies: Optional[dict] = None,
    capture_traceback: bool = True,
    **kwargs,
) -> Run:
    return Run(
        with_id=with_id,
        project=project,
        api_token=api_token,
        custom_run_id=custom_run_id,
        mode=mode,
        name=name,
        description=description,
        tags=tags,
        source_files=source_files,
        capture_stdout=capture_stdout,
        capture_stderr=capture_stderr,
        capture_hardware_metrics=capture_hardware_metrics,
        fail_on_exception=fail_on_exception,
        monitoring_namespace=monitoring_namespace,
        flush_period=flush_period,
        proxies=proxies,
        capture_traceback=capture_traceback,
        **kwargs,
    )
