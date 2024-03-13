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
import functools
import re

import bravado_core.model
from bravado_core.model import (
    _bless_models,
    _collect_models,
    _get_unprocessed_uri,
    _post_process_spec,
    _tag_models,
)


def _run_post_processing(spec):
    visited_models = {}

    def _call_post_process_spec(spec_dict):
        # Discover all the models in spec_dict
        _post_process_spec(
            spec_dict=spec_dict,
            spec_resolver=spec.resolver,
            on_container_callbacks=[
                functools.partial(
                    _tag_models,
                    visited_models=visited_models,
                    swagger_spec=spec,
                ),
                functools.partial(
                    _bless_models,
                    visited_models=visited_models,
                    swagger_spec=spec,
                ),
                functools.partial(
                    _collect_models,
                    models=spec.definitions,
                    swagger_spec=spec,
                ),
            ],
        )

    # Post process specs to identify models
    _call_post_process_spec(spec.spec_dict)

    processed_uris = {
        uri
        for uri in spec.resolver.store
        if uri == spec.origin_url or re.match(r"http(s)?://json-schema\.org/draft(/\d{4})?-\d+/(schema|meta/.*)", uri)
    }
    additional_uri = _get_unprocessed_uri(spec, processed_uris)
    while additional_uri is not None:
        # Post process each referenced specs to identify models in definitions of linked files
        with spec.resolver.in_scope(additional_uri):
            _call_post_process_spec(
                spec.resolver.store[additional_uri],
            )

        processed_uris.add(additional_uri)
        additional_uri = _get_unprocessed_uri(spec, processed_uris)


# Issue: https://github.com/Yelp/bravado-core/issues/388
# Bravado currently makes additional requests to `json-schema.org` in order to gather mission schemas
# This makes `neptune` unable to run without internet connection or with a many security policies
def patch():
    bravado_core.model._run_post_processing = _run_post_processing
