#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
import pkgutil
import inspect
import importlib

EXCLUDED_MODULES = [
    'neptune.internal',
    'neptune.new.internal',
    'neptune.vendor.pynvml',
    'neptune.management.internal',
]


def extract_module_classes(module_name: str):
    for module_info in pkgutil.iter_modules([module_name]):
        if module_info.name.startswith('_'):
            continue

        if module_info.ispkg:
            for module_dotted, classname in extract_module_classes(f'{module_name}/{module_info.name}'):
                yield (module_dotted, classname)
        else:
            module_dotted = module_name.replace('/', '.') + '.' + module_info.name
            # print(module_dotted)
            module = importlib.import_module(module_dotted)
            for classname, _ in inspect.getmembers(module, inspect.isclass):
                yield (module_dotted, classname)


if __name__ == '__main__':
    print("""#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
# pylint: disable=unused-import,reimported,import-error
import unittest

""")
    for module_name, classname in sorted(list(extract_module_classes('neptune'))):
        excluded = False

        for excluded_module in EXCLUDED_MODULES:
            if module_name.startswith(excluded_module):
                excluded = True
                break

        if not excluded:
            print('from', module_name, 'import', classname)

    print("""

class TestImports(unittest.TestCase):
    def test_imports(self):
        pass""")
