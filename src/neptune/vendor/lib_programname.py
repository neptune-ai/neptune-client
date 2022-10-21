# MIT License
#
# Copyright (c) 1990-2022 Robert Nowotny
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# flake8: noqa

import pathlib
import sys

import __main__  # noqa

__all__ = ["empty_path", "get_path_executed_script"]

empty_path = pathlib.Path()


def get_path_executed_script() -> pathlib.Path:
    """
    getting the full path of the program from which a Python module is running

    >>> ### TEST get it via __main__.__file__
    >>> # Setup
    >>> # force __main__.__file__ valid
    >>> save_main_file = str(__main__.__file__)
    >>> __main__.__file__ = __file__

    >>> # Test via __main__.__file__
    >>> assert get_path_executed_script() == pathlib.Path(__file__).resolve()


    >>> ### TEST get it via sys.argv
    >>> # Setup
    >>> # force __main__.__file__ invalid
    >>> __main__.__file__ = str((pathlib.Path(__file__).parent / 'invalid_file.py'))  # .resolve() seems not to work on a non existing file in python 3.5

    >>> # force sys.argv valid
    >>> save_sys_argv = list(sys.argv)
    >>> valid_path = str((pathlib.Path(__file__).resolve()))
    >>> sys.argv = [valid_path]

    >>> # Test via sys.argv
    >>> assert get_path_executed_script() == pathlib.Path(__file__).resolve()


    >>> ### TEST get it via stack
    >>> # Setup
    >>> # force sys.argv invalid
    >>> invalid_path = str((pathlib.Path(__file__).parent / 'invalid_file.py'))  # .resolve() seems not to work on a non existing file in python 3.5
    >>> sys.argv = [invalid_path]


    >>> assert get_path_executed_script()

    >>> # teardown
    >>> __main__.__file__ = save_main_file
    >>> sys.argv = list(save_sys_argv)

    """

    # try to get it from __main__.__file__ - does not work under pytest, doctest
    path_candidate = get_fullpath_from_main_file()
    if path_candidate != empty_path:
        return path_candidate

    # try to get it from sys_argv - does not work when loaded from uwsgi, works in eclipse and pydev
    path_candidate = get_fullpath_from_sys_argv()
    if path_candidate != empty_path:
        return path_candidate

    return empty_path


def get_fullpath_from_main_file() -> pathlib.Path:
    """try to get it from __main__.__file__ - does not work under pytest, doctest

    >>> # test no attrib __main__.__file__
    >>> save_main_file = str(__main__.__file__)
    >>> delattr(__main__, '__file__')
    >>> assert get_fullpath_from_main_file() == empty_path
    >>> setattr(__main__, '__file__', save_main_file)

    """
    if not hasattr(sys.modules["__main__"], "__file__"):
        return empty_path

    arg_string = str(sys.modules["__main__"].__file__)
    valid_executable_path = get_valid_executable_path_or_empty_path(arg_string)
    return valid_executable_path


def get_fullpath_from_sys_argv() -> pathlib.Path:
    """try to get it from sys_argv - does not work when loaded from uwsgi, works in eclipse and pydev

    >>> # force test invalid sys.path
    >>> save_sys_argv = list(sys.argv)
    >>> invalid_path = str((pathlib.Path(__file__).parent / 'invalid_file.py'))  # .resolve() seems not to work on a non existing file in python 3.5
    >>> sys.argv = [invalid_path]
    >>> assert get_fullpath_from_sys_argv() == pathlib.Path()
    >>> sys.argv = list(save_sys_argv)

    >>> # force test valid sys.path
    >>> save_sys_path = list(sys.argv)
    >>> valid_path = str((pathlib.Path(__file__).resolve()))
    >>> sys.argv = [valid_path]
    >>> assert get_fullpath_from_sys_argv() == pathlib.Path(valid_path)
    >>> sys.argv = list(save_sys_argv)


    """

    for arg_string in sys.argv:
        valid_executable_path = get_valid_executable_path_or_empty_path(arg_string)
        if valid_executable_path != empty_path:
            return valid_executable_path
    return empty_path


def get_valid_executable_path_or_empty_path(arg_string: str) -> pathlib.Path:
    arg_string = remove_doctest_and_docrunner_parameters(arg_string)
    arg_string = add_python_extension_if_not_there(arg_string)
    path = pathlib.Path(arg_string)
    if path.is_file():
        path = path.resolve()  # .resolve does not work on a non existing file in python 3.5
        return path
    else:
        return empty_path


def remove_doctest_and_docrunner_parameters(arg_string: str) -> str:
    """
    >>> # Setup
    >>> arg_string_with_parameter = __file__ + '::::::some docrunner parameter'
    >>> arg_string_without_parameter = __file__

    >>> # Test with and without docrunner parameters
    >>> assert remove_doctest_and_docrunner_parameters(arg_string_with_parameter) == __file__
    >>> assert remove_doctest_and_docrunner_parameters(arg_string_without_parameter) == __file__
    """
    path = arg_string.split("::", 1)[0]
    return path


def add_python_extension_if_not_there(arg_string: str) -> str:
    """
    >>> # Setup
    >>> arg_string_with_py = __file__
    >>> arg_string_without_py = __file__.rsplit('.py',1)[0]

    >>> # Test with and without .py suffix
    >>> assert add_python_extension_if_not_there(arg_string_with_py) == __file__
    >>> assert add_python_extension_if_not_there(arg_string_without_py) == __file__

    """

    if not arg_string.endswith(".py"):
        arg_string = arg_string + ".py"
    return arg_string
