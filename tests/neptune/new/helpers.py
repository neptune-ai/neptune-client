import contextlib
from tempfile import NamedTemporaryFile
import os


@contextlib.contextmanager
def create_file(content=None, binary_mode=False) -> str:
    """
    A lot this is motivated by:
    Whether the name can be used to open the file a second time,
    while the named temporary file is still open, varies across platforms
    (it can be so used on Unix; it cannot on Windows NT or later).
     ref. https://docs.python.org/3.9/library/tempfile.html#tempfile.NamedTemporaryFile
    """
    if binary_mode:
        mode = 'wb'
    else:
        mode = 'w'
    with NamedTemporaryFile(mode, delete=False) as file:
        if content:
            file.write(content)
        file.close()
        yield file.name
        os.remove(file.name)
