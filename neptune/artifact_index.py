import hashlib

from pathlib import Path
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from typing import List, Dict, Optional
from marshmallow import fields


# Side note: we should consider using dataclasses_json and marshmallow as Python's equivalents of Jackson.
# They allow for (de-)serializing graphs of objects to and from JSON without writing boilerplate code.
@dataclass_json
@dataclass
class File:
    """
    A file entry in the index.

    rel_path - file's path relative to the FileSet directory root.
    sha1 - file's SHA1
    ctime - file's modification time when its SHA was last calculated. Allows for caching SHAs.
    """
    rel_path: Path = field(
        metadata=config(
            encoder=str,
            decoder=Path,
            mm_field=fields.Str()
        )
    )
    sha1: str
    ctime: float


# IndexData is a mapping from relative file paths to File records. Closely analogous to Git's repo index.
IndexData = Dict[str, File]


def sha1sum(filename: Path) -> str:
    """
    Calculates a file's SHA1 without reading the whole file into memory.
    """
    h  = hashlib.sha1()
    b  = bytearray(128*1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


def get_index_data(path: Path) -> IndexData:
    """
    Calculates index data for files under a `path`. Does not use cached SHAs.
    """
    res = {}
    for p in path.iterdir():
        rel_path = p.relative_to(path)
        res[str(rel_path)] = get_file_record(p, rel_path)
    return res


def get_file_record(path: Path, rel_path: Path) -> File:
    """
    Computes a file record given a path and a path relative to FileSet root.
    """
    stat = path.stat()
    sha = sha1sum(path)
    ctime = max(stat.st_ctime, stat.st_mtime)
    return File(rel_path, sha, ctime)


def get_index_path(base_path: Path) -> Path:
    """
    Get the path to the index file.

    Assumes that there is a .neptune folder in the working directory, or else creates one. IRL we'd perhaps require
    the user to create such a directory like in `git init`, or look for a .neptune directory in parents.
    """
    hash = hashlib.sha1(str(base_path.absolute()).encode("utf-8")).hexdigest()
    return Path('.neptune/index') / hash


def write_index(base_path: Path, index_data: IndexData) -> None:
    """
    Writes index data of a directory to a file at the default location.
    """
    index_path = get_index_path(base_path)
    index_path.parent.mkdir(parents=True, exist_ok=True)
    with open(index_path, 'w+') as f:
        json = File.schema().dumps(index_data.values(), many=True)
        f.write(json)


def read_index(base_path: Path) -> Optional[IndexData]:
    """
    Reads index data of a directory from a file at the default location.
    """
    index_path = get_index_path(base_path)
    if index_path.exists():
        with open(index_path, 'r') as f:
            files: List[File] = File.schema().loads(f.read(), many=True)
            return {str(file.rel_path): file for file in files}


def get_index_data_with_cache(index_data: IndexData, path: Path) -> IndexData:
    """
    Calculates index data for a directory, using cached SHAs from a previous index on disk if one exists.
    """
    res = {}
    for p in path.iterdir():
        rel_path = p.relative_to(path)
        if p in index_data:
            record = index_data[rel_path]
            stat = p.stat()
            ctime = max(stat.st_ctime, stat.st_mtime)
            if record.ctime >= ctime:
                res[str(rel_path)] = record
                continue
        res[str(rel_path)] = get_file_record(p, rel_path)
    return res


def upload(path: Path) -> IndexData:
    """
    This method would compute the index of a directory, possibly using an old index, and write the new index.

    IRL it would upload files to the Neptune server.

    The details of how the server decides which files should be uploaded and which files can be re-used based on their
    SHAs will be proposed in a different PoC on the server side.

    Note: this assumes that files are not modified between the time their SHA is calculated and they are uploaded.
    But this is a real concern which should be dealt with, but which requires making some assumptions.
    We could always have an equivalent of Git's staging area where files are copied for SHA calculation and upload,
    but I'm not sure we want to copy all such files. In other words, this is an open problem.

    Note: the current design keep an index per root of a FileSet directory.
    It does not use the index and cached SHAs if the user subsequently uploads a subdirectory.
    E.g. suppose a user upload /foo which contains /foo/bar/a_large_file
    If the user uploads /foo/bar as a new artifact / FileSet, this design would not be able to reuse the cached
    SHA of /foo/bar/a_large_file because an index is saved per root of an uploaded FileSet / artifact.
    But different designs are possible.
    """
    index_data = read_index(path)
    if index_data:
        new_index_data = get_index_data_with_cache(index_data, path)
    else:
        new_index_data = get_index_data(path)
    write_index(path, new_index_data)
    return new_index_data


def tmp_directory(path: Path, artifact_dir_name: str) -> Path:
    """
    Creates sample contents in the given directory.
    """
    artifact_dir_path = path / artifact_dir_name
    artifact_dir_path.mkdir()

    with open(artifact_dir_path / "foo.txt", 'w+') as f:
        f.write("foo")

    with open(artifact_dir_path / "bar.txt", 'w+') as f:
        f.write("bar")

    return path / artifact_dir_name


def test_index(tmp_path: Path):
    # given a directory with files
    artifact_dir = tmp_directory(tmp_path, "artifact")

    # when user "uploads" the directory
    upload(artifact_dir)

    # then index should be written
    with open(get_index_path(artifact_dir), 'r') as f:
        print(f.read())

    # and given one of the files change but not the other
    foo_file = artifact_dir / "foo.txt"
    bar_file = artifact_dir / "bar.txt"
    with open(foo_file, 'a+') as f:
        f.write("more")
    foo_atime = foo_file.stat().st_atime
    bar_atime = bar_file.stat().st_atime

    # when user uploads the directory again
    upload(artifact_dir)

    # then index should be written
    with open(get_index_path(artifact_dir), 'r') as f:
        print(f.read())

    # and the unmodified file should not be accessed
    new_foo_atime = foo_file.stat().st_atime
    new_bar_atime = bar_file.stat().st_atime
    assert new_foo_atime > foo_atime
    assert new_bar_atime == bar_atime
