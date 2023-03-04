from typing import (
    Optional,
    Tuple,
)

def parse(version: str) -> Version: ...

class Version:
    def __init__(self, version: str) -> None: ...
    @property
    def epoch(self) -> int: ...
    @property
    def release(self) -> Tuple[int, ...]: ...
    @property
    def pre(self) -> Optional[Tuple[str, int]]: ...
    @property
    def post(self) -> Optional[int]: ...
    @property
    def dev(self) -> Optional[int]: ...
    @property
    def local(self) -> Optional[str]: ...
    @property
    def public(self) -> str: ...
    @property
    def base_version(self) -> str: ...
    @property
    def is_prerelease(self) -> bool: ...
    @property
    def is_postrelease(self) -> bool: ...
    @property
    def is_devrelease(self) -> bool: ...
    @property
    def major(self) -> int: ...
    @property
    def minor(self) -> int: ...
    @property
    def micro(self) -> int: ...
