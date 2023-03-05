from ctypes import *
from ctypes.util import find_library as find_library
from typing import (
    NewType,
    Optional,
)

nvmlDevice_t = NewType("nvmlDevice_t", c_void_p)

class nvmlUtilization_t(Structure):
    gpu: c_uint
    memory: c_uint

class nvmlMemory_v2_t(Structure):
    total: c_ulonglong
    free: c_ulonglong
    used: c_ulonglong

def nvmlInit() -> None: ...
def nvmlDeviceGetUtilizationRates(handle: nvmlDevice_t) -> nvmlUtilization_t: ...
def nvmlDeviceGetMemoryInfo(handle: nvmlDevice_t, version: Optional[None] = ...) -> nvmlMemory_v2_t: ...
def nvmlDeviceGetHandleByIndex(index: int) -> nvmlDevice_t: ...
def nvmlDeviceGetCount() -> int: ...

class NVMLError(Exception): ...
