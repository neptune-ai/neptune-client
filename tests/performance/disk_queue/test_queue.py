import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import RLock
from typing import (
    List,
    Union,
)

import numpy as np
import pandas as pd
import pytest

from neptune.core.components.queue.aggregating_disk_queue import AggregatingDiskQueue

Queue = Union[AggregatingDiskQueue[int, int]]


SEQUENCE_SIZE = 10**2
PERMUTATION_RATIO = 0.1
NONE_RATIO = 0.1
RESULT_PATH = Path("./results")


@dataclass(frozen=True)
class QueueElement:
    value: int


def setup_queue(dir: Path):
    return AggregatingDiskQueue[int, int](
        data_path=dir,
        to_dict=lambda x: x.__dict__,
        from_dict=lambda x: QueueElement(**x),
        lock=RLock(),
        max_file_size=64 * 1024**2,
        max_batch_size_bytes=None,
        extension="log",
    )


def generate_step_sequence(size: int, permutation_ratio: float, none_ration: float) -> List[int]:
    sequence = np.arange(size, dtype=float)
    none_cnt = int(size * none_ration)
    sequence[np.random.randint(0, size, none_cnt)] = None
    permutation_cnt = int(size * permutation_ratio)
    permutations_pairs = np.random.randint(0, size, (permutation_cnt, 2))
    for i, j in permutations_pairs:
        sequence[[i, j]] = sequence[[j, i]]
    return sequence.tolist()


def generate_random_element(size: int) -> List[QueueElement]:
    return [QueueElement(v.item()) for v in np.random.randint(0, size, size)]


@pytest.mark.performance
def test_put_performance():
    with TemporaryDirectory() as tmp_dir:
        record = []
        queue = setup_queue(Path(tmp_dir))
        elements = generate_random_element(SEQUENCE_SIZE)
        steps = generate_step_sequence(SEQUENCE_SIZE, PERMUTATION_RATIO, NONE_RATIO)

        def benchmark_put(queue: Queue, element: QueueElement, category: int, iter: int):
            t0 = time.time()
            queue.put(element, category=category)
            dt = time.time() - t0
            record.append({"time": dt})

        for i, (element, step) in enumerate(zip(elements, steps)):
            benchmark_put(queue, element, category=step, iter=i)

    assert queue.size() == len(record)

    path = RESULT_PATH / "put_performance.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(record)
    df.to_csv(path, index=True)
    df.describe().to_csv(RESULT_PATH / "put_performance_summary.csv")
