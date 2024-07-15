import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import RLock
from typing import (
    List,
    Literal,
    Optional,
    Union,
)

import numpy as np
import pandas as pd
import pytest

from neptune.core.components.queue.aggregating_disk_queue import AggregatingDiskQueue

Queue = Union[AggregatingDiskQueue[int, int]]


SEQUENCE_SIZE = 10**6
PERMUTATION_RATIO = 0.1
NONE_RATIO = 0.1
BATCH_SIZE = 2**10
RESULT_PATH = Path("./performance-raport/queue")
SEQUENCE_BATCH_SIZE = SEQUENCE_SIZE // 10
FLUSH_INTERVAL = 10
PRECISION = 7


@dataclass(frozen=True)
class QueueElement:
    value: int


def describe(df):
    return pd.concat([df.describe().T, df.sum().rename("sum")], axis=1).T


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


def generate_random_step_sequence(size: int, none_ration: float) -> List[Optional[int]]:
    sequence = np.arange(size, dtype=float) // SEQUENCE_BATCH_SIZE
    sequence = np.random.permutation(sequence)
    none_cnt = int(size * none_ration)
    sequence[np.random.randint(0, size, none_cnt)] = np.nan
    return [None if np.isnan(v) else v for v in sequence.tolist()]


def generate_batched_step_sequence(size: int, permutation_ratio: float, none_ration: float) -> List[Optional[int]]:
    sequence = np.arange(size, dtype=float) // SEQUENCE_BATCH_SIZE
    none_cnt = int(size * none_ration)
    sequence[np.random.randint(0, size, none_cnt)] = np.nan
    permutation_cnt = int(size * permutation_ratio)
    permutations_pairs = np.random.randint(0, size, (permutation_cnt, 2))
    for i, j in permutations_pairs:
        sequence[[i, j]] = sequence[[j, i]]

    return [None if np.isnan(v) else v for v in sequence.tolist()]


def generate_random_element(size: int) -> List[QueueElement]:
    return [QueueElement(v.item()) for v in np.random.randint(0, size, size)]


def get_step_sequence(sequence_type: Literal["random", "batched"]) -> List[Optional[int]]:
    if sequence_type == "random":
        return generate_random_step_sequence(SEQUENCE_SIZE, NONE_RATIO)
    elif sequence_type == "batched":
        return generate_batched_step_sequence(SEQUENCE_SIZE, PERMUTATION_RATIO, NONE_RATIO)
    else:
        raise ValueError("Invalid sequence generator type")


@pytest.mark.performance
def test_put_performance():
    with TemporaryDirectory() as tmp_dir:
        record = []
        queue = setup_queue(Path(tmp_dir))
        elements = generate_random_element(SEQUENCE_SIZE)
        steps = generate_batched_step_sequence(SEQUENCE_SIZE, PERMUTATION_RATIO, NONE_RATIO)

        def benchmark_put(
            queue: Queue,
            element: QueueElement,
            category: int,
            iter: int,
            flush: bool,
            flush_ammount: Optional[int] = None,
        ):
            t0 = time.time()
            queue.put(element, category=category)
            t1 = time.time()
            if flush:
                queue.flush()
            t2 = time.time()

            record.append(
                {
                    "put_dt": t1 - t0,
                    "flush_dt": (t2 - t1) / flush_ammount if flush else None,
                }
            )

        last_flush = 0
        for i, (element, step) in enumerate(zip(elements, steps)):
            should_flush = (i != 0) and ((i % FLUSH_INTERVAL == 0) or (i == SEQUENCE_SIZE - 1))
            if should_flush:
                flush_size = i - last_flush
                last_flush = i
            else:
                flush_size = None
            benchmark_put(queue, element, category=step, iter=i, flush=should_flush, flush_ammount=flush_size)

    assert queue.size() == len(record)

    path = RESULT_PATH / "put_performance.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(record)
    describe(df).round(PRECISION).to_csv(RESULT_PATH / "put_performance_summary.csv")


@pytest.mark.performance
@pytest.mark.parametrize("sequence_type", ["random", "batched"])
def test_get_batch_performance(sequence_type: str):
    with TemporaryDirectory() as tmp_dir:
        record = []
        queue = setup_queue(Path(tmp_dir))
        elements = generate_random_element(SEQUENCE_SIZE)
        step_sequence = get_step_sequence(sequence_type)

        for element, step in zip(elements, step_sequence):
            queue.put(element, category=step)
        queue.flush()

        def benchmark_get_batch(queue: Queue):
            t0 = time.time()
            batch = queue.get_batch(size=BATCH_SIZE)
            batch_size = len(batch)
            t1 = time.time()
            queue.ack(batch[-1].ver)
            t2 = time.time()

            record.append(
                {"get_batch_dt": (t1 - t0) / batch_size, "ack_dt": (t2 - t1) / batch_size, "batch_size": len(batch)}
            )

        while queue.size() > 0:
            benchmark_get_batch(queue)

    path = RESULT_PATH / f"get_batch_performance_{sequence_type}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(record)
    describe(df).round(PRECISION).to_csv(RESULT_PATH / f"get_batch_performance_summary_{sequence_type}.csv")
