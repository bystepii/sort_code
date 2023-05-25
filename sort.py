import asyncio
import functools
import random

import pandas as pd
from lithops import Storage
import numpy as np
from typing import List, Dict

from IO import write_obj
from utils import _get_read_range, read_and_adjust, get_data_size, serialize_partitions, _writer_multiple_files, \
    concat_progressive, reader, serialize


def scan(storage: Storage,
         bucket: str,
         key: str,
         partition_id: int,
         num_partitions: int,
         delimiter: str = ",",
         names: List[str] = None,
         types: Dict[str, str] = None
         ) \
        -> pd.DataFrame:
    lower_bound, upper_bound = _get_read_range(storage,
                                               bucket,
                                               key,
                                               partition_id,
                                               num_partitions)

    total_size = get_data_size(storage,
                               bucket,
                               key)

    data, _, _ = read_and_adjust(storage=storage,
                                 read_bucket=bucket,
                                 read_path=key,
                                 lower_bound=lower_bound,
                                 upper_bound=upper_bound,
                                 total_size=total_size,
                                 delimiter=delimiter,
                                 names=names,
                                 types=types)

    return data


def partition(
        partition: pd.DataFrame,
        segment_info: list,
        sort_key: str) \
        -> np.ndarray:
    return np.searchsorted(segment_info, partition[sort_key])


def exchange_write(
        storage: Storage,
        partition_obj: pd.DataFrame,
        partition_id: int,
        num_partitions: int,
        intermediate_bucket: str,
        hash_list: np.ndarray
):
    subpartitions = serialize_partitions(num_partitions,
                                         partition_obj,
                                         hash_list)

    _writer_multiple_files(storage=storage,
                           subpartitions=subpartitions,
                           partition_id=partition_id,
                           bucket=intermediate_bucket)


def exchange_read(
        storage: Storage,
        partition_id: int,
        num_partitions: int,
        intermediate_bucket: str) \
        -> pd.DataFrame:
    async def reads():
        map_partitions = list(range(num_partitions))
        random.shuffle(map_partitions)

        tasks = [
            loop.run_in_executor(None, functools.partial(reader,
                                                         source_partition=map_partition,
                                                         destiny_partition=partition_id,
                                                         bucket=intermediate_bucket,
                                                         storage=storage))
            for map_partition in map_partitions
        ]

        objects = await asyncio.gather(
            *tasks
        )

        return objects

    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(reads())

    partition_obj = concat_progressive(res)

    return partition_obj


def sort(partition_obj: pd.DataFrame,
         sort_key: str):
    partition_obj.sort_values(sort_key, inplace=True)


def write(
        storage: Storage,
        partition_obj: pd.DataFrame,
        partition_id: int,
        bucket: str):
    out_key = f"out_{partition_id}"

    serialized_partition = serialize(partition_obj)

    write_obj(
        storage=storage,
        Bucket=bucket,
        Key=out_key,
        Body=serialized_partition,
    )

    print(f"Reducer {partition_id} written to {out_key}")
