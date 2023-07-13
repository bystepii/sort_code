import asyncio
import functools
import random
import time
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
from aio_pika import Message
from aio_pika.abc import AbstractChannel, AbstractExchange
from lithops import Storage

from IO import write_obj
from utils import _get_read_range, read_and_adjust, get_data_size, serialize_partitions, concat_progressive,\
    serialize, reader


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

    print("Get read range done")
    total_size = get_data_size(storage,
                               bucket,
                               key)

    print("Get data size done")
    data, _, _ = read_and_adjust(storage=storage,
                                 chunk_size=1,
                                 read_bucket=bucket,
                                 read_path=key,
                                 lower_bound=lower_bound,
                                 upper_bound=upper_bound,
                                 total_size=total_size,
                                 delimiter=delimiter,
                                 names=names,
                                 types=types)

    print("Read and adjust done")
    return data


def partition(
        partition: pd.DataFrame,
        segment_info: list,
        sort_key: str) \
        -> np.ndarray:
    return np.searchsorted(segment_info, partition[sort_key])


async def exchange_write_rabbitmq(
        channel: AbstractChannel,
        partition_obj: pd.DataFrame,
        partition_id: int,
        num_partitions: int,
        exchange: AbstractExchange,
        queue_prefix: str,
        hash_list: np.ndarray
) -> float:
    subpartitions = serialize_partitions(num_partitions,
                                         partition_obj,
                                         hash_list)
    timestamp = time.time()
    await asyncio.gather(
        *[
            exchange.publish(
                message=Message(body=subpartition_data[1]),
                routing_key=f"{queue_prefix}_{subpartition_data[0]}",
            )
            for subpartition_data in subpartitions.items()
        ]
    )
    return timestamp


async def exchange_read_rabbitmq(
        channel: AbstractChannel,
        partition_id: int,
        num_partitions: int,
        exchange: AbstractExchange,
        queue_prefix: str) \
        -> Tuple[pd.DataFrame, float]:
    queue = await channel.declare_queue(f"{queue_prefix}_{partition_id}", durable=True)

    res = []
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                res.append(message.body)

                if len(res) == num_partitions:
                    break

    timestamp = time.time()

    partition_obj = concat_progressive(res)

    return partition_obj, timestamp


async def exchange_write_s3(
        storage: Storage,
        partition_obj: pd.DataFrame,
        partition_id: int,
        num_partitions: int,
        intermediate_bucket: str,
        hash_list: np.ndarray,
        timestamp_prefix: str
) -> float:
    subpartitions = serialize_partitions(num_partitions,
                                         partition_obj,
                                         hash_list)
    loop = asyncio.get_event_loop()

    timestamp = time.time()
    await asyncio.gather(
        *[
            loop.run_in_executor(None, functools.partial(write_obj,
                                                         storage=storage,
                                                         Bucket=intermediate_bucket,
                                                         Key=f"{timestamp_prefix}/intermediates/{partition_id}",
                                                         sufixes=[str(subpartition_data[0])],
                                                         Body=subpartition_data[1]))
            for subpartition_data in subpartitions.items()
        ]
    )
    return timestamp


async def exchange_read_s3(
        storage: Storage,
        partition_id: int,
        num_partitions: int,
        intermediate_bucket: str,
        timestamp_prefix: str) \
        -> Tuple[pd.DataFrame, float]:
    map_partitions = list(range(num_partitions))
    random.shuffle(map_partitions)

    loop = asyncio.get_event_loop()
    objects = await asyncio.gather(
        *[
            loop.run_in_executor(None, functools.partial(reader,
                                                         source_partition=map_partition,
                                                         destiny_partition=partition_id,
                                                         bucket=intermediate_bucket,
                                                         storage=storage,
                                                         prefix=f"{timestamp_prefix}/intermediates"))
            for map_partition in map_partitions
        ]
    )

    timestamp = time.time()

    partition_obj = concat_progressive(objects)

    return partition_obj, timestamp


def sort(partition_obj: pd.DataFrame,
         sort_key: str):
    partition_obj.sort_values(sort_key, inplace=True)


def write(
        storage: Storage,
        partition_obj: pd.DataFrame,
        partition_id: int,
        bucket: str,
        prefix: str):
    out_key = f"{prefix}/out_{partition_id}"

    serialized_partition = serialize(partition_obj)

    write_obj(
        storage=storage,
        Bucket=bucket,
        Key=out_key,
        Body=serialized_partition,
    )

    print(f"Reducer {partition_id} written to {out_key}")
