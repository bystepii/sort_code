import asyncio
import json
import logging
import os
import statistics
import time
from datetime import datetime
from multiprocessing import Pool

import cloudpickle as pickle
from aio_pika import connect_robust, ExchangeType
from lithops import Storage
from tabulate import tabulate

from logger import setup_logger
from sample import Sample
from sort import scan, partition, exchange_write_rabbitmq, exchange_read_rabbitmq, sort, write, exchange_write_s3, \
    exchange_read_s3

logger = logging.getLogger(__name__)

NUM_EXECUTIONS = 3

in_bucket = "benchmark-objects"
out_bucket = "stepan-lithops-sandbox"
timestamp_bucket = "stepan-lithops-sandbox"
intermediate_bucket = "stepan-lithops-sandbox"

parallel = True

key = "terasort-1g"

sort_key = "0"
names = ["0", "1"]
types = {
    "0": "string[pyarrow]",
    "1": "string[pyarrow]"
}

map_partitions = 6
reduce_partitions = 6

rabbitmq_url = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
exchange_name = 'sort'
queue_prefix = 'reducer'

use_rabbitmq = True


async def test_sort(map_partitions: int,
                    reduce_partitions: int):

    storage = Storage()

    if use_rabbitmq:
        connection = await connect_robust(rabbitmq_url)
        channel = await connection.channel()
        exchange = await channel.declare_exchange(exchange_name, durable=True, type=ExchangeType.DIRECT)
        queues = await asyncio.gather(
            *[
                channel.declare_queue(f"{queue_prefix}_{partition_id}", durable=True)
                for partition_id in range(reduce_partitions)
            ]
        )
        await asyncio.gather(
            *[
                queue.bind(exchange, routing_key=f"{queue_prefix}_{partition_id}")
                for partition_id, queue in enumerate(queues)
            ]
        )

        await channel.close()
        await connection.close()

    exchange_times = []
    mapper_tables = []
    reducer_tables = []

    for _ in range(NUM_EXECUTIONS):
        timestamp_prefix = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")

        sampler = Sample(
            bucket=in_bucket,
            key=key,
            sort_key=sort_key,
            num_partitions=reduce_partitions,
            delimiter=",",
            names=names,
            types=types
        )

        sampler.set_storage(storage)

        segment_info = pickle.loads(sampler.run())

        if not parallel:
            with Pool(processes=max(map_partitions, reduce_partitions)) as pool:
                pool.starmap(mapper, [
                    (exchange_name, map_partitions, partition_id, reduce_partitions, segment_info, timestamp_prefix)
                    for partition_id in range(map_partitions)
                ])
                pool.starmap(reducer, [
                    (exchange_name, map_partitions, partition_id, timestamp_prefix)
                    for partition_id in range(reduce_partitions)
                ])
        else:
            with Pool(processes=map_partitions+reduce_partitions) as pool:
                f1 = pool.starmap_async(mapper, [
                    (exchange_name, map_partitions, partition_id, reduce_partitions, segment_info, timestamp_prefix)
                    for partition_id in range(map_partitions)
                ])
                f2 = pool.starmap_async(reducer, [
                    (exchange_name, map_partitions, partition_id, timestamp_prefix)
                    for partition_id in range(reduce_partitions)
                ])
                f1.get()
                f2.get()

        mappers = [
            json.loads(storage.get_object(
                bucket=timestamp_bucket,
                key=f"{timestamp_prefix}/timestamps/mapper_{i}.json"
            )) for i in range(map_partitions)
        ]
        reducers = [
            json.loads(storage.get_object(
                bucket=timestamp_bucket,
                key=f"{timestamp_prefix}/timestamps/reducer_{i}.json"
            )) for i in range(reduce_partitions)
        ]

        mapper_timestamps = [m["write_start"] for m in mappers]
        reducers_timestamps = [r["read_finish"] for r in reducers]
        logger.info(f"Mapper timestamps: {mapper_timestamps}")
        logger.info(f"Reducer timestamps: {reducers_timestamps}")
        exchange_time = max(reducers_timestamps) - min(mapper_timestamps)
        exchange_times.append(exchange_time)
        logger.info(f"Exchange duration: {exchange_time}")

        mapper_table = [[m["scan_time"], m["partition_time"], m["exchange_time"]] for m in mappers]
        reducer_table = [[r["exchange_time"], r["sort_time"], r["write_time"]] for r in reducers]
        mapper_tables.append(mapper_table)
        reducer_tables.append(reducer_table)

    for i in range(NUM_EXECUTIONS):
        logger.info(f"Mapper table {i}:")
        print(tabulate(mapper_tables[i], headers=["scan", "partition", "exchange"]))
        logger.info(f"Reducer table {i}:")
        print(tabulate(reducer_tables[i], headers=["exchange", "sort", "write"]))

    logger.info(f"Exchange times: {exchange_times}")
    avg = sum(exchange_times) / len(exchange_times)
    stdev = statistics.stdev(exchange_times)
    logger.info(f"Exchange time: {avg} ± {stdev} (average ± stdev)")


def mapper(
        exchange_name: str,
        map_partitions: int,
        partition_id: int,
        reduce_partitions: int,
        segment_info: list,
        timestamp_prefix: str
):
    async def _mapper():
        if use_rabbitmq:
            connection = await connect_robust(rabbitmq_url)
            channel = await connection.channel()
            exchange = await channel.declare_exchange(exchange_name, durable=True, type=ExchangeType.DIRECT)
        storage = Storage()

        start = time.time()
        # Get partition for this worker from persistent storage (object store)
        partition_obj = scan(
            storage=storage,
            bucket=in_bucket,
            key=key,
            partition_id=partition_id,
            num_partitions=map_partitions,
            names=names,
            types=types
        )
        end = time.time()
        scan_time = end - start

        logger.info(f"Mapper {partition_id} got {len(partition_obj)} rows.")

        start = time.time()
        # Calculate the destination worker for each row
        hash_list = partition(partition=partition_obj,
                              segment_info=segment_info,
                              sort_key=sort_key)
        end = time.time()
        partition_time = end - start

        start = time.time()
        # Write the subpartition corresponding to each worker
        if use_rabbitmq:
            timestamp = await exchange_write_rabbitmq(channel=channel,
                                                      partition_obj=partition_obj,
                                                      partition_id=partition_id,
                                                      num_partitions=reduce_partitions,
                                                      exchange=exchange,
                                                      queue_prefix=queue_prefix,
                                                      hash_list=hash_list)
        else:
            timestamp = await exchange_write_s3(storage=storage,
                                                partition_obj=partition_obj,
                                                partition_id=partition_id,
                                                num_partitions=reduce_partitions,
                                                intermediate_bucket=intermediate_bucket,
                                                hash_list=hash_list,
                                                timestamp_prefix=timestamp_prefix)
        end = time.time()
        exchange_time = end - start

        storage.put_object(
            bucket=timestamp_bucket,
            key=f"{timestamp_prefix}/timestamps/mapper_{partition_id}.json",
            body=json.dumps({
                "scan_time": scan_time,
                "partition_time": partition_time,
                "exchange_time": exchange_time,
                "write_start": timestamp
            })
        )

        if use_rabbitmq:
            await channel.close()
            await connection.close()
    asyncio.run(_mapper())


def reducer(
        exchange_name: str,
        map_partitions: int,
        partition_id: int,
        timestamp_prefix: str
):
    async def _reducer():
        if use_rabbitmq:
            connection = await connect_robust(rabbitmq_url)
            channel = await connection.channel()
            exchange = await channel.declare_exchange(exchange_name, durable=True, type=ExchangeType.DIRECT)

        storage = Storage()

        start = time.time()
        # Read the corresponding subpartitions from each mappers' output, and concat all of them
        if use_rabbitmq:
            partition_obj, timestamp = await exchange_read_rabbitmq(
                channel=channel,
                partition_id=partition_id,
                num_partitions=map_partitions,
                exchange=exchange,
                queue_prefix=queue_prefix,
            )
        else:
            partition_obj, timestamp = await exchange_read_s3(
                storage=storage,
                partition_id=partition_id,
                num_partitions=map_partitions,
                intermediate_bucket=intermediate_bucket,
                timestamp_prefix=timestamp_prefix,
            )
        end = time.time()
        exchange_time = end - start

        logger.info(f"Reducer {partition_id} got {len(partition_obj)} rows.")

        start = time.time()
        # Sort the partition
        sort(
            partition_obj=partition_obj,
            sort_key=sort_key
        )
        end = time.time()
        sort_time = end - start

        start = time.time()
        # Write the partition to persistent storage (object store)
        write(
            storage=storage,
            partition_obj=partition_obj,
            partition_id=partition_id,
            bucket=out_bucket,
            prefix=timestamp_prefix,
        )
        end = time.time()
        write_time = end - start

        storage.put_object(
            bucket=timestamp_bucket,
            key=f"{timestamp_prefix}/timestamps/reducer_{partition_id}.json",
            body=json.dumps({
                "exchange_time": exchange_time,
                "sort_time": sort_time,
                "write_time": write_time,
                "read_finish": timestamp
            })
        )

        if use_rabbitmq:
            await channel.close()
            await connection.close()
    asyncio.run(_reducer())


if __name__ == "__main__":
    setup_logger(log_level=logging.INFO)
    asyncio.run(test_sort(map_partitions, reduce_partitions))
