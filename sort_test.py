import asyncio
import functools
import json
import logging
import os
import random
import statistics
import time
from datetime import datetime
from multiprocessing import Pool, Queue, Manager
from typing import List, Dict

import click
import cloudpickle as pickle
from aio_pika import connect_robust, ExchangeType, Message
from lithops import Storage
from tabulate import tabulate

from IO import write_obj
from config import NUM_EXECUTIONS, in_bucket, out_bucket, timestamp_bucket, intermediate_bucket, parallel, \
    use_rabbitmq, key, burst_size, total_workers, exchange_name, queue_prefix, sort_key, names, types
from logger import setup_logger
from sort import scan, partition, sort, write
from utils import serialize_partitions, reader, concat_progressive

logger = logging.getLogger(__name__)


async def test_sort(
        servers: List[str],
        burst_size: int,
        total_workers: int,
        server_id: int,
):
    storage = Storage()

    process_range = range(burst_size * server_id, burst_size * (server_id + 1))

    # setup rabbitmq for the server with id burst_id
    # each server has burst_size queues, each queue is bound to a different reducer
    if use_rabbitmq:
        async def setup_rabbitmq(server: str, num_reducers: int, server_id: int):
            connection = await connect_robust(server)
            channel = await connection.channel()
            exchange = await channel.declare_exchange(exchange_name, durable=True, type=ExchangeType.DIRECT)
            queues = await asyncio.gather(
                *[
                    # the queue name is queue_prefix_0, queue_prefix_1, ..., queue_prefix_(num_reducers - 1)
                    channel.declare_queue(f"{queue_prefix}_{reducer_id}", durable=True)
                    for reducer_id in process_range
                ]
            )
            await asyncio.gather(
                *[
                    queue.bind(exchange, routing_key=queue.name)
                    for queue in queues
                ]
            )

            await channel.close()
            await connection.close()

        await setup_rabbitmq(servers[server_id], burst_size, server_id)

    exchange_times = []
    mapper_tables = []
    reducer_tables = []

    # sampler = Sample(
    #     bucket=in_bucket,
    #     key=key,
    #     sort_key=sort_key,
    #     num_partitions=total_workers // 2,
    #     delimiter=",",
    #     names=names,
    #     types=types
    # )
    #
    # sampler.set_storage(storage)
    #
    # segment_info = pickle.loads(sampler.run())

    segment_info = pickle.loads(storage.get_object(
        bucket=intermediate_bucket, key=f"{key}/{total_workers}/segment_info.pkl"
    ))

    # there are total_workers // 2 mappers and total_workers // 2 reducers
    map_partitions = reduce_partitions = total_workers // 2

    logger.info(f"process range: {process_range}")
    m = Manager()

    for _ in range(NUM_EXECUTIONS):
        timestamp_prefix = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}-{_}"

        queues = {
            partition_id: m.Queue()
            for partition_id in process_range
        }

        if not parallel:
            with Pool(processes=burst_size) as pool:
                pool.starmap(mapper, [
                    (servers, queues, exchange_name, map_partitions, partition_id,
                     server_id, reduce_partitions, segment_info, timestamp_prefix)
                    for partition_id in process_range
                ])
                pool.starmap(reducer, [
                    (servers, queues, exchange_name, map_partitions, partition_id,
                     server_id, timestamp_prefix)
                    for partition_id in process_range
                ])
        else:
            with Pool(processes=burst_size * 2) as pool:
                f1 = pool.starmap_async(mapper, [
                    (servers, queues, exchange_name, map_partitions, partition_id,
                     server_id, reduce_partitions, segment_info, timestamp_prefix)
                    for partition_id in process_range
                ])
                f2 = pool.starmap_async(reducer, [
                    (servers, queues, exchange_name, map_partitions, partition_id,
                     server_id, timestamp_prefix)
                    for partition_id in process_range
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
        servers: List[str],
        queues: Dict[int, Queue],
        exchange_name: str,
        map_partitions: int,
        partition_id: int,
        server_id: int,
        reduce_partitions: int,
        segment_info: list,
        timestamp_prefix: str
):
    async def _mapper():
        # setup rabbitmq to reducer
        if use_rabbitmq:
            connections = {
                i: await connect_robust(servers[i])
                for i in range(len(servers)) if i != server_id
            }
            channels = {
                i: await connection.channel()
                for i, connection in connections.items()
            }
            exchanges = {
                i: await channel.declare_exchange(
                    exchange_name, ExchangeType.DIRECT, durable=True
                )
                for i, channel in channels.items()
            }

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
        subpartitions = serialize_partitions(
            reduce_partitions,
            partition_obj,
            hash_list
        )

        futures = []
        loop = asyncio.get_event_loop()

        timestamp = time.time()
        for dest_reducer, data in subpartitions.items():
            dest_server = dest_reducer // (reduce_partitions // len(servers))
            # if same machine, write to shared memory
            if dest_server == server_id:
                futures.append(
                    loop.run_in_executor(
                        None,
                        queues[dest_reducer].put,
                        data
                    )
                )
            # else write to rabbitmq or s3
            else:
                if use_rabbitmq:
                    futures.append(
                        exchanges[dest_server].publish(
                            Message(body=data),
                            routing_key=f"{queue_prefix}_{dest_reducer}",
                        )
                    )
                else:
                    futures.append(
                        loop.run_in_executor(
                            None,
                            functools.partial(
                                write_obj,
                                storage=storage,
                                Bucket=intermediate_bucket,
                                Key=f"{timestamp_prefix}/intermediates/{partition_id}",
                                sufixes=[str(dest_reducer)],
                                Body=data
                            )
                        )
                    )
        await asyncio.gather(*futures)
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
            await asyncio.gather(
                *[channel.close() for channel in channels.values()],
            )
            await asyncio.gather(
                *[connection.close() for connection in connections.values()],
            )

    asyncio.run(_mapper())


def reducer(
        servers: List[str],
        queues: Dict[int, Queue],
        exchange_name: str,
        map_partitions: int,
        partition_id: int,
        server_id: int,
        timestamp_prefix: str
):
    async def _reducer():
        if use_rabbitmq:
            connection = await connect_robust(servers[server_id])
            channel = await connection.channel()
            exchange = await channel.declare_exchange(exchange_name, durable=True, type=ExchangeType.DIRECT)
            queue = await channel.declare_queue(f"{queue_prefix}_{partition_id}", durable=True)

        storage = Storage()

        start = time.time()

        # Read the corresponding subpartitions from each mappers' output, and concat all of them
        async def exchange_read_shared_memory():
            res = []
            while len(res) < burst_size:
                data = queues[partition_id].get(block=True)
                res.append(data)
            return res

        async def exchange_read_rabbitmq():
            res = []
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        res.append(message.body)

                        if len(res) == map_partitions - burst_size:
                            break
            return res

        async def exchange_read_s3():
            map_parts = list(range(map_partitions))
            random.shuffle(map_parts)

            loop = asyncio.get_event_loop()
            objects = await asyncio.gather(
                *[
                    loop.run_in_executor(
                        None, functools.partial(
                            reader,
                            source_partition=map_partition,
                            destiny_partition=partition_id,
                            bucket=intermediate_bucket,
                            storage=storage,
                            prefix=f"{timestamp_prefix}/intermediates"
                        )
                    )
                    for map_partition in map_parts
                ]
            )
            return objects

        task1 = asyncio.create_task(
            exchange_read_shared_memory()
        )
        if use_rabbitmq:
            task2 = asyncio.create_task(
                exchange_read_rabbitmq()
            )
        else:
            task2 = asyncio.create_task(
                exchange_read_s3()
            )
        await asyncio.gather(task1, task2)
        timestamp = time.time()

        partition_obj = concat_progressive(task1.result() + task2.result())

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


@click.command()
@click.argument("servers", nargs=-1)
@click.option("--server-id", type=int, default=os.environ.get("SERVER_ID", 0), help="Server ID")
def main(servers: List[str], server_id: int):
    setup_logger(log_level=logging.INFO)

    if len(servers) != total_workers // burst_size // 2:
        logger.error(f"Expected {total_workers // burst_size // 2} servers, got {len(servers)}")
        exit(1)

    asyncio.run(test_sort(servers, burst_size, total_workers, server_id))


if __name__ == "__main__":
    main()
