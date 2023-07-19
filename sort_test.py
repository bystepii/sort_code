import asyncio
import functools
import itertools
import logging
import os
import random
import time
from collections import defaultdict
from datetime import datetime
from multiprocessing import Pool, Queue, Manager
from typing import Dict, List

import click
import cloudpickle as pickle
import redis.asyncio as aioredis
from aio_pika import connect_robust, ExchangeType, Message
from aio_pika.exceptions import DeliveryError
from lithops import Storage
from pamqp.commands import Basic

from IO import write_obj
from config import in_bucket, out_bucket, intermediate_bucket, parallel, \
    use_rabbitmq, key, burst_size, total_workers, exchange_name, queue_prefix, sort_key, names, types
from logger import setup_logger
from sort import scan, partition, sort, write
from utils import serialize_partitions, reader, concat_progressive

logger = logging.getLogger(__name__)


async def test_sort(
        rabbitmq_server: str,
        redis_server: str,
        burst_size: int,
        total_workers: int,
        server_id: int,
        run: int,
):
    storage = Storage()

    logger.info(f"server id: {server_id}")
    logger.info(f"key: {key}")
    logger.info(f"burst size: {burst_size}")
    logger.info(f"total workers: {total_workers}")
    logger.info(f"using rabbitmq: {use_rabbitmq}")
    logger.info(f"parallel: {parallel}")

    process_range = range(burst_size * server_id, burst_size * (server_id + 1))
    logger.info(f"process range: {process_range}")

    # setup rabbitmq for the server with id burst_id
    # each server has burst_size queues, each queue is bound to a different reducer
    if use_rabbitmq:
        connection = await connect_robust(rabbitmq_server)
        channel = await connection.channel(publisher_confirms=True)
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

    m = Manager()

    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M')

    logger.info(f"==================== RUN {run} ====================")
    timestamp_prefix = f"{timestamp}-{run}"

    queues = {
        partition_id: m.Queue()
        for partition_id in process_range
    }

    if not parallel:
        with Pool(processes=burst_size) as pool:
            pool.starmap(mapper, [
                (rabbitmq_server, redis_server, run, queues, exchange_name, map_partitions, partition_id,
                 server_id, reduce_partitions, segment_info, timestamp_prefix)
                for partition_id in process_range
            ])
            pool.starmap(reducer, [
                (rabbitmq_server, redis_server, run, queues, exchange_name, map_partitions, partition_id,
                 server_id, timestamp_prefix)
                for partition_id in process_range
            ])
    else:
        with Pool(processes=burst_size * 2) as pool:
            f1 = pool.starmap_async(mapper, [
                (rabbitmq_server, redis_server, run, queues, exchange_name, map_partitions, partition_id,
                 server_id, reduce_partitions, segment_info, timestamp_prefix)
                for partition_id in process_range
            ])
            f2 = pool.starmap_async(reducer, [
                (rabbitmq_server, redis_server, run, queues, exchange_name, map_partitions, partition_id,
                 server_id, timestamp_prefix)
                for partition_id in process_range
            ])
            f1.get()
            f2.get()

    logger.info("==================== DONE ====================")


def mapper(
        rabbitmq_server: str,
        redis_server: str,
        run: int,
        queues: Dict[int, Queue],
        exchange_name: str,
        map_partitions: int,
        partition_id: int,
        server_id: int,
        reduce_partitions: int,
        segment_info: list,
        timestamp_prefix: str
):
    async def publish_and_handle_confirm(exchange, queue_name, message_body, headers):
        try:
            confirmation = None
            while not isinstance(confirmation, Basic.Ack):
                confirmation = await exchange.publish(
                    Message(message_body, headers=headers),
                    routing_key=queue_name,
                    timeout=3.0,
                )
            logger.info(f"Acknowledgement received for message!")
        except DeliveryError as e:
            print(f"Delivery of message failed with exception: {e}")
        except TimeoutError:
            print(f"Timeout occurred for message")

    async def _mapper():
        # setup rabbitmq to reducer
        if use_rabbitmq:
            connection = await connect_robust(rabbitmq_server)
            channel = await connection.channel(publisher_confirms=True)
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
        subpartitions = serialize_partitions(
            reduce_partitions,
            partition_obj,
            hash_list
        )

        futures = []
        loop = asyncio.get_event_loop()

        timestamp = time.time()
        for dest_reducer, data in subpartitions.items():
            for i, chunk in enumerate(data):
                last_chunk = False
                if i == len(data) - 1:
                    last_chunk = True

                logger.info(f"Mapper --> partition_id: {partition_id}, "
                            f"chunk_id: {i}, "
                            f"last_chunk: {last_chunk}, "
                            f"dest reducer: {dest_reducer},"
                            f"length data: {len(chunk)}")

                dest_server = dest_reducer // burst_size
                # if same machine, write to shared memory
                if dest_server == server_id:
                    futures.append(
                        loop.run_in_executor(
                            None,
                            queues[dest_reducer].put,
                            {
                                "partition_id": partition_id,
                                "chunk_id": i,
                                "last_chunk": last_chunk,
                                "data": chunk
                            }
                        )
                    )
                # else write to rabbitmq or s3
                else:
                    if use_rabbitmq:
                        futures.append(
                            publish_and_handle_confirm(
                                exchange,
                                f"{queue_prefix}_{dest_reducer}",
                                chunk,
                                {
                                    "partition_id": partition_id,
                                    "chunk_id": i,
                                    "last_chunk": last_chunk,
                                }
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
                                    Body=chunk
                                )
                            )
                        )
        await asyncio.gather(*futures)
        end = time.time()
        exchange_time = end - start

        r = aioredis.from_url(redis_server)

        await r.hset(
            f"{run}/mapper/{partition_id}",
            mapping={
                "id": partition_id,
                "scan_time": scan_time,
                "partition_time": partition_time,
                "exchange_time": exchange_time,
                "write_start": timestamp
            }
        )

        await r.close()

        # storage.put_object(
        #     bucket=timestamp_bucket,
        #     key=f"{timestamp_prefix}/timestamps/mapper_{partition_id}.json",
        #     body=json.dumps({
        #         "scan_time": scan_time,
        #         "partition_time": partition_time,
        #         "exchange_time": exchange_time,
        #         "write_start": timestamp
        #     })
        # )

        if use_rabbitmq:
            await channel.close()
            await connection.close()

    asyncio.run(_mapper())


def reducer(
        rabbitmq_server: str,
        redis_server: str,
        run: int,
        queues: Dict[int, Queue],
        exchange_name: str,
        map_partitions: int,
        partition_id: int,
        server_id: int,
        timestamp_prefix: str
):
    async def _reducer():
        if use_rabbitmq:
            connection = await connect_robust(rabbitmq_server)
            channel = await connection.channel()
            exchange = await channel.declare_exchange(exchange_name, durable=True, type=ExchangeType.DIRECT)
            queue = await channel.declare_queue(f"{queue_prefix}_{partition_id}", durable=True)

        storage = Storage()

        start = time.time()

        # Read the corresponding subpartitions from each mappers' output, and concat all of them
        def exchange_read_shared_memory():
            partition_chunks: Dict[int, List[Dict]] = defaultdict(list)
            num_partitions = 0
            while num_partitions < burst_size:
                data = queues[partition_id].get(block=True)
                m = int(data['partition_id'])
                logger.info(f"Reducer {partition_id} --> from mapper: {m}, "
                            f"chunk_id: {data['chunk_id']}, "
                            f"last_chunk: {data['last_chunk']}, "
                            f"length data: {len(data['data'])}")
                partition_chunks[m].append(data)

                # check if last chunk is received
                try:
                    index = [d['last_chunk'] for d in partition_chunks[m]].index(True)

                    # if last chunk is received, check if all chunks are received
                    if len(partition_chunks[m]) == partition_chunks[m][index]['chunk_id'] + 1:
                        num_partitions += 1
                except ValueError:
                    pass
            return partition_chunks

        async def exchange_read_rabbitmq():
            partition_chunks: Dict[int, List[Dict]] = defaultdict(list)
            num_partitions = 0
            logger.info(f"Reducer {partition_id} waiting for {map_partitions - burst_size} messages via rabbitmq")
            if map_partitions - burst_size <= 0:
                return partition_chunks
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        logger.info(f"Received message with headers: {message.headers} via rabbitmq with "
                                    f"body length {len(message.body)}")
                        m = int(message.headers["partition_id"])
                        partition_chunks[m].append({
                            "partition_id": m,
                            "chunk_id": int(message.headers["chunk_id"]),
                            "last_chunk": message.headers["last_chunk"],
                            "data": message.body
                        })

                        # check if last chunk is received
                        try:
                            index = [d['last_chunk'] for d in partition_chunks[m]].index(True)

                            # if last chunk is received, check if all chunks are received
                            if len(partition_chunks[m]) == partition_chunks[m][index]['chunk_id'] + 1:
                                num_partitions += 1
                        except ValueError:
                            pass

                        if num_partitions == map_partitions - burst_size:
                            break
                        logger.info(f"Reducer {partition_id} remaining messages: "
                                    f"{map_partitions - burst_size - num_partitions} via rabbitmq")
            return partition_chunks

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

        loop = asyncio.get_event_loop()
        task1 = loop.run_in_executor(
            None,
            exchange_read_shared_memory
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

        res = []
        # Sort chunks by chunk_id and concat them
        for m, chunks in itertools.chain(task1.result().items(), task2.result().items()):
            logger.info(f"Reducer {partition_id} got {len(chunks)} chunks from mapper {m}")
            chunks.sort(key=lambda x: x["chunk_id"])
            res.append(b"".join([chunk["data"] for chunk in chunks]))
            logger.info(f"Reducer {partition_id} got {len(res[-1])} bytes from mapper {m}")
        partition_obj = concat_progressive(res)

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

        r = aioredis.from_url(redis_server)
        await r.hset(
            f"{run}/reducer/{partition_id}",
            mapping={
                "id": partition_id,
                "exchange_time": exchange_time,
                "sort_time": sort_time,
                "write_time": write_time,
                "read_finish": timestamp
            }
        )
        await r.close()
        # storage.put_object(
        #     bucket=timestamp_bucket,
        #     key=f"{timestamp_prefix}/timestamps/reducer_{partition_id}.json",
        #     body=json.dumps({
        #         "exchange_time": exchange_time,
        #         "sort_time": sort_time,
        #         "write_time": write_time,
        #         "read_finish": timestamp
        #     })
        # )
        if use_rabbitmq:
            await channel.close()
            await connection.close()

    asyncio.run(_reducer())


@click.command()
@click.argument("rabbitmq_server", type=str, default=os.environ.get("RABBITMQ_SERVER", "localhost"), nargs=1)
@click.option("--server-id", type=int, default=os.environ.get("SERVER_ID", 0), help="Server ID")
@click.option("--redis-server", type=str, default=os.environ.get("REDIS_SERVER", "localhost"), help="Redis server")
@click.option("--run", type=int, default=os.environ.get("RUN", 0), help="Run number")
def main(rabbitmq_server: str, redis_server: str, server_id: int, run: int):
    setup_logger(log_level=logging.INFO)
    if server_id >= (total_workers // burst_size // 2):
        logger.error(f"Server ID {server_id} is invalid. Must be less than {total_workers // burst_size // 2}")
        exit(1)
    asyncio.run(test_sort(rabbitmq_server, redis_server, burst_size, total_workers, server_id, run))


if __name__ == "__main__":
    main()