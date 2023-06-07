import asyncio
import glob
import os
import random
from multiprocessing import Process, Pool

from lithops import Storage
import cloudpickle as pickle
from sample import Sample
from sort import scan, partition, exchange_write, exchange_read, sort, write
from aio_pika import connect_robust, ExchangeType

# Assumes data is in "/tmp/lithops/sandbox/terasort-5m"
in_bucket = "benchmark-objects"
out_bucket = "stepan-lithops-sandbox"
timestamp_bucket = "stepan-lithops-sandbox"
key = "terasort-5m"
sort_key = "0"
names = ["0", "1"]
types = {
    "0": "string[pyarrow]",
    "1": "string[pyarrow]"
}
map_partitions = 5
reduce_partitions = 5

rabbitmq_url = 'amqp://guest:guest@localhost:5672/'

exchange_name = 'sort'
queue_prefix = 'reducer'


async def sequential_sort(map_partitions: int,
                          reduce_partitions: int):

    random.seed(0)

    storage = Storage()

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

    with Pool(processes=max(map_partitions, reduce_partitions)) as pool:
        pool.starmap(mapper, [
            (exchange_name, map_partitions, partition_id, reduce_partitions, segment_info)
            for partition_id in range(map_partitions)
        ])
        pool.starmap(reducer, [
            (exchange_name, map_partitions, partition_id)
            for partition_id in range(reduce_partitions)
        ])

    mapper_timestamps = [
        float(storage.get_object(timestamp_bucket, f"mapper_{i}")) for i in range(map_partitions)
    ]

    reducers_timestamps = [
        float(storage.get_object(timestamp_bucket, f"reducer_{i}")) for i in range(reduce_partitions)
    ]

    print(f"Mapper timestamps: {mapper_timestamps}")
    print(f"Reducer timestamps: {reducers_timestamps}")

    print(f"Exchange duration: {max(reducers_timestamps) - min(mapper_timestamps)}")


def mapper(
        exchange_name: str,
        map_partitions: int,
        partition_id: int,
        reduce_partitions: int,
        segment_info: list
):
    async def _mapper():
        connection = await connect_robust(rabbitmq_url)
        channel = await connection.channel()
        exchange = await channel.declare_exchange(exchange_name, durable=True, type=ExchangeType.DIRECT)
        storage = Storage()
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
        print(f"Mapper {partition_id} got {len(partition_obj)} rows.")
        # Calculate the destination worker for each row
        hash_list = partition(partition=partition_obj,
                              segment_info=segment_info,
                              sort_key=sort_key)
        # Write the subpartition corresponding to each worker
        await exchange_write(channel=channel,
                             storage=storage,
                             partition_obj=partition_obj,
                             partition_id=partition_id,
                             num_partitions=reduce_partitions,
                             exchange=exchange,
                             queue_prefix=queue_prefix,
                             timestamp_bucket=timestamp_bucket,
                             hash_list=hash_list)
        await channel.close()
        await connection.close()
    asyncio.run(_mapper())


def reducer(
        exchange_name: str,
        map_partitions: int,
        partition_id: int
):
    async def _reducer():
        connection = await connect_robust(rabbitmq_url)
        channel = await connection.channel()
        exchange = await channel.declare_exchange(exchange_name, durable=True, type=ExchangeType.DIRECT)
        storage = Storage()
        # Read the corresponding subpartitions from each mappers' output, and concat all of them
        partition_obj = await exchange_read(
            channel=channel,
            storage=storage,
            partition_id=partition_id,
            num_partitions=map_partitions,
            exchange=exchange,
            queue_prefix=queue_prefix,
            timestamp_bucket=timestamp_bucket,
        )
        print(f"Reducer {partition_id} got {len(partition_obj)} rows.")
        # Sort the partition
        sort(
            partition_obj=partition_obj,
            sort_key=sort_key
        )
        # Write the partition to persistent storage (object store)
        write(
            storage=storage,
            partition_obj=partition_obj,
            partition_id=partition_id,
            bucket=out_bucket,
        )
        await channel.close()
        await connection.close()
    asyncio.run(_reducer())


if __name__ == "__main__":
    asyncio.run(sequential_sort(map_partitions, reduce_partitions))
