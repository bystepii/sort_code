import asyncio
import glob
import os
import random

from lithops import Storage
import cloudpickle as pickle
from sample import Sample
from sort import scan, partition, exchange_write, exchange_read, sort, write
from aio_pika import connect_robust, ExchangeType

# Assumes data is in "/tmp/lithops/sandbox/terasort-5m"
bucket = "sandbox"
intermediate_bucket = "sandbox/intermediates"
key = "terasort-5m"
sort_key = "0"
names = ["0", "1"]
types = {
    "0": "string[pyarrow]",
    "1": "string[pyarrow]"
}
map_partitions = 5
reduce_partitions = 5

rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')

exchange_name = 'sort'
queue_prefix = 'reducer'


async def sequential_sort(map_partitions: int,
                          reduce_partitions: int):

    random.seed(0)

    storage = Storage(backend="localhost")

    connection = await connect_robust(host=rabbitmq_host)
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

    sampler = Sample(
        bucket=bucket,
        key=key,
        sort_key=sort_key,
        num_partitions=reduce_partitions,
        delimiter=",",
        names=names,
        types=types
    )

    sampler.set_storage()

    segment_info = pickle.loads(sampler.run())

    for partition_id in range(map_partitions):

        # Get partition for this worker from persistent storage (object store)
        partition_obj = scan(
            storage=storage,
            bucket=bucket,
            key=key,
            partition_id=partition_id,
            num_partitions=5,
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
                             partition_obj=partition_obj,
                             partition_id=partition_id,
                             num_partitions=reduce_partitions,
                             exchange=exchange,
                             queue_prefix=queue_prefix,
                             hash_list=hash_list)


    for partition_id in range(reduce_partitions):

        # Read the corresponding subpartitions from each mappers' output, and concat all of them
        partition_obj = await exchange_read(
            channel=channel,
            partition_id=partition_id,
            num_partitions=map_partitions,
            exchange=exchange,
            queue_prefix=queue_prefix,
        )

        print(f"Reducer {partition_id} got {len(partition_obj)} rows.")

        # Sort the partition
        sort(
            partition_obj=partition_obj,
            sort_key = sort_key
        )

        # Write the partition to persistent storage (object store)
        write(
            storage = storage,
            partition_obj=partition_obj,
            partition_id=partition_id,
            bucket=bucket
        )

    # Remove intermediates
    for f in glob.glob("/tmp/lithops/sandbox/intermediates/*"):
        os.remove(f)

    await channel.close()
    await connection.close()


if __name__ == "__main__":
    asyncio.run(sequential_sort(map_partitions, reduce_partitions))