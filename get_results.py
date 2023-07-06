import asyncio
import logging
import os
import statistics

import click
import redis.asyncio as aioredis
from tabulate import tabulate

from config import NUM_EXECUTIONS, total_workers
from logger import setup_logger

logger = logging.getLogger(__name__)


async def results(redis_server: str):
    r = aioredis.from_url(redis_server, decode_responses=True)

    # there are total_workers // 2 mappers and total_workers // 2 reducers
    map_partitions = reduce_partitions = total_workers // 2

    exchange_times = []
    mapper_tables = []
    reducer_tables = []

    for run in range(NUM_EXECUTIONS):
        logger.info(f"==================== RUN {run} ====================")

        mappers = asyncio.gather(
            *[
                r.hgetall(f"{run}/mapper/{i}")
                for i in range(map_partitions)
            ]
        )
        reducers = asyncio.gather(
            *[
                r.hgetall(f"{run}/reducer/{i}")
                for i in range(reduce_partitions)
            ]
        )
        mappers, reducers = await asyncio.gather(mappers, reducers)

        # mappers = [
        #     json.loads(storage.get_object(
        #         bucket=timestamp_bucket,
        #         key=f"{timestamp_prefix}/timestamps/mapper_{i}.json"
        #     )) for i in range(map_partitions)
        # ]
        # reducers = [
        #     json.loads(storage.get_object(
        #         bucket=timestamp_bucket,
        #         key=f"{timestamp_prefix}/timestamps/reducer_{i}.json"
        #     )) for i in range(reduce_partitions)
        # ]

        mapper_timestamps = [float(m["write_start"]) for m in mappers]
        reducers_timestamps = [float(r["read_finish"]) for r in reducers]
        logger.info(f"Mapper timestamps: {mapper_timestamps}")
        logger.info(f"Reducer timestamps: {reducers_timestamps}")
        exchange_time = max(reducers_timestamps) - min(mapper_timestamps)
        exchange_times.append(exchange_time)
        logger.info(f"Exchange duration: {exchange_time}")

        mapper_table = [[m["id"], m["scan_time"], m["partition_time"], m["exchange_time"]] for m in mappers]
        reducer_table = [[r["id"], r["exchange_time"], r["sort_time"], r["write_time"]] for r in reducers]
        mapper_tables.append(mapper_table)
        reducer_tables.append(reducer_table)

    await r.close()

    for run in range(NUM_EXECUTIONS):
        logger.info(f"Mapper table {run}:")
        print(tabulate(mapper_tables[run], headers=["id", "scan", "partition", "exchange"]))
        logger.info(f"Reducer table {run}:")
        print(tabulate(reducer_tables[run], headers=["id", "exchange", "sort", "write"]))

    logger.info(f"Exchange times: {exchange_times}")
    # remove the first run
    exchange_times = exchange_times[1:]
    avg = sum(exchange_times) / len(exchange_times)
    stdev = statistics.stdev(exchange_times)
    logger.info(f"Exchange time: {avg},{stdev} (average,stdev)")


@click.command()
@click.argument("redis_server", type=str, default=os.environ.get("REDIS_SERVER", "localhost"), nargs=1)
def main(redis_server: str):
    setup_logger(log_level=logging.INFO)
    asyncio.run(results(redis_server))


if __name__ == "__main__":
    main()
