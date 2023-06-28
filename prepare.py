import logging
import random

from lithops import Storage

from config import in_bucket, key, total_workers, sort_key, names, types, intermediate_bucket
from logger import setup_logger
from sample import Sample


logger = logging.getLogger(__name__)


def main():
    storage = Storage()

    random.seed(0)

    sampler = Sample(
        bucket=in_bucket,
        key=key,
        sort_key=sort_key,
        num_partitions=total_workers // 2,
        delimiter=",",
        names=names,
        types=types
    )

    sampler.set_storage(storage)

    storage.put_object(
        intermediate_bucket,
        key=f"{key}/{total_workers}/segment_info.pkl",
        body=sampler.run()
    )


if __name__ == '__main__':
    setup_logger(log_level=logging.INFO)
    main()
