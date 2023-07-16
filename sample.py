from typing import List, Dict
from lithops import Storage
from math import floor
import random
import numpy as np
from utils import read_and_adjust
import re
import cloudpickle as pickle


def get_data_size(storage: Storage, bucket: str, path: str):
    return int(storage.head_object(bucket, path)['content-length'])


MAX_SAMPLE_SIZE: int = 300 * 1024 * 1024
SAMPLE_RATIO: float = 0.01
SAMPLE_FRAGMENTS: int = 20
START_MARGIN: float = 0.02
END_MARGIN: float = 0.02
SAMPLE_SUFIX: str = "sampled"


class Sample():
    storage: Storage

    def __init__(self,
                 bucket: str,
                 key: str,
                 sort_key: str,
                 num_partitions: int,
                 delimiter: str = ",",
                 names: List[str] = None,
                 types: Dict[str, str] = None):

        self.bucket = bucket
        self.key = key
        self.sort_key = sort_key
        self.num_partitions = num_partitions
        self.delimiter = delimiter
        self.names = names
        self.types = types

    def set_storage(self, storage: Storage):
        self.storage = storage

    def run(self):

        ds_size = get_data_size(self.storage, self.bucket, self.key)

        # Avoid dataset head and tail
        start_limit = int(ds_size * START_MARGIN)
        end_limit = int(ds_size * (1 - END_MARGIN))
        choosable_size = end_limit - start_limit

        # Size of each sampled fragment
        fragment_size = floor(min(floor((end_limit - start_limit) * SAMPLE_RATIO), MAX_SAMPLE_SIZE) / SAMPLE_FRAGMENTS)

        # Select bounds randomly
        num_parts = int(choosable_size / fragment_size)
        selected_fragments = sorted(random.sample(range(num_parts), SAMPLE_FRAGMENTS))

        keys_arrays = []

        # Read from each bound a fragment size, adjusting limits
        for f in selected_fragments:
            lower_bound = start_limit + f * fragment_size
            upper_bound = lower_bound + fragment_size

            df, part_size, _ = read_and_adjust(storage=self.storage,
                                               read_bucket=self.bucket,
                                               read_path=self.key,
                                               lower_bound=lower_bound,
                                               upper_bound=upper_bound,
                                               total_size=end_limit,
                                               delimiter=self.delimiter,
                                               names=self.names,
                                               types=self.types)

            keys_arrays.append(np.array(df[self.sort_key]))

        # Concat keys, sort them
        keys = np.concatenate(keys_arrays)
        keys.sort()

        # Find quantiles (num tasks)
        quantiles = [i * 1 / self.num_partitions for i in range(1, self.num_partitions)]
        if str(df[self.sort_key].dtype) not in ['bytes', 'str', 'object', 'string'] and \
                re.match("\|S[0-9]*", str(df[self.sort_key].dtype)) is None:
            segment_bounds = [np.quantile(keys, q) for q in quantiles]
        else:
            segment_bounds = [keys[int(q * len(keys))] for q in quantiles]

        # write function (sort_key, op_sufix, task)
        return pickle.dumps(segment_bounds)

    def explain(self):

        return "%s (%s/%s)" % (self.__class__.__name__, self.bucket, self.key)

    def __str__(self):

        return str(self.__dict__)