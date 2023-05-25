import asyncio
import functools
import gc
import http
import io
from math import floor

from botocore.exceptions import ClientError
from lithops import Storage
from typing import Tuple, Union, Dict, List
import pandas as pd
import time
from io import BytesIO, StringIO
import numpy as np

from IO import write_obj, read_obj

KB = 1024
MB = 1024 * KB
GB = 1024 * MB
BOUND_EXTRACTION_MARGIN = KB
MAX_RETRIES: int = 5
MAX_READ_TIME: int = 15
RETRY_WAIT_TIME: float = 2.5

def adjust_bounds(part: Union[str, bytes],
                  start_byte: int,
                  end_byte: int) \
        -> memoryview:

    total_size = len(part)

    if start_byte > 0:

        lower_bound = start_byte

        while lower_bound > 0:
            if part[lower_bound:lower_bound + 1] == b'\n':
                lower_bound += 1
                break
            else:
                lower_bound -= 1
    else:
        lower_bound = 0

    if end_byte < total_size:

        upper_bound = end_byte

        while upper_bound < total_size:
            if part[upper_bound:upper_bound + 1] == b'\n':
                break
            else:
                upper_bound += 1
    else:
        upper_bound = end_byte

    bm = memoryview(part)
    return bm[lower_bound:upper_bound]


def part_to_IO(read_part) \
        -> Union[BytesIO, StringIO]:

    if isinstance(read_part, str):
        read_part = StringIO(read_part)
    if isinstance(read_part, bytes) or isinstance(read_part, memoryview):
        read_part = BytesIO(read_part)

    return read_part


def read_and_adjust(storage: Storage,
                    read_bucket: str,
                    read_path: str,
                    lower_bound: int,
                    upper_bound: int,
                    total_size: int,
                    delimiter: str = ",",
                    names: List[str] = None,
                    types: Dict[str, str] = None) \
        -> Tuple[pd.DataFrame, int, float]:

    lower_bound2 = max(0, lower_bound - BOUND_EXTRACTION_MARGIN)
    upper_bound2 = min(total_size, upper_bound + BOUND_EXTRACTION_MARGIN)

    start_time = time.time()

    read_part = storage.get_object(read_bucket, read_path,
                                   extra_get_args={"Range": ''.join(
                                       ['bytes=', str(lower_bound2), '-',
                                        str(upper_bound2)])
                                   })
    end_time = time.time()

    read_part = adjust_bounds(read_part, lower_bound - lower_bound2, upper_bound - lower_bound2)

    part_length = len(read_part)

    read_part = part_to_IO(read_part)

    df = pd.read_csv(read_part,
                     engine='c',
                     index_col=None,
                     header=None,
                     delimiter=delimiter,
                     names=names,
                     dtype=types,
                     quoting=3,
                     on_bad_lines="warn")

    return df, part_length, end_time - start_time

def get_data_size(storage: Storage,
                  bucket:str,
                  path:str) \
        -> int:
    return int(storage.head_object(bucket, path)['content-length'])


def _get_read_range(storage: Storage,
                    bucket: str, key: str,
                    partition_id: int,
                    num_partitions: int)\
        -> Tuple[int, int]:
    """
    Calculate byte range to read from a dataset, given the id of the partition.
    """

    total_size = get_data_size(storage, bucket, key)

    partition_size = floor(total_size / num_partitions)

    lower_bound = partition_id * partition_size
    upper_bound = lower_bound + partition_size + 1

    print("Scanning bytes=%d-%d (%d)" % (lower_bound, upper_bound,
                                         upper_bound - lower_bound))

    return lower_bound, upper_bound


def serialize(partition_obj: pd.DataFrame) -> bytes:

    return partition_obj.to_parquet(engine="pyarrow", compression="snappy", index=False)


def deserialize(b: bytes) -> object:

    return pd.read_parquet(io.BytesIO(b), engine="pyarrow")


def serialize_partitions(num_partitions: int,
                         partition_obj: pd.DataFrame,
                         hash_list: np.ndarray)\
        -> Dict[int, bytes]:

    serialized_partitions = {}

    for destination_partition in range(num_partitions):

        serialization_result = _serialize_partition(destination_partition,
                                                    partition_obj,
                                                    hash_list)

        serialized_partitions[destination_partition] = serialization_result

    return serialized_partitions


def _serialize_partition(partition_id: int,
                         partition_obj: pd.DataFrame,
                         hash_list: np.ndarray)\
        -> bytes:


    # Get rows corresponding to this worker
    pointers_ni = np.where(hash_list == partition_id)[0]

    pointers_ni = np.sort(pointers_ni.astype("uint32"))

    obj = serialize(partition_obj.iloc[pointers_ni])

    return obj


def _writer_multiple_files(
        storage: Storage,
        subpartitions: Dict[int, bytes],
        partition_id: int,
        bucket: str):

    async def _writes(_subpartitions):
        loop = asyncio.get_event_loop()

        objects = await asyncio.gather(
            *[
                loop.run_in_executor(None, functools.partial(write_obj,
                                                             storage=storage,
                                                             Bucket=bucket,
                                                             Key=str(partition_id),
                                                             sufixes=[str(subpartition_data[0])],
                                                             Body=subpartition_data[1]))
                for subpartition_data in _subpartitions
            ]
        )
        return objects

    loop = asyncio.get_event_loop()
    loop.run_until_complete(_writes(subpartitions.items()))


def concat_progressive(
        subpartitions: List[bytes]) \
        -> pd.DataFrame:

    df = None

    for r_i, r in enumerate(subpartitions):

        new_chunk = deserialize(subpartitions[r_i])

        # Remove data for memory efficiency
        subpartitions[r_i] = b""
        gc.collect()

        if df is None:
            df = new_chunk
        else:
            df = pd.concat([df, new_chunk], ignore_index=True)

    return df


def reader(source_partition: int,
           destiny_partition: int,
           bucket: str,
           storage: Storage) \
        -> bytes:

    retry = 0

    before_readt = time.time()

    while retry < MAX_RETRIES:

        try:

            data = read_obj(
                storage=storage,
                Bucket=bucket,
                Key = str(source_partition),
                sufixes=[str(destiny_partition)]
            )

            return data


        except ClientError as ex:

            if ex.response['Error']['Code'] == 'NoSuchKey':
                if time.time() - before_readt > MAX_READ_TIME:
                    return None
            time.sleep(RETRY_WAIT_TIME)
            continue

        except (http.client.IncompleteRead) as e:

            if retry == MAX_RETRIES:
                return None
            retry += 1
            continue

        except Exception as e:

            print(e)

            return None