import os
import time
from typing import List, Union, TextIO, BinaryIO
from botocore.exceptions import ClientError
from lithops import Storage


def write_obj(storage: Storage,
              Bucket: str,
              Key: str,
              Body: Union[str, bytes, TextIO, BinaryIO],
              sufixes: List[str] = [],
              delimiter: str = "_",
              timed: bool = False) \
        -> dict:
    try:

        if timed:
            start_time = time.time()
        storage.put_object(Bucket,
                           delimiter.join([Key] + sufixes),
                           Body)

        if timed:
            return {
                "time": time.time() - start_time,
                "size": len(Body)
            }
    except ClientError as e:
        raise


def read_obj(storage: Storage,
             Bucket: str,
             Key: str,
             sufixes: List[str],
             timed: bool = False,
             Range: tuple = None):

    if Range is not None:
        extra_get_args = {"Range": "bytes=%d-%d" % (Range[0], Range[1])}
    else:
        extra_get_args = {}

    if timed: start_time = time.time()
    obj = storage.get_object(bucket=Bucket,
                             key="_".join([Key] + [ str(s) for s in sufixes]),
                             extra_get_args=extra_get_args)

    if timed:
        return obj, {
            "time": time.time() - start_time,
            "size": len(obj)
        }
    else:
        return obj
