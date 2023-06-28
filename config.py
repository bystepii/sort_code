NUM_EXECUTIONS = 3

in_bucket = "benchmark-objects"
out_bucket = "stepan-lithops-sandbox"
timestamp_bucket = "stepan-lithops-sandbox"
intermediate_bucket = "stepan-lithops-sandbox"

parallel = True
use_rabbitmq = True

key = "terasort-5m"

burst_size = 2
total_workers = 8

exchange_name = 'sort'
queue_prefix = 'reducer'
sort_key = "0"
names = ["0", "1"]
types = {
    "0": "string[pyarrow]",
    "1": "string[pyarrow]"
}
