NUM_EXECUTIONS = 2

in_bucket = "stepan-benchmark-objects"
out_bucket = "stepan-lithops-sandbox"
timestamp_bucket = "stepan-lithops-sandbox"
intermediate_bucket = "stepan-lithops-sandbox"

parallel = True
use_rabbitmq = True

key = "terasort-1g"

burst_size = 4
total_workers = 24

exchange_name = 'sort'
queue_prefix = 'reducer'
sort_key = "0"
names = ["0", "1"]
types = {
    "0": "string[pyarrow]",
    "1": "string[pyarrow]"
}
