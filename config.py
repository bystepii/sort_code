NUM_EXECUTIONS = 4

in_bucket = "stepan-benchmark-objects"
out_bucket = "stepan-lithops-sandbox"
timestamp_bucket = "stepan-lithops-sandbox"
intermediate_bucket = "stepan-lithops-sandbox"

parallel = True
use_rabbitmq = False

key = "terasort-5g"

burst_size = 10
total_workers = 20

num_machines = total_workers // 2 // burst_size

exchange_name = 'sort'
queue_prefix = 'reducer'
sort_key = "0"
names = ["0", "1"]
types = {
    "0": "string[pyarrow]",
    "1": "string[pyarrow]"
}