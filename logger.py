import logging.config
import sys


def setup_logger(log_level=logging.INFO):
    root = logging.getLogger()
    root.setLevel(log_level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s [PID:%(process)d] [%(levelname)s] %(filename)s:%(lineno)s -- %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)
