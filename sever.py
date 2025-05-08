import socket
import threading
import time
from collections import defaultdict

class TupleSpaceServer:
    def __init__(self, port):
        self.port = port
        self.tuple_space = {}
        self.lock = threading.Lock()
        self.stats = {
            "client_count": 0,
            "total_ops": 0,
            "reads": 0,
            "gets": 0,
            "puts": 0,
            "errors": 0,
            "total_tuple_size": 0,
            "total_key_size": 0,
            "total_value_size": 0
        }

        threading.Thread(target=self.print_stats_loop, daemon=True).start()