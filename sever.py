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

    def print_stats(self):
        """Print current statistical information"""
        with self.lock:
            count = len(self.tuple_space)
            avg_tuple = self.stats["total_tuple_size"] / count if count else 0
            avg_key = self.stats["total_key_size"] / count if count else 0
            avg_value = self.stats["total_value_size"] / count if count else 0
            print(f"\n=== SERVER STATS ({time.ctime()}) ===")
            print(f"Tuples: {count}, Avg Tuple Size: {avg_tuple:.2f}")
            print(f"Avg Key Size: {avg_key:.2f}, Avg Value Size: {avg_value:.2f}")
            print(f"Clients: {self.stats['client_count']}, Total Ops: {self.stats['total_ops']}")
            print(f"READs: {self.stats['reads']}, GETs: {self.stats['gets']}, PUTs: {self.stats['puts']}")
            print(f"Errors: {self.stats['errors']}\n")

    def print_stats_loop(self):
        """Print statistical information every 10 seconds"""
        while True:
            time.sleep(10)
            self.print_stats()
