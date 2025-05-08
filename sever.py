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

    def handle_client(self, conn, addr):
        """Handle individual client connections"""
        self.stats["client_count"] += 1
        try:
            while True:
                data = self.receive_full_message(conn)
                if not data:
                    break

                req_len = int(data[:3])
                cmd = data[3]
                key = data[5:data.find(' ', 5)] if cmd != 'P' else data[5:data.find(' ', 5)]
                value = data[data.find(' ', 5) + 1:] if cmd == 'P' else None

                with self.lock:
                     self.stats["total_ops"] += 1
                     response = ""
                     if cmd == 'R':
                         self.stats["reads"] += 1
                         response = self.handle_read(key)
                     elif cmd == 'G':
                         self.stats["gets"] += 1
                         response = self.handle_get(key)
                     elif cmd == 'P':
                         self.stats["puts"] += 1
                         response = self.handle_put(key, value)
                     else:
                         response = self.generate_error_response(key, "invalid command")
                        
                conn.sendall(response.encode('utf-8'))

