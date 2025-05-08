import socket
import sys


def main():
    if len(sys.argv) != 4:
        print("Usage: python client.py <hostname> <port> <request_file>")
        sys.exit(1)
    hostname, port, file_path = sys.argv[1], int(sys.argv[2]), sys.argv[3]

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((hostname, port))
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(' ', 2)
                    if len(parts) < 2 or len(parts) > 3:
                       print(f"Invalid format: {line}")
                       continue
                    cmd, key = parts[0], parts[1]
                    value = parts[2] if len(parts) == 3 else None

                    collated = f"{key} {value}" if value else key
                    if len(collated) > 970:
                       print(f"Error: collated size exceeds 970 characters in {line}")
                       continue