import socket
import sys


def main():
    if len(sys.argv) != 4:
        print("Usage: python client.py <hostname> <port> <request_file>")
        sys.exit(1)
    hostname, port, file_path = sys.argv[1], int(sys.argv[2]), sys.argv[3]