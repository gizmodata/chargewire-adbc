"""A recording TCP proxy, run as its own process (the test process may be inside the driver
library while it connects): ``python tcp_recorder.py TARGET_PORT LOG`` forwards
127.0.0.1:<free port> to 127.0.0.1:TARGET_PORT byte for byte, appends every byte a client sends
to LOG, and prints its port."""

import socket
import sys
import threading

TARGET_PORT = int(sys.argv[1])
LOG = sys.argv[2]
lock = threading.Lock()


def pipe(src: socket.socket, dst: socket.socket, record: bool) -> None:
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            if record:
                with lock, open(file=LOG, mode="ab") as log:
                    log.write(data)
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for sock in (src, dst):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


listener = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
listener.bind(("127.0.0.1", 0))
listener.listen(64)
print(listener.getsockname()[1], flush=True)
while True:
    client, _ = listener.accept()
    try:
        upstream = socket.create_connection(address=("127.0.0.1", TARGET_PORT))
    except OSError:
        client.close()
        continue
    threading.Thread(target=pipe, args=(client, upstream, True), daemon=True).start()
    threading.Thread(target=pipe, args=(upstream, client, False), daemon=True).start()
