# Created by Chenye Yang on 2020/5/1.
# Copyright © 2020 Chenye Yang. All rights reserved.

import socket

if __name__ == "__main__":
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 12302)) # port localhost:12302 to send control signal to data_generator
    control = b'1' # control signal to send
    sock.send(control)
    print(sock.recv(1024).decode("utf-8"))
    sock.close()
