# Created by Chenye Yang on 2020/5/1.
# Copyright © 2020 Chenye Yang. All rights reserved.

import socket

if __name__ == "__main__":

    print("Stream Processor is starting")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', 12301))  # port localhost:12301 is used to receive generated data
    sock.listen(5)  # the max connection number, FIFO
    print("Stream Processor is listening port 16889, with max connection 5")

    while True: # wait for connection
        connection, address = sock.accept()
        try:
            while True: # wait for data
                buf = connection.recv(1024)
                print("Get value " + str(buf))
                if buf == b'1':
                    print("send welcome")
                    connection.send(b'welcome to server!')
                elif buf != b'0':
                    connection.send(b'please go out!')
                    print("send refuse")
                else:
                    print("close")
                    break
        except socket.error:
            print('A peer closed the connection.')
        print('Closing our connection and wait for new connection')
        connection.close()
