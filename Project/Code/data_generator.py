# Created by Chenye Yang on 2020/5/1.
# Copyright © 2020 Chenye Yang. All rights reserved.

import socket
import time
import sys
import threading

# data sending thread of data generator
class Send_Thread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def send_data(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 12301)) # port localhost:12301 is used to send data
        data = b'1'
        print('Connection complete. Now sending generated data')
        while True:
            time.sleep(3)
            print('send to server with value: ' + str(data))
            sock.send(data)
            print(sock.recv(1024).decode("utf-8"))
            data = (data == b'1') and b'2' or b'1'  # change to another type of value each time
            global stop_send_Thread
            if stop_send_Thread:
                stop_send_Thread = False # reset this Flag so that another send_Thread can start
                break # break the loop and then this thread is terminated
    def run(self) -> None: # override run() in Thread. When start() is called, run() is called.
        print('Start sending generated data')
        self.send_data()

# control receiving thread of data generator
class Recv_Thread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def recv_control(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 12302))  # port localhost:12302 is used to receive control signal
        sock.listen(5)  # the max connection number, FIFO
        print('Connection complete. Now listening to control signal')
        while True:  # wait for connection
            connection, address = sock.accept()
            control = connection.recv(1024).decode("utf-8")
            if control == 'stop_send_Thread':
                connection.send(b'Stop signal received, closing now')
                global stop_send_Thread
                stop_send_Thread = True
            elif control == 'start_send_Thread':
                connection.send(b'Start signal received, starting now')
                send_Thread = Send_Thread(threadID=1, name='send_Thread')
                send_Thread.start()
                send_Thread.join()
            connection.send(b'received')
            connection.close()
            print('The control signal received is: ' + control)
    def run(self) -> None:
        print('Start listening to control signal')
        self.recv_control()


if __name__ == "__main__":
    global stop_send_Thread
    stop_send_Thread = False
    # initialize classes
    send_Thread = Send_Thread(threadID=1, name='send_Thread')
    recv_Thread = Recv_Thread(threadID=2, name='recv_Thread')
    # start threads
    send_Thread.start()
    recv_Thread.start()
    # wait till threads terminate
    send_Thread.join()
    recv_Thread.join()


