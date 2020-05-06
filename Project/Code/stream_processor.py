# Created by Chenye Yang on 2020/5/1.
# Copyright © 2020 Chenye Yang. All rights reserved.

import socket
import time
import threading

# format the print output
class Color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

# stream processor
class Stream_Processor():
    def __init__(self, H, T, k, X):
        self.H = H
        self.T = T
        self.k = k
        self.X = X

    def change_parameter(self, H, T, k, X):
        self.H = H
        self.T = T
        self.k = k
        self.X = X

    # List protocols that are consuming more than H percent of the total external
    # bandwidth over the last T time units
    def function1(self):

        return

    # List the top-k most resource intensive protocols over the last T time units
    def function2(self):

        return

    # List all protocols that are consuming more than X times the standard deviation of
    # the average traffic consumption of all protocols over the last T time units
    def function3(self):

        return

    # List IP addresses that are consuming more than H percent of the total external
    # bandwidth over the last T time units
    def function4(self):

        return

    # List the top-k most resource intensive IP addresses over the last T time units
    def function5(self):

        return

    # List all IP addresses that are consuming more than X times the standard deviation
    # of the average traffic consumption of all IP addresses over the last T time units
    def function6(self):

        return

# control receiving thread of stream processor
class Recv_Control_Thread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def recv_control(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 12303))  # port localhost:12303 is used to receive control signal
        sock.listen(5)  # the max connection number, FIFO
        print(
            '{}{}GOOD:{}{} Connection complete. Stream Processor is listening control signal, from port 12303.'.format(
                Color.GREEN, Color.BOLD, Color.END, Color.END))
        while True:  # wait for connection
            connection, address = sock.accept()
            control = connection.recv(1024).decode("utf-8")
            if control == 'stop_receive_Data_Thread':
                connection.send(b'Stop receiving data signal received, closing now')
                global stop_receive_Thread
                stop_receive_Thread = True
            # elif control == 'start_send_Thread':
            #     connection.send(b'Start signal received, starting now')
            #     send_Thread = Send_Thread(threadID=1, name='send_Thread')
            #     send_Thread.start()
            #     send_Thread.join()
            connection.send(b'Control signal received')
            connection.close()
            print('The control signal received is: ' + control)

    def run(self) -> None:
        # override run() in Thread. When start() is called, run() is called.
        self.recv_control()

# data receiving thread of stream processor
class Recv_Data_Thread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def recv_data(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 12301))  # port localhost:12301 is used to receive generated data
        sock.listen(5)  # the max connection number, FIFO
        print('{}{}GOOD:{}{} Connection complete. Stream Processor is listening data, from port 12301.'.format(
            Color.GREEN, Color.BOLD, Color.END, Color.END))
        while True:  # wait for connection
            connection, address = sock.accept()
            try:
                while True:  # wait for data
                    global stop_receive_Thread
                    if stop_receive_Thread:
                        stop_receive_Thread = False # reset this Flag so that another send_Thread can start
                        connection.close() # close socket connection
                        sock.close() # close socket before exit
                        return # then this thread is terminated
                    data = connection.recv(1024).decode("utf-8")
                    print('Received data: {}'.format(data))
                    connection.send(b'Data received')
            except socket.error:
                print(
                    '{}{}ERROR:{}{} Connection is closed by a peer. Closing our connection and wait for new one'.format(
                        Color.RED, Color.BOLD, Color.END, Color.END))
            connection.close()

    def run(self) -> None:
        # override run() in Thread. When start() is called, run() is called.
        self.recv_data()

if __name__ == "__main__":
    # global variables
    stop_receive_Thread = False
    print('Stream Processor is starting'.center(100, '*'))
    # initialize classes
    recv_Data_Thread = Recv_Data_Thread(threadID=1, name='recv_Data_Thread')
    recv_Control_Thread = Recv_Control_Thread(threadID=2, name='recv_Control_Thread')
    # start threads
    recv_Data_Thread.start()
    print('{}{}GOOD:{}{} Data receiving thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
    recv_Control_Thread.start()
    print('{}{}GOOD:{}{} Control receiving thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
    # wait till threads terminate
    recv_Data_Thread.join()
    recv_Control_Thread.join()
