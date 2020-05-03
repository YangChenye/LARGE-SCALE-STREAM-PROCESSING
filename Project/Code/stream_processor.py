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
        print('{}{}GOOD:{}{} Connection complete. Stream Processor is listening control signal, from port 12303.'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
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
        # print('Start listening to control signal')
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
        print('{}{}GOOD:{}{} Connection complete. Stream Processor is listening data, from port 12301.'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
        while True:  # wait for connection
            connection, address = sock.accept()
            try:
                while True:  # wait for data
                    global stop_receive_Thread
                    if stop_receive_Thread:
                        stop_receive_Thread = False
                        connection.close()
                        sock.close()
                        return
                    data = connection.recv(1024).decode("utf-8")
                    print('Received data: {}'.format(data))
                    connection.send(b'Data received')
            except socket.error:
                print('{}{}ERROR:{}{} Connection is closed by a peer. Closing our connection and wait for new one'.format(Color.RED, Color.BOLD, Color.END, Color.END))
            connection.close()
    def run(self) -> None:
        # print('Start listening to data signal')
        self.recv_data()

if __name__ == "__main__":
    global stop_receive_Thread
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
