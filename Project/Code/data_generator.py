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

# data sending thread of data generator
class Send_Data_Thread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def send_data(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(('localhost', 12301)) # port localhost:12301 is used to send data
        except socket.error:
            print('{}{}ERROR:{}{} Stream Processor is NOT listening for data. Start it, then restart send_Data_Thread.'.format(Color.RED, Color.BOLD, Color.END, Color.END))
            sock.close()
            return
        data = b'1'
        print('{}{}GOOD:{}{} Connection complete. Data Generator is sending generated data, to port 12301.'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
        try:
            while True:
                time.sleep(3)
                print('Data sending: ' + str(data))
                sock.send(data)
                sock.recv(1024).decode("utf-8")
                global stop_send_Thread
                if stop_send_Thread:
                    stop_send_Thread = False # reset this Flag so that another send_Thread can start
                    sock.close()
                    return # break the loop and then this thread is terminated
        except socket.error:
            print('Connection is closed by a peer. Waiting for start send_Data_Thread manually.')
    def run(self) -> None: # override run() in Thread. When start() is called, run() is called.
        # print('Start sending generated data')
        self.send_data()

# control receiving thread of data generator
class Recv_Control_Thread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def recv_control(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 12302))  # port localhost:12302 is used to receive control signal
        sock.listen(5)  # the max connection number, FIFO
        print('{}{}GOOD:{}{} Connection complete. Data Generator is listening control signal, from port 12302.'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
        while True:  # wait for connection
            connection, address = sock.accept()
            control = connection.recv(1024).decode("utf-8")
            if control == 'stop_send_Thread':
                connection.send(b'Stop signal received, closing now')
                global stop_send_Thread
                stop_send_Thread = True
            elif control == 'start_send_Thread':
                connection.send(b'Start signal received, starting now')
                send_Data_Thread = Send_Data_Thread(threadID=1, name='send_Data_Thread')
                send_Data_Thread.start()
                send_Data_Thread.join()
            connection.send(b'received')
            connection.close()
            print('The control signal received is: ' + control)
    def run(self) -> None:
        # print('Start listening to control signal')
        self.recv_control()


if __name__ == "__main__":
    global stop_send_Thread
    stop_send_Thread = False
    print(' Data Generator is starting '.center(100, '*'))
    # initialize classes
    send_Data_Thread = Send_Data_Thread(threadID=1, name='send_Data_Thread')
    recv_Control_Thread = Recv_Control_Thread(threadID=2, name='recv_Control_Thread')
    # start threads
    send_Data_Thread.start()
    print('{}{}GOOD:{}{} Data sending thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
    recv_Control_Thread.start()
    print('{}{}GOOD:{}{} Control receiving thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
    # wait till threads terminate
    send_Data_Thread.join()
    recv_Control_Thread.join()


