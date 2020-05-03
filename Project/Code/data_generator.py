# Created by Chenye Yang on 2020/5/1.
# Copyright © 2020 Chenye Yang. All rights reserved.

import socket
import time
import threading
from random import randint

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

# data generator
class Data_Generator():
    def __init__(self, rate, ipNum, protocolNum, ipPercent, protocolPercent):
        self.rate = rate # <int> Hz
        self.ipNum = ipNum # <int> less than or equal to 15
        self.protocolNum = protocolNum # <int> less than or equal to 19
        self.ipPercent = ipPercent # <float list>
        self.protocolPercent = protocolPercent # <float list>
        self.protocols = ['SOAP', 'SSDP', 'TCAP', 'UPnP', 'DHCP', 'DNS', 'HTTP', 'HTTPS', 'NFS', 'POP3', 'SMTP', 'SNMP',
                          'FTP', 'NTP', 'IRC', 'Telnet', 'SSH', 'TFTP', 'AMQP']
        self.ips = ['53.215.218.189', '133.98.231.165', '222.186.237.75', '11.71.50.83', '45.43.227.63',
                    '116.168.68.91', '20.232.17.27', '158.223.93.237', '84.191.253.211', '153.17.103.198',
                    '224.80.117.250', '97.211.109.139', '21.50.108.54', '109.126.189.56', '90.227.18.21']
    def data_generator(self):
        packetSize = randint(16, 12288) # generate the size of network packet, Byte



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
            sock.close() # close socket before exit
            return # then this thread is terminated
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
                    sock.close() # close socket before exit
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
    def run(self) -> None: # override run() in Thread. When start() is called, run() is called.
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


