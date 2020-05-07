# Created by Chenye Yang on 2020/5/1.
# Copyright © 2020 Chenye Yang. All rights reserved.

import random
import socket
import threading
import time
from datetime import datetime
import pytz
import asyncio
import websockets

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
        # what if the percent list is wrong
        if len(ipPercent) != ipNum:
            self.ipPercentPDF = [1 / ipNum for i in range(ipNum)]
        else:
            self.ipPercentPDF = ipPercent
        if len(protocolPercent) != protocolNum:
            self.protocolPercentPDF = [1 / protocolNum for i in range(protocolNum)]
        else:
            self.protocolPercentPDF = protocolPercent
        # convert the pdf list to cdf list
        self.ipPercent = self.PDF2CDF(self.ipPercentPDF)
        self.protocolPercent = self.PDF2CDF(self.protocolPercentPDF)
        self.protocols = ['SOAP', 'SSDP', 'TCAP', 'UPnP', 'DHCP', 'DNS', 'HTTP', 'HTTPS', 'NFS', 'POP3', 'SMTP', 'SNMP',
                          'FTP', 'NTP', 'IRC', 'Telnet', 'SSH', 'TFTP', 'AMQP']
        self.ips = ['53.215.218.189', '133.98.231.165', '222.186.237.75', '11.71.50.83', '45.43.227.63',
                    '116.168.68.91', '20.232.17.27', '158.223.93.237', '84.191.253.211', '153.17.103.198',
                    '224.80.117.250', '97.211.109.139', '21.50.108.54', '109.126.189.56', '90.227.18.21']

    def PDF2CDF(self, PDF):
        # convert the pdf to cdf
        # pdf=[0.1, 0.2, 0.3, 0.4] -> cdf=[0.1, 0.3, 0.6, 1.0]
        CDF = PDF
        for i in range(1, len(CDF)):
            CDF[i] = CDF[i] + CDF[i-1]
        return CDF

    def data_generator(self):
        # At its finest level, individual flows are identified and associated with an application protocol,
        # a source and destination IP addresses, as well as a set of network packets and their sizes.

        # decide which protocol to generate
        r = random.random()
        i = 0
        for i in range(self.protocolNum):
            if r < self.protocolPercent[i]:
                break
        protocol = self.protocols[i]
        # decide which source IP to generate
        r = random.random()
        i = 0
        for i in range(self.ipNum):
            if r < self.ipPercent[i]:
                break
        sourceIP = self.ips[i]
        # decide which destination IP to generate
        r = random.random()
        i = 0
        for i in range(self.ipNum):
            if r < self.ipPercent[i]:
                break
        destinationIP = self.ips[i]
        packetSize = random.randint(16, 12288)  # generate the size of network packet, Byte
        time = datetime.now(tz=pytz.timezone('US/Eastern')).strftime('%Y-%m-%d_%H:%M:%S.%f')
        # time = datetime.now(tz=pytz.timezone('US/Eastern'))
        dataToSend = '{} {} {} {} {}'.format(time, protocol, sourceIP, destinationIP, packetSize)
        return dataToSend

# data sending thread of data generator
class Send_Data_Thread(threading.Thread):
    def __init__(self, threadID, name, rate, ipNum, protocolNum, ipPercent, protocolPercent):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.rate = rate  # <int> Hz
        self.ipNum = ipNum  # <int> less than or equal to 15
        self.protocolNum = protocolNum  # <int> less than or equal to 19
        self.ipPercent = ipPercent  # <float list>
        self.protocolPercent = protocolPercent  # <float list>

    def send_data(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        '''
        First attempt: 
        data_generator: socket client, sending data
        stream processor: socket server, receiving data
        '''
        # try:
        #     sock.connect(('localhost', 12301)) # port localhost:12301 is used to send data
        # except socket.error:
        #     print(
        #         '{}{}ERROR:{}{} Stream Processor is NOT listening for data. Start it, then restart send_Data_Thread.'.format(
        #             Color.RED, Color.BOLD, Color.END, Color.END))
        #     sock.close() # close socket before exit
        #     return # then this thread is terminated
        # # data = b'1'
        # data_Generator = Data_Generator(self.rate, self.ipNum, self.protocolNum, self.ipPercent, self.protocolPercent)
        # print('{}{}GOOD:{}{} Connection complete. Data Generator is sending generated data, to port 12301.'.format(
        #     Color.GREEN, Color.BOLD, Color.END, Color.END))
        # try:
        #     while True:
        #         time.sleep(1 / self.rate) # sending frequency
        #         data = '{}{}'.format(data_Generator.data_generator(), '\n').encode('utf-8')
        #         print('Data sending: ' + str(data))
        #         sock.send(data)
        #         sock.recv(1024).decode("utf-8")
        #         global stop_send_Thread
        #         if stop_send_Thread:
        #             stop_send_Thread = False # reset this Flag so that another send_Thread can start
        #             sock.close() # close socket before exit
        #             return # break the loop and then this thread is terminated
        # except socket.error:
        #     print('Connection is closed by a peer. Waiting for start send_Data_Thread manually.')
        '''
        Second attempt: 
        data_generator: socket server, wait for connection from spark streaming
        stream processor: spark streaming, initiative connect to socket server
        '''
        sock.bind(('localhost', 12301))
        sock.listen(5)
        print('{}{}GOOD:{}{} Connection complete. Data Generator is listening for spark stream, from port 12301.'.format(
            Color.GREEN, Color.BOLD, Color.END, Color.END))
        connection, address = sock.accept()
        data_Generator = Data_Generator(self.rate, self.ipNum, self.protocolNum, self.ipPercent, self.protocolPercent)
        try:
            while True:
                time.sleep(1 / self.rate)  # sending frequency
                data = '{}{}'.format(data_Generator.data_generator(), '\n').encode('utf-8')
                connection.send(data)
                global stop_send_Thread
                if stop_send_Thread:
                    stop_send_Thread = False # reset this Flag so that another send_Thread can start
                    sock.close() # close socket before exit
                    return # break the loop and then this thread is terminated
        except socket.error:
            print('Connection is closed by a peer. Please manually restart the data generator.')
            sock.close()

    def run(self) -> None:
        # override run() in Thread. When start() is called, run() is called.
        self.send_data()

# control receiving thread of data generator
class Recv_Control_Thread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def recv_control(self):
        '''using socket to communicate with web ui'''
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 12302))  # port localhost:12302 is used to receive control signal
        sock.listen(5)  # the max connection number, FIFO
        print('{}{}GOOD:{}{} Connection complete. Data Generator is listening for control signal, from port 12302.'.format(
            Color.GREEN, Color.BOLD, Color.END, Color.END))
        while True:  # wait for connection
            connection, address = sock.accept()
            control = connection.recv(1024).decode("utf-8")
            if control == 'stop_send_Thread':
                connection.send(b'Stop signal received, closing now')
                global stop_send_Thread
                stop_send_Thread = True
            elif control == 'start_send_Thread':
                connection.send(b'Start signal received, starting now')
                global rate
                global ipNum
                global protocolNum
                global ipPercent
                global protocolPercent
                send_Data_Thread = Send_Data_Thread(threadID=1, name='send_Data_Thread', rate=rate, ipNum=ipNum,
                                                    protocolNum=protocolNum, ipPercent=ipPercent,
                                                    protocolPercent=protocolPercent)
                send_Data_Thread.start()
                print('{}{}GOOD:{}{} Data sending thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
                send_Data_Thread.join()
            connection.send(b'received')
            connection.close()
            print('The control signal received is: ' + control)

    def run(self) -> None:
        # override run() in Thread. When start() is called, run() is called.
        self.recv_control()


# control receiving thread of data generator
def recv_control():
    '''using websocket to communicate with web ui'''
    async def generator(websocket, path):
        global rate
        global ipNum
        global protocolNum
        global ipPercent
        global protocolPercent
        global stop_send_Thread
        print('{}{}GOOD:{}{} Control receiving thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
        msg = await websocket.recv()
        print(msg)
        response = 'RCVD'
        if msg == 'stop':
            # response = 'stop signal received, closing now'
            stop_send_Thread = True
        elif msg == 'start':
            # response = 'start signal received, starting now'
            send_Data_Thread = Send_Data_Thread(threadID=1, name='send_Data_Thread', rate=rate, ipNum=ipNum,
                                                protocolNum=protocolNum, ipPercent=ipPercent,
                                                protocolPercent=protocolPercent)
            send_Data_Thread.start()
            print('{}{}GOOD:{}{} Data sending thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
            send_Data_Thread.join()
        elif msg.split('_')[0] == 'change':
            stop_send_Thread = True
            time.sleep(1) # wait for the thread ending
            msgs = msg.split('_')
            rate = int(msgs[0])
            ipNum = int(msgs[1])
            protocolNum = int(msgs[2])
            # ipPercent =
            send_Data_Thread = Send_Data_Thread(threadID=1, name='send_Data_Thread', rate=rate, ipNum=ipNum,
                                                protocolNum=protocolNum, ipPercent=ipPercent,
                                                protocolPercent=protocolPercent)
            send_Data_Thread.start()
            print('{}{}GOOD:{}{} Data sending thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
            send_Data_Thread.join()

        await websocket.send(response)

    start_server = websockets.serve(generator, "localhost", 12302) # port localhost:12302 is used to receive control signal

    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    # global variables for data generator
    stop_send_Thread = False
    rate = 5
    ipNum = 5
    protocolNum = 8
    ipPercent = []
    protocolPercent = []

    print(' Data Generator is starting '.center(100, '*'))
    # initialize classes
    send_Data_Thread = Send_Data_Thread(threadID=1, name='send_Data_Thread', rate=rate, ipNum=ipNum,
                                        protocolNum=protocolNum, ipPercent=ipPercent, protocolPercent=protocolPercent)
    # recv_Control_Thread = Recv_Control_Thread(threadID=2, name='recv_Control_Thread')
    # start threads
    send_Data_Thread.start()
    print('{}{}GOOD:{}{} Data sending thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
    # recv_Control_Thread.start()
    # print('{}{}GOOD:{}{} Control receiving thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
    # wait till threads terminate
    send_Data_Thread.join()
    # recv_Control_Thread.join()

    recv_control()
