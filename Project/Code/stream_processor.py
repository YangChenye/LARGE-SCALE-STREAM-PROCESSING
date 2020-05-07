# Created by Chenye Yang on 2020/5/1.
# Copyright © 2020 Chenye Yang. All rights reserved.

import socket
import time
import threading
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import *

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
class Stream_Processor_Thread(threading.Thread):
    def __init__(self, threadID, name, H, T, k, X):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.H = H # threshold percentage
        self.T = T # time duration
        self.k = k # top k results
        self.X = X # multiplier of standard deviation

    def change_parameter(self, H, T, k, X):
        self.H = H
        self.T = T
        self.k = k
        self.X = X

    # List protocols that are consuming more than H percent of the total external
    # bandwidth over the last T time units
    def function1(self):
        # with watermark, we can handle the late data properly. Discard very late data and keep not very late data.
        # with window size = T seconds slide = 1 second, we group the log in T seconds by "datetime"
        # with groupBy, we group the DataFrame using the specified columns, so we can run aggregation on them.
        # with agg, we aggregate the items in window and "ip", the result should be the sum of "bytes"
        # with orderBy, we get new DataFrame sorted by the specified column(s) by timestamp ascending and total bytes descending
        # with show, we print 20 sorted items without truncating strings longer than 20 chars
        # CONTENT IN log_windowed:
        # columns in log_windowed are ['window', 'protocol', 'sum(packet_size)']
        # data types are [('window', 'struct<start:timestamp,end:timestamp>'), ('ip', 'string'), ('sum(bytes)', 'bigint')]

        log_windowed = self.log_dataframe \
            .withWatermark('datetime', '1 minute') \
            .groupBy(window(self.log_dataframe.datetime, '{} seconds'.format(self.T), '1 second'),
                     self.log_dataframe.protocol) \
            .agg(sum('packet_size')) \
            .orderBy(['window.start', 'sum(packet_size)'], ascending=[True, False])
        print('All the windowed data')
        log_windowed.show(20, truncate=False)

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

    # override run() in Thread. When start() is called, run() is called.
    def run(self) -> None:
        self.conf = pyspark.SparkConf().setAppName('Project').setMaster('local[*]')  # set the configuration
        self.sc = pyspark.SparkContext(conf=self.conf)  # creat a spark context object
        self.ssc = StreamingContext(self.sc, self.T)  # take all data received in T second
        self.log_lines = self.ssc.socketTextStream('localhost', 12301)
        # (datetime, protocol, source IP, destination IP, packet size)
        self.log_tuples = self.log_lines.map(lambda x: (
            datetime.strptime(x.split(' ')[0], '%Y-%m-%d_%H:%M:%S.%f'), x.split(' ')[1], x.split(' ')[2],
            x.split(' ')[3], int(x.split(' ')[4])))
        # use Spark Structured Streaming to ensure the deal with log data in even time,
        # rather than received time, i.e. deal with the late data
        # create a spark session
        self.spark = SparkSession \
            .builder \
            .appName('project') \
            .getOrCreate()
        # set the schema of data frame
        self.schema = StructType([
            StructField('datetime', TimestampType(), True),
            StructField('protocol', StringType(), True),
            StructField('source_ip', StringType(), True),
            StructField('destination_ip', StringType(), True),
            StructField('packet_size', IntegerType(), True)])
        # creat data frame with RDDs and schema
        # columns in log_dataframe are ['datetime', 'protocol', 'source_ip', 'destination_ip', 'packet_size']
        self.log_dataframe = self.spark.createDataFrame(self.log_tuples, self.schema)
        self.function1()
        self.log_tuples.pprint()
        self.ssc.start()
        self.ssc.awaitTermination()

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

    # override run() in Thread. When start() is called, run() is called.
    def run(self) -> None:
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

    # override run() in Thread. When start() is called, run() is called.
    def run(self) -> None:
        self.recv_data()

if __name__ == "__main__":
    '''
    First attempt: 
    data_generator: socket client, sending data
    stream processor: socket server, receiving data
    '''
    # # global variables
    # stop_receive_Thread = False
    # print('Stream Processor is starting'.center(100, '*'))
    # # initialize classes
    # recv_Data_Thread = Recv_Data_Thread(threadID=1, name='recv_Data_Thread')
    # recv_Control_Thread = Recv_Control_Thread(threadID=2, name='recv_Control_Thread')
    # # start threads
    # recv_Data_Thread.start()
    # print('{}{}GOOD:{}{} Data receiving thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
    # recv_Control_Thread.start()
    # print('{}{}GOOD:{}{} Control receiving thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
    # # wait till threads terminate
    # recv_Data_Thread.join()
    # recv_Control_Thread.join()
    '''
    Second attempt: 
    data_generator: socket server, wait for connection from spark streaming
    stream processor: spark streaming, initiative connect to socket server
    '''
    stream_Processor_Thread = Stream_Processor_Thread(threadID=3, name='stream_Processor_Thread', H=0.2, T=10, k=3, X=2)
    stream_Processor_Thread.start()
    print('{}{}GOOD:{}{} Stream processor thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
    stream_Processor_Thread.join()
