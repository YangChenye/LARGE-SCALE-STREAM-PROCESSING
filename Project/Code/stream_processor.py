# Created by Chenye Yang on 2020/5/1.
# Copyright © 2020 Chenye Yang, Zhuoyue Xing. All rights reserved.

import socket
import time
import threading
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import *
import requests
import json
import asyncio
import websockets
from pyspark.sql.functions import regexp_extract
from functools import partial


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
        self.H = H  # threshold percentage
        self.T = T  # time duration
        self.k = k  # top k results
        self.X = X  # multiplier of standard deviation

    def change_parameter(self, H, T, k, X):
        self.H = H
        self.T = T
        self.k = k
        self.X = X

    # List protocols that are consuming more than H percent of the total external
    # bandwidth over the last T time units
    def function1(self):
        # '''Attempt to use structured streaming and watermark'''
        #
        # # with watermark, we can handle the late data properly. Discard very late data and keep not very late data.
        # # with window size = T seconds slide = 1 second, we group the log in T seconds by "datetime"
        # # with groupBy, we group the DataFrame using the specified columns, so we can run aggregation on them.
        # # with agg, we aggregate the items in window and "ip", the result should be the sum of "bytes"
        # # with orderBy, we get new DataFrame sorted by the specified column(s) by timestamp ascending and total bytes descending
        # # with show, we print 20 sorted items without truncating strings longer than 20 chars
        # # CONTENT IN log_windowed:
        # # columns in log_windowed are ['window', 'protocol', 'sum(packet_size)']
        # # data types are [('window', 'struct<start:timestamp,end:timestamp>'), ('ip', 'string'), ('sum(bytes)', 'bigint')]
        #
        # # log_windowed = self.log_dataframe \
        # #     .withWatermark('datetime', '1 minute') \
        # #     .groupBy(window(self.log_dataframe.datetime, '{} seconds'.format(self.T), '1 second'),
        # #              self.log_dataframe.protocol) \
        # #     .agg(sum('packet_size')) \
        # #     .orderBy(['window.start', 'sum(packet_size)'], ascending=[True, False])
        # # print('All the windowed data')
        # # log_windowed.show(20, truncate=False)
        #
        # '''Not using watermark'''
        # log_simple = self.log_tuples.map(lambda x: (x[1], x[4])) # ('protocol', packet_size)
        # log_agg = log_simple.reduceByKey(lambda x,y: x+y)
        # log_sum = log_agg.map(lambda x: ('str', x[1])).reduceByKey(lambda x,y: x+y)
        # # log_sum.saveAsTextFiles('./checkpoints/function1')
        # # newRdd = pyspark.SparkContext.textFile('./checkpoints/function1')
        # # log_reduced = log_agg.reduce(lambda x: x[1] > self.H)
        # # log_reduced = log_agg.updateStateByKey(update_func)
        # # log_reduced = log_agg.reduce()
        # print(
        #     'protocols that are consuming more than {} percent of the total external bandwidth over the last {} time units'.format(
        #         self.H, self.T))
        # # log_reduced.pprint()
        # # perform your operation
        # # note that you do not need a lambda expression for json.loads
        # jsonRDD = log_agg.map(json.loads)
        # # map jsons back to string, reduce to one big string with one json on each line
        # json_string = jsonRDD.map(json.dumps).reduce(lambda x, y: x + "\n" + y)
        # # write your string to a file
        # with open("./checkpoints/function1.json", "w") as f:
        #     f.write(json_string)
        # log_agg.pprint()


        log_windowed = self.log_tuples_df \
            .withWatermark('datetime', '1 minute') \
            .groupBy(window(self.log_tuples_df.datetime, '{} seconds'.format(self.T), '1 second'),
                     self.log_tuples_df.protocol) \
            .agg(sum('packet_size')) \
            .orderBy(['window.start', 'sum(packet_size)'], ascending=[True, False])
        return

    # List the top-k most resource intensive protocols over the last T time units
    def function2(self):
        # log_simple = self.log_tuples.map(lambda x: (x[1], x[4])) # ('protocol', packet_size)
        # log_agg = log_simple.reduceByKey(lambda x, y: x + y) # sum up the packet sizes
        # log_sorted = log_agg.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False)) # sort, top-k
        # print('the top-{} most resource intensive protocols over the last {} time units'.format(self.k, self.T))
        # log_sorted.pprint(num=self.k)
        # # log_top_k = log_sorted.take(self.k)
        # # print([' '.join(map(str, item)) for item in log_top_k])
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
        # self.conf = pyspark.SparkConf().setAppName('Project').setMaster('local[*]')  # set the configuration
        # self.sc = pyspark.SparkContext(conf=self.conf)  # creat a spark context object
        # self.sc.setLogLevel("ERROR")
        # self.ssc = StreamingContext(self.sc, self.T)  # take all data received in T second
        # # self.ssc.checkpoint('/Users/yangchenye/Downloads/spark_checkpoint')
        # self.log_lines = self.ssc.socketTextStream('localhost', 12301)
        # # (datetime, protocol, source IP, destination IP, packet size)
        # self.log_tuples = self.log_lines.map(lambda x: (
        #     datetime.strptime(x.split(' ')[0], '%Y-%m-%d_%H:%M:%S.%f'), x.split(' ')[1], x.split(' ')[2],
        #     x.split(' ')[3], int(x.split(' ')[4])))
        '''
        -------------------------------------------
        Time: 2020-05-07 13:15:50
        -------------------------------------------
        (datetime.datetime(2020, 5, 7, 13, 15, 40, 179311), 'SOAP', '53.215.218.189', '45.43.227.63', 4626)
        (datetime.datetime(2020, 5, 7, 13, 15, 40, 381757), 'UPnP', '45.43.227.63', '133.98.231.165', 2818)
        (datetime.datetime(2020, 5, 7, 13, 15, 40, 585995), 'SSDP', '53.215.218.189', '222.186.237.75', 3243)
        (datetime.datetime(2020, 5, 7, 13, 15, 40, 788869), 'DNS', '45.43.227.63', '45.43.227.63', 9320)
        (datetime.datetime(2020, 5, 7, 13, 15, 40, 992985), 'SSDP', '11.71.50.83', '11.71.50.83', 9013)
        (datetime.datetime(2020, 5, 7, 13, 15, 41, 194503), 'DNS', '222.186.237.75', '45.43.227.63', 915)
        (datetime.datetime(2020, 5, 7, 13, 15, 41, 395340), 'UPnP', '53.215.218.189', '133.98.231.165', 6200)
        (datetime.datetime(2020, 5, 7, 13, 15, 41, 599333), 'UPnP', '222.186.237.75', '45.43.227.63', 6927)
        (datetime.datetime(2020, 5, 7, 13, 15, 41, 803432), 'SOAP', '45.43.227.63', '133.98.231.165', 1834)
        (datetime.datetime(2020, 5, 7, 13, 15, 42, 7557), 'SSDP', '45.43.227.63', '45.43.227.63', 3442)
        ...
        '''
        '''Attempt to use structured streaming and watermark'''
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
        self.log_lines_df = self.spark.readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 12301) \
            .load()
        # print(self.log_lines_df.isStreaming())
        self.log_lines_df.printSchema()
            # root
            # | -- value: string(nullable=true)
        # TextSocketSource doesn't provide any integrated parsing options. It is only possible to use one of the two formats:
        # timestamp and text if includeTimestamp is set to true, with the following schema:
        #     StructType([
        #         StructField("value", StringType()),
        #         StructField("timestamp", TimestampType())
        #     ])
        # text only if includeTimestamp is set to false, with the schema as shown below:
        #     StructType([StructField("value", StringType())]))
        # If you want to change this format you 'll have to transform the stream to extract fields of interest,
        # for example with regular expressions:
        fields = partial(
            regexp_extract, str='value', pattern='^([0-9]*-[0-9]*-[0-9]*_[0-9]*:[0-9]*:[0-9]*.[0-9]*)\s*(\w*)\s*([0-9]*.[0-9]*.[0-9]*.[0-9]*)\s*([0-9]*.[0-9]*.[0-9]*.[0-9]*)\s*([0-9]*)$'
        )
        self.log_tuples_df = self.log_lines_df.select(
            fields(idx=1).alias('datetime'),
            fields(idx=2).alias('protocol'),
            fields(idx=3).alias('source_ip'),
            fields(idx=4).alias('destination_ip'),
            fields(idx=5).alias('packet_size')
        ) # split the string in Structured Streaming Dataframe
        self.log_tuples_df.printSchema()
            # root
            # | -- datetime: string(nullable=true)
            # | -- protocol: string(nullable=true)
            # | -- source_ip: string(nullable=true)
            # | -- destination_ip: string(nullable=true)
            # | -- packet_size: string(nullable=true)
        # change the data type of the dataframe
        self.log_tuples_df = self.log_tuples_df.withColumn('datetime', to_timestamp('datetime', 'yyyy-MM-dd_HH:mm:ss.SSS'))
        self.log_tuples_df = self.log_tuples_df.withColumn('packet_size', self.log_tuples_df['packet_size'].cast(IntegerType()))
        self.log_tuples_df.printSchema()
            # root
            # | -- datetime: timestamp(nullable=true)
            # | -- protocol: string(nullable=true)
            # | -- source_ip: string(nullable=true)
            # | -- destination_ip: string(nullable=true)
            # | -- packet_size: integer(nullable=true)
        self.query = self.log_tuples_df.writeStream.format('console').start().awaitTermination()

        # self.function1()

        # '''Not using watermark'''
        # # self.log_tuples.pprint()
        # self.function1()
        #
        # self.ssc.start()
        # self.ssc.awaitTermination()


# control receiving thread of stream processor
class Recv_Control_Thread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def recv_control(self):
        '''using socket to communicate with web ui'''
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 12303))  # port localhost:12303 is used to receive control signal
        sock.listen(5)  # the max connection number, FIFO
        print(
            '{}{}GOOD:{}{} Connection complete. Stream Processor is listening control signal, from port 12303.'.format(
                Color.GREEN, Color.BOLD, Color.END, Color.END))
        while True:  # wait for connection
            connection, address = sock.accept()
            control = connection.recv(1024).decode("utf-8")
            if control == 'stop':
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


# control receiving thread of stream processor
def recv_control():
    '''using websocket to communicate with web ui'''

    async def generator(websocket, path):
        global H
        global T
        global k
        global X
        global stop_receive_Thread
        print('{}{}GOOD:{}{} Control receiving thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
        msg = await websocket.recv()
        print(msg)
        response = 'RCVD'
        if msg == 'stop':
            # response = 'stop signal received, closing now'
            stop_receive_Thread = True
        elif msg == 'start':
            # response = 'start signal received, starting now'
            stream_Processor_Thread = Stream_Processor_Thread(threadID=3, name='stream_Processor_Thread', H=H, T=T, k=k,
                                                              X=X)
            stream_Processor_Thread.start()
            print('{}{}GOOD:{}{} Stream processor thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
            stream_Processor_Thread.join()
        elif msg.split('_')[0] == 'change':
            stop_receive_Thread = True
            time.sleep(0.5)  # wait for the thread ending
            msgs = msg.split('_')
            H, T, k, X = float(msgs[1]), int(msgs[2]), int(msgs[3]), float(msgs[4])
            stream_Processor_Thread = Stream_Processor_Thread(threadID=3, name='stream_Processor_Thread', H=H, T=T, k=k,
                                                              X=X)
            stream_Processor_Thread.start()
            print('{}{}GOOD:{}{} Stream processor thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
            stream_Processor_Thread.join()

    start_server = websockets.serve(generator, "localhost",
                                    12303)  # port localhost:12303 is used to receive control signal

    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()


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
                        stop_receive_Thread = False  # reset this Flag so that another send_Thread can start
                        connection.close()  # close socket connection
                        sock.close()  # close socket before exit
                        return  # then this thread is terminated
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


# url here is the url that flask is running on(http://127.0.0.1:5000/) add "6889final/insert"
def data_sender(sdata, url="http://127.0.0.1:5000/6889final/insert"):
    headers = {
        "Content-Type": "application/json; charset=UTF-8"
    }
    response = requests.post(url, data=json.dumps(sdata), headers=headers).text
    # If data is json type,change the above line to
    # response = requests.post(url, data=sdata, headers=headers).text
    print(response)


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
    H = 0.2
    T = 5
    k = 3
    X = 2
    stop_receive_Thread = False
    stream_Processor_Thread = Stream_Processor_Thread(threadID=3, name='stream_Processor_Thread', H=H, T=T, k=k, X=X)
    stream_Processor_Thread.start()
    print('{}{}GOOD:{}{} Stream processor thread started'.format(Color.GREEN, Color.BOLD, Color.END, Color.END))
    stream_Processor_Thread.join()

    recv_control()
