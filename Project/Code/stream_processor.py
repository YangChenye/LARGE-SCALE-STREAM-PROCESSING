# Created by Chenye Yang on 2020/5/1.
# Copyright © 2020 Chenye Yang. All rights reserved.

import socket

if __name__ == "__main__":


    print("Server is starting")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', 12301))  # port localhost:12301 is used to receive generated data
    sock.listen(5)  # the max connection number, FIFO
    print("Server is listenting port 16889, with max connection 5")

    while True:  # wait for connection
        connection, address = sock.accept()
        try:
            connection.settimeout(50)
            # 获得一个连接，然后开始循环处理这个连接发送的信息
            '''
            如果server要同时处理多个连接，则下面的语句块应该用多线程来处理，
            否则server就始终在下面这个while语句块里被第一个连接所占用，
            无法去扫描其他新连接了，但多线程会影响代码结构，所以记得在连接数大于1时
            下面的语句要改为多线程即可。
            '''
            while True:
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
                    break # 退出连接监听循环
        except socket.timeout:  # 如果建立连接后，该连接在设定的时间内无数据发来，则time out
            print('time out')

        print("closing one connection")  # 当一个连接监听循环退出后，连接可以关掉
        connection.close()

