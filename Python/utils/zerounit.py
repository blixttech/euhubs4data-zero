import sys
import socket
import math
import argparse
import urllib.parse
import csv
import io
import os
import asyncio
import cbor2
import re
import time
import multiprocessing
from datetime import datetime
from utils.zerosampleblock import *

class zerounitinfo:
    def __init__(self):
        self.remote_addr = ''
        self.stop_thread = False
        self.loop = None
        self.csvwritetimeout = 1
        self.olddatatimeout = 10
        self.newfiletimeout = 10
        self.queue = multiprocessing.Manager().Queue(1000)

    def getlocalport(self):
        lastipbyte = self.getlastipbyte()
        if lastipbyte > 0:
            return 50000 + lastipbyte
        else:
            return -1
        
    def getlastipbyte(self):
        retval = -1
        if len(self.remote_addr) > 0:
            octets = self.remote_addr.split('.')
            if len(octets) == 4:
                if (int(octets[3]) > 0 and int(octets[3]) < 255):
                    retval = int(octets[3])
                else:
                    print('Broadcast IP not allowed: ' + self.remote_addr)
            else:
                print('Wrong format on IP, should be x.x.x.x, is: ' + self.remote_addr)
        else:
            print('No IP!?!?')
        return retval

class zerounit:
    def __init__(self, info):
        self.transport = None
        self.info = info
        self.loop = info.loop
        self.synctime = True
        self.referencetime = time.time()
        self.started = False
        self.lastIndex = 0
        self.lastrectime = 0

    def connection_made(self, transport):
        self.transport = transport
        sock = transport.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        bfs = sock.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF)
        print('bufz ' + str(bfs))
        sock.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF, bfs*2)
        bfs = sock.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF)
        print('bufz ' + str(bfs))
        self._send_data_request()

    def datagram_received(self, data, addr):
        ipaddr = addr[0]
        if ipaddr == self.info.remote_addr:
            if self.synctime:
                self.synctime = False
                tt = parseudpdata(data, 0)
                self.referencetime = time.time() - tt.calcblocktime()
            else:
                bd = parseudpdata(data, self.referencetime)
                if self.lastIndex > 0 and bd.index > self.lastIndex + 1:
                    print('IP ' + ipaddr + ' Missing ' + str(bd.index - self.lastIndex) + ' blocks (UDP package lost), time since last rec ' + str(time.time()-self.lastrectime) + ' ct ' + str(time.time()))
                if bd.index <= self.lastIndex:
                    print('IP ' + ipaddr + ' block index reversed, previous ' + self.lastIndex + ' received ' + bd.index)
                self.lastIndex = bd.index
                
                #TODO::Add sanity check for the calculated timestamp, it should not be allowed to reverse, if so it should resync
                self.info.queue.put(bd)                
        else:
            print('wrong ip, received: ' + ipaddr + ' expected: ' + self.info.remote_addr)
        tt = time.time() - self.lastrectime
        if tt > 0.01:
            print('Slow rec ' + str(tt))
        self.lastrectime = time.time()

    def connection_lost(self, exc):
        pass

    def _send_data_request(self):
        if not self.info.stop_thread and self.info.getlastipbyte() > 0:
            if not self.started:
                print('Sending request to ' + self.info.remote_addr + ' from port ' + str(self.info.getlocalport()))
                self.started = True
            self.transport.sendto(bytearray([0x01]), (self.info.remote_addr, 5555))
            self.loop.call_later(1, self._send_data_request)
        else:
            if self.info.stop_thread:
                print('Requested stop UDP receive thread: ' + self.info.remote_addr)
                self.info.queue.put(True)
                time.sleep(0.1)
            else:
                print('No valid IP to send request to, aborting... ' + self.info.remote_addr)
            self.loop.stop()
