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
#from utils.adc_dataprotocol import dataprotocol
from utils.zerounit import *
from datetime import datetime
from utils.zerounitthread import zerounitthread

if __name__ == "__main__":
  
    parser = argparse.ArgumentParser(description='ADC sample collection client for Blixt Zero')
    parser.add_argument('address', metavar='ADDRESS', type=str,
                        help='IP (or broadcast) address in IP:Port format')
    parser.add_argument('-output', '--output', metavar='OUTPUT', default=None, type=str, help='Output CSV file')

    args = parser.parse_args()
    config = vars(args)
    
    url = urllib.parse.urlsplit('//' + config['address'])
    config['remote_addr'] = url.hostname
    config['remote_port'] = url.port

    #TODO::Read ip-list from arguments
    urls = []
    urls.append('192.168.78.201')
    #urls.append('192.168.78.202')

    zerounits = {}
    
    try:
        for url in urls:
            zero_info = zerounitinfo()
            zero_info.remote_addr = url
            zerounits[url] = zerounitthread(zero_info)

        while True:
            time.sleep(1)
    except KeyboardInterrupt as e:
        print("Stopping...")

    for zu in zerounits.values():
        zu.info.stop_thread = True
        
    time.sleep(2)
    print('All stopped')
