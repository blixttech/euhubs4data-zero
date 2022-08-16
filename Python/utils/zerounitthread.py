import csv
import io
import os
import asyncio
import cbor2
import re
import time
import threading
import multiprocessing
from datetime import datetime
from utils.zerosampleblock import *
from utils.zerounit import *

class zerounitthread:
    def __init__(self, zero_info):
        lastipbyte = zero_info.getlastipbyte()
        if lastipbyte > 0:
            print('Starting thread for ' + zero_info.remote_addr) 
            self.info = zero_info
            thread = threading.Thread(target = _zeroThread, args=(self.info,))
            thread.start()
            csvProcess = multiprocessing.Process(target = _saveToCsv, args = (self.info.queue, self.info.remote_addr), daemon = True)
            csvProcess.start()
        else:
            print('No thread started for ' + zero_info.remote_addr)
        
def _zeroThread(zero_info):
    zero_info.loop = asyncio.new_event_loop()
    asyncio.set_event_loop(zero_info.loop)
    zero_info.loop.run_until_complete(main_coro_t(zero_info))
    zero_info.loop.run_forever()

def _saveToCsv(queue, remote_addr):
    file = None
    fileStartTime = 0
    sampleblocks = []
    writeTimeout = 1
    newfiletimeout = 10
    stopProcess = False
    fileChars = 0
    fileIndex = 1
    startTimeStr = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    while not stopProcess:
        try:
            time.sleep(0.1)
            while True:
                rVal = queue.get(False)
                if isinstance(rVal, zerosampleblock):
                    if len(sampleblocks) < 10000:
                        sampleblocks.append(rVal)
                    else:
                        print('Blockbuffer full ' + remote_addr + ' discarding blocks...')
                if rVal is True:
                    stopProcess = True
                    print('Exiting csv process')
        except:
            pass
        if len(sampleblocks) > 0 and (sampleblocks[0].udphandletime < time.time() - writeTimeout or stopProcess):
            #Write to csv
            strr = ''
            try:
                for sampleblock in sampleblocks:
                    for z in sampleblock.samples:
                        strr += str(z.timestamp) + ';' + str(z.lgCurrent)+ ';'+ str(z.hgCurrent)+ ';'+ str(z.voltage) + '\n'
            except:
                strr = ''
            if len(strr) > 0:
                print('Adding ' + str(len(sampleblocks)) + ' blocks to csv ' + remote_addr)
                sampleblocks.clear()
                try:
                    if file is None:
                        fileName = 'IP' + remote_addr + 'D' + startTimeStr + 'P' + str(fileIndex)
                        file = open(fileName,'w',encoding='utf-8')
                        fileIndex += 1
                    if file is not None:
                        file.write(strr)
                        file.flush()
                        fileChars += len(strr)
                    else:
                        print('No CSV to write for ' + remote_addr)
                except:
                    print('Exception when opening or writing to file ' + remote_addr)
        if file is not None and fileChars > 10000000 or stopProcess:
            fileChars = 0
            file.close()
            os.rename(file.name, file.name + '.csv')
            file = None
            print('Finalizing part csv ' + remote_addr)
  
async def main_coro_t(zero_info):
    _, _ = await zero_info.loop.create_datagram_endpoint(
        lambda: zerounit(zero_info),
        local_addr=('0.0.0.0', zero_info.getlocalport()))
