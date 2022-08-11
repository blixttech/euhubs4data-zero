import csv
import io
import os
import asyncio
import cbor2
import re
import time
import threading
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
            csvThread = threading.Thread(target = _saveToCsv, args = (self.info,))
            csvThread.start()
        else:
            print('No thread started for ' + zero_info.remote_addr)
        
def _zeroThread(zero_info):
    #print('in ' + zero_info.remote_addr)
    zero_info.loop = asyncio.new_event_loop()
    asyncio.set_event_loop(zero_info.loop)
    zero_info.loop.run_until_complete(main_coro_t(zero_info))
    zero_info.loop.run_forever()

def _saveToCsv(zero_info):
    file = None #open("test", 'w', encoding='utf-8')
    fileStartTime = 0
    lastBlockNr = 0
    while not zero_info.stop_thread:
        time.sleep(0.1)
        currentTime = time.time()
        nrOfBlocks = len(zero_info.sampleblocks)
        if nrOfBlocks > 0 and zero_info.sampleblocks[0].udphandletime < currentTime - zero_info.csvwritetimeout:
            #Write to csv
            print('Writing ' + str(nrOfBlocks) + ' blocks to csv ' + zero_info.remote_addr)
            if file is None:
                fileName = 'IP'+ zero_info.remote_addr + 'D'+datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
                file = open(fileName,'w',encoding='utf-8')
                fileStartTime = zero_info.sampleblocks[0].udphandletime

            handleTime = 0
            bIndex = -1
            strr = ''
            while bIndex < nrOfBlocks:
                bIndex += 1
                sampleblock = zero_info.sampleblocks[bIndex]
                handleTime = sampleblock.udphandletime
                #if lastBlockNr > 0 and sampleblock.index > lastBlockNr + 1:
                #    print('IP ' + zero_info.remote_addr + ' Missing ' + str(sampleblock.index - lastBlockNr) + ' blocks')
                #if sampleblock.index <= lastBlockNr:
                #    print('IP ' + zero_info.remote_addr + ' reversing block nr')
                lastBlockNr = sampleblock.index
                for z in sampleblock.samples:
                    #strr += str(z.timestamp) + ';' + str(z.blockNr) + ';'+ str(z.sampleNr) + ';'+ str(z.adcTime)+ ';'+ str(z.lgCurrent)+ ';'+ str(z.hgCurrent)+ ';'+ str(z.voltage) + '\n'
                    strr += str(z.timestamp) + ';' + str(z.lgCurrent)+ ';'+ str(z.hgCurrent)+ ';'+ str(z.voltage) + '\n'
            del zero_info.sampleblocks[0:bIndex + 1]
            file.write(strr)
            file.flush()
        if file is not None and fileStartTime + zero_info.newfiletimeout < time.time():
            print('Closing file ')
            file.close()
            os.rename(file.name, file.name + '.csv')
            file = None
    if file is not None:
        file.close()
        os.rename(file.name, file.name + '.csv')
        file = None


async def main_coro_t(zero_info):
    _, _ = await zero_info.loop.create_datagram_endpoint(
        lambda: zerounit(zero_info),
        local_addr=('0.0.0.0', zero_info.getlocalport()))
