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
from datetime import datetime
from utils.zerosample import zerosample

class zerosampleblock:
    def __init__(self, refstarttime, cbor_data):
        self.refstarttime = refstarttime
        self.udphandletime = time.time()
        self.index = cbor_data[0]
        self.sample_interval = cbor_data[1]
        self.sample_time = cbor_data[2]
        self.channels = cbor_data[3]
        self.resolution = cbor_data[4]
        self.a_b_data = {}
        self.adc_data = {}
        self.samples = []
        
    def calcblocktime(self):
        blockSize = len(self.adc_data[0])
        adc_time = self.sample_interval
        sTime = adc_time * self.channels / 1000000000.0
        blockstarttime = self.index * blockSize * sTime
        retval = self.refstarttime + blockstarttime
        return retval

def parseudpdata(data, refstarttime):
    decoder = cbor2.CBORDecoder(io.BytesIO(data))
    cbor_data = decoder.decode()
    sample_block = None
    if (len(cbor_data) == 7):
        resolution = cbor_data[4]
        bytes_per_sample = math.ceil(resolution / 8)
        a_b_data_bytes = cbor_data[5]
        sample_bytes = cbor_data[6]
        if len(a_b_data_bytes) % 4 or len(sample_bytes) % bytes_per_sample:
            return None
        
        sample_block = zerosampleblock(refstarttime, cbor_data)

        for i in range(0, sample_block.channels):
            a_val = int(a_b_data_bytes[i*4]) + int(a_b_data_bytes[(i*4)+1] << 8)
            b_val = int(a_b_data_bytes[(i*4)+2]) + int(a_b_data_bytes[(i*4)+3] << 8)
            sample_block.a_b_data[i] = (a_val, b_val)
       
            sample_block.adc_data = {i: [] for i in range(0, sample_block.channels)}

            for i in range(0, len(sample_bytes), bytes_per_sample * sample_block.channels):
                for c in range(0, sample_block.channels):
                    value = 0
                    for b in range(0, bytes_per_sample):
                        value += (sample_bytes[i + (c * bytes_per_sample) + b] << (8 * b))
                    sample_block.adc_data[c].append(value)

        if (sample_block.channels > 0 and len(sample_block.a_b_data) == 3 and len(sample_block.adc_data) > 0):
            lca = sample_block.a_b_data[0][0]
            lcb = sample_block.a_b_data[0][1]
            hca = sample_block.a_b_data[1][0]
            hcb = sample_block.a_b_data[1][1]
            va = sample_block.a_b_data[2][0]
            vb = sample_block.a_b_data[2][1]

            #nrSamplesInBlock = len(sample_block.adc_data[0])
            #print('samplesinblock ' +str(nrSamplesInBlock))
            #adc_time = sample_block.sample_interval
            sTime = sample_block.sample_interval * sample_block.channels / 1000000000.0
            blockStartTime = sample_block.calcblocktime()
            sampleNr = 0
            for z in zip(*sample_block.adc_data.values()):
                #print('v: '+str(z))
                sampleTimeOffset = sampleNr * sTime
                sampleTime = blockStartTime + sampleTimeOffset
                samp = zerosample()
                samp.timestamp = sampleTime
                samp.blockNr = sample_block.index
                samp.sampleNr = sampleNr
                samp.adcTime = sample_block.sample_interval
                sampleNr += 1
                i = 0
                for val in z:
                    if (i == 0):
                        #low gain current
                        samp.lgCurrent = round((val - lcb) / lca, 3)
                    if (i == 1):
                        #high gain current
                        samp.hgCurrent = round((val - hcb) / hca, 3)
                    if (i == 2):
                        #Voltage
                        samp.voltage = round((val - vb) / va, 3)
                    i += 1
                sample_block.samples.append(samp)
    return sample_block
