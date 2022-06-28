import sys
import socket
import math
import logging
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

logger = logging.getLogger(__name__)

class NewLineFormatter(logging.Formatter):
    def __init__(self, fmt, datefmt=None):
        logging.Formatter.__init__(self, fmt, datefmt)

    def escape_ansi(self, line):
        ansi_escape = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
        return ansi_escape.sub('', line)

    def format(self, record):
        msg = logging.Formatter.format(self, record)
        if record.message != "":
            parts = msg.split(record.message)
            msg = msg.replace('\n', '\n' + parts[0])
        msg = self.escape_ansi(msg)
        return msg

def setup_logger(config):
    date_format = '%d-%m-%Y %H:%M:%S'
    log_format = '%(asctime)s %(levelname)-8s %(message)s'
    formatter = NewLineFormatter(log_format, datefmt=date_format)

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(config['log_level'])

class DataProtocol:

    class SampleBlock:
        def __init__(self):
            self.index = 0
            self.sample_interval = 0
            self.sample_time = 0
            self.channels = 0
            self.resolution = 0
            self.adcCount = 0
            self.a_b_data = {}
            self.adc_data = {}
            self.timeStamp = time.time()

    class StatsItem:
        def __init__(self):
            self.blocks = 0
            self.samples = 0
            self.missed = False

    class CsvWriters:
        def __init__(self):
            self.enableBroadcast = False
            self.writers = {}
        
    class CsvWriter:
        def __init__(self, fileName):
            self.name = fileName
            self.file = open(self.name, 'w', encoding='utf-8')
            self.writer = csv.writer(self.file, delimiter = ';')
            self.lastBlock = -1
            self.droppedPackages = 0
            self.blockSize = 0
            self.blockCounter = 0
            self.lastBlockTime = time.time()
            self.startTime = time.time()

    def __init__(self, config, csv_writers, loop):
        self.config = config
        self.loop = loop
        self.transport = None
        self.stats = {}
        self.prev_lines = 0
        self.csv_writers = csv_writers

    def connection_made(self, transport):
        self.transport = transport
        sock = transport.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._send_data_request()
        self._print_stats()

    def datagram_received(self, data, addr):
        sample_block = self._get_sample_block(data)
        ipaddr = addr[0]
        if ipaddr not in self.stats:
            item = DataProtocol.StatsItem()
            self.stats[ipaddr] = item
        else:
            item = self.stats[ipaddr]

        item.blocks += 1
        item.samples += len(sample_block.adc_data[0])

        self._write_sample_block(sample_block, ipaddr)

    def connection_lost(self, exc):
        pass

    def _send_data_request(self):
        logger.debug("Sending request")
        self.transport.sendto(bytearray([0x01]),
                              (self.config.get('remote_addr'),
                               self.config.get('remote_port')))
        self.loop.call_later(1, self._send_data_request)

    def _print_stats(self):
        for _ in range(self.prev_lines):
            sys.stdout.write('\033[A')
        sys.stdout.write('\r')
        for ipaddr, item in self.stats.items():
            sys.stdout.write('%s => %d\n' % (ipaddr, item.samples))

        self.prev_lines = len(self.stats)
        self.loop.call_later(0.1, self._print_stats)

    def _get_sample_block(self, data):
        sample_block = DataProtocol.SampleBlock()
        decoder = cbor2.CBORDecoder(io.BytesIO(data))
        cbor_data = decoder.decode()

        sample_block.timeStamp = time.time() #TODO::This should be included in, and read from, the cbor data
        sample_block.index = cbor_data[0]
        sample_block.sample_interval = cbor_data[1]
        sample_block.sample_time = cbor_data[2]
        sample_block.channels = cbor_data[3]
        sample_block.resolution = cbor_data[4]
        bytes_per_sample = math.ceil(sample_block.resolution / 8)

        sample_bytes = None
        if (len(cbor_data) == 6):
            sample_bytes = cbor_data[5]
        if (len(cbor_data) == 7):
            a_b_data_bytes = cbor_data[5]
            if len(a_b_data_bytes) % 4:
                logger.error("Invalid a and b values %d", len(a_b_data_bytes))
                return None

            for i in range(0, sample_block.channels):
                a_val = int(a_b_data_bytes[i*4]) + int(a_b_data_bytes[(i*4)+1] << 8)
                b_val = int(a_b_data_bytes[(i*4)+2]) + int(a_b_data_bytes[(i*4)+3] << 8)
                sample_block.a_b_data[i] = (a_val, b_val)
            sample_bytes = cbor_data[6]
        if (sample_bytes == None):
            logger.error("Invalid cbor data length")
            return None
        
        adc_data = {i: [] for i in range(0, sample_block.channels)}
        if len(sample_bytes) % bytes_per_sample:
            logger.error("Invalid samples: samples %d, bytes/sample %d",
                         len(sample_bytes), bytes_per_sample)
            return None
        adcCount = 0;
        for i in range(0, len(sample_bytes), bytes_per_sample * sample_block.channels):
            for c in range(0, sample_block.channels):
                value = 0
                for b in range(0, bytes_per_sample):
                    value += (sample_bytes[i + (c * bytes_per_sample) + b] << (8 * b))
                adc_data[c].append(value)
                adcCount += 1

        sample_block.adc_data = adc_data
        sample_block.adcCount = adcCount

        return sample_block

    def _write_sample_block(self, sample_block, ipaddr):
        if (self.csv_writers.enableBroadcast is True and self.csv_writers.writers.get(ipaddr) is None):
            fileName = 'IP'+ ipaddr + 'D'+datetime.now().strftime("%Y-%m-%dT%H-%M-%S.csv")
            print('Creating file: ' + fileName)
            self.csv_writers.writers[ipaddr] = DataProtocol.CsvWriter(fileName)
        csv_writer = self.csv_writers.writers.get(ipaddr)

        if (csv_writer is not None and sample_block.channels > 0):
            blockIndex = sample_block.index
            if (csv_writer.lastBlock >= 0): #Check if this was the very first message or not
                blockJump = blockIndex - csv_writer.lastBlock
            else:
                blockJump = 1
                
            if (blockJump > 1):
                lastBlock = csv_writer.lastBlock
                packages = blockIndex - lastBlock
                csv_writer.droppedPackages += packages
                print(csv_writer.name + ' Dropped ' + str(packages) + ' UDP package(s) at blockindex ' +
                      str(lastBlock) + ' total dropped ' + str(csv_writer.droppedPackages) + '\n')
            if (blockJump <= 0):
                print('Block index reverse jump, SSCB restarted or overflow in block counter\n')
                csv_writer.blockCounter += 10000 #At 10kHz: 10000 -> 1s
            else:
                csv_writer.blockCounter += blockJump
            
            blockSize = sample_block.adcCount / sample_block.channels

            if (len(sample_block.a_b_data)==3):
                lca = sample_block.a_b_data[0][0]
                lcb = sample_block.a_b_data[0][1]
                hca = sample_block.a_b_data[1][0]
                hcb = sample_block.a_b_data[1][1]
                va = sample_block.a_b_data[2][0]
                vb = sample_block.a_b_data[2][1]
            else:
                lca = 1650
                lcb = 32768
                hca = 17187
                hcb = 32768
                va = 61
                vb = 32768
                
            row_row = []
            adcTime = sample_block.sample_interval
            sTime = adcTime * sample_block.channels / 1000000000.0
            sampleNr = 0
            for z in zip(*sample_block.adc_data.values()):
                sampleTimeOffset = (blockSize - sampleNr) * sTime
                sampleTime = sample_block.timeStamp - sampleTimeOffset
                row = []
                row.append(sampleTime)
                row.append(sample_block.index)
                row.append(sampleNr)
                row.append(adcTime)
                sampleNr += 1
                i = 0
                for val in z:
                    if (i == 0):
                        #low gain current
                        row.append(round((val - lcb) / lca, 3))
                    if (i == 1):
                        #high gain current
                        row.append(round((val - hcb) / hca, 3))
                    if (i == 2):
                        #Voltage
                        row.append(round((val - vb) / va, 3))
                    i += 1
                row_row.append(row)
            if (sample_block.timeStamp - blockSize*sTime > csv_writer.lastBlockTime):
                csv_writer.writer.writerows(row_row)
            else:
                print('Time error!?!?!')
            
            csv_writer.lastBlock = sample_block.index
            csv_writer.lastBlockTime = sample_block.timeStamp

async def main_coro(config, csv_writers, loop):
    _, _ = await loop.create_datagram_endpoint(
        lambda: DataProtocol(config, csv_writers, loop),
        local_addr=('0.0.0.0', 56789))

if __name__ == "__main__":
    def validate_log_level(arg):
        arg = str(arg).lower()
        level = {"info": logging.INFO, "warn": logging.WARN, "debug": logging.DEBUG,
                 "i": logging.INFO, "w": logging.WARN, "d": logging.DEBUG}
        if arg not in level:
            raise argparse.ArgumentTypeError("log level should be (i)nfo, (w)arn or (d)ebug")
        return level[arg]
    
    parser = argparse.ArgumentParser(description='ADC sample collection client for Blixt Zero')
    parser.add_argument('address', metavar='ADDRESS', type=str,
                        help='IP (or broadcast) address in IP:Port format')
    parser.add_argument('-output', '--output', metavar='OUTPUT', default=None, type=str, help='Output CSV file')
    parser.add_argument('-l', '--log-level', type=validate_log_level, default="info",
                        help='Set log level: info, warn, debug')

    args = parser.parse_args()
    config = vars(args)
    
    url = urllib.parse.urlsplit('//' + config['address'])
    config['remote_addr'] = url.hostname
    config['remote_port'] = url.port

    csv_writers = DataProtocol.CsvWriters()
    if (config['output'] is not None):
        print('Saving all data to: ' + config['output'] + ' from IP ' + url.hostname)
        csv_writers.writers[url.hostname] = DataProtocol.CsvWriter(config['output'])
    else:
        print('Auto generating filename based on IP -  Works for multiple sensors')
        csv_writers.enableBroadcast = True

    loop = asyncio.new_event_loop()
    
    try:
        loop.run_until_complete(main_coro(config, csv_writers, loop))
        loop.run_forever()
    except KeyboardInterrupt as e:
        print("Stopping...")
    finally:
        loop.stop()

    for cf in csv_writers.writers.values():
        cf.file.close()
        print('Closed file: ' + cf.name + ' with size: ' + str(round(os.path.getsize(cf.name)/1000000,2)) + ' megabytes')
