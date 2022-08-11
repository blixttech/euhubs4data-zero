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

class zerosample:
    def __init__(self):
        self.timestamp = 0
        self.blockNr = 0
        self.sampleNr = 0
        self.adcTime = 0
        self.lgCurrent = 0
        self.hgCurrent = 0
        self.voltage = 0
