import os
import sys
import csv
import re
from statistics import mean, stdev
import configparser
import argparse
from copy import deepcopy
from pprint import pprint
from datetime import timedelta
from numpy import average, percentile

def processCsv(csv_filename):
  try:
    csv_file = open(csv_filename, 'r')

    samples = {}

    with csv_file:
      reader = csv.DictReader(csv_file, restkey = 'additional_data', delimiter = ',')
      for line in reader:
        if line['processing_time'] not in ['NULL', 'null', '', None]:
          for key, val in line.items():
            try:
              samples[key].append(val)
            except KeyError:
              samples[key] = [val]

    return samples
  except:
    print('processCsv: Error processing csv file')
    #exit(1)
  
def summaryStats(samples):
  ##
  # first do processing times
  ##
  try:
    raw_proc_times = deepcopy(samples['processing_time'])
  except KeyError:
    print('summaryStats: Error, unable to find processing_time key')
    #exit(1)
  
  # convert to timedeltas
  proc_times = []
  for str_time in raw_proc_times:
    time = [int(t) for t in str_time.split(':')]
    proc_times.append((timedelta(hours = time[0], minutes = time[1], seconds = time[2])).total_seconds())

  proc_times.sort()

  ##
  # processing time avg
  ##
  proc_avg = average(proc_times)

  ##
  # 95th percentile
  ##
  proc_95_cent = percentile(proc_times, 95)

  return {
      'proc_times': {
        'mean': proc_avg,
        '95_cent': proc_95_cent
      }
    }






