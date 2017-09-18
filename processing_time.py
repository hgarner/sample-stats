import os
import sys
import csv
import re
from statistics import mean, stdev
import configparser
import argparse
from copy import deepcopy
from pprint import pprint
from datetime import timedelta, datetime
from numpy import average, percentile
import json

def processCsv(csv_filename):
  try:
    csv_file = open(csv_filename, 'r')

    samples = {}

    with csv_file:
      reader = csv.DictReader(csv_file, restkey = 'additional_data', delimiter = '\t')
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
    exit(1)
  
def summaryStats(samples):
  ##
  # first do processing times
  ##
  try:
    raw_proc_times = deepcopy(samples['processing_time'])
  except KeyError:
    print('summaryStats: Error, unable to find processing_time key')
    exit(1)
  
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
        'units': 'seconds',
        'mean': proc_avg,
        '95_cent': proc_95_cent
      }
    }

def loadConfig(config_filename = '', config = None):
  root_dir = os.getcwd()
  if config is None:
    config = configparser.ConfigParser()
  config.read(os.path.join(root_dir, config_filename))
  return config

if __name__ == '__main__':
  global config 
  path_to_script = os.path.abspath(sys.argv[0])
  root_dir = os.path.dirname(os.path.dirname(path_to_script))

  parser = argparse.ArgumentParser(description='Generate summary stats from sample processing times etc')
  parser.add_argument('--input', dest='input_filename', action='store', help='input csv filename')
  parser.add_argument('--outdir', dest='output_dir', action='store', help='output directory')
  parser.add_argument('--config', dest='config_filename', action='store', help='optional config .ini file')
  global args
  args = parser.parse_args()
  try:
    config = load_config(args.config_filename, config)
  except:
    pass

  if args.input_filename is not None:
    input_filepath = os.path.abspath(args.input_filename)
    input_filename = os.path.split(os.path.abspath(input_filepath))[1]
    samples = processCsv(input_filepath)
    summary = summaryStats(samples)
    summary['input_filename'] = input_filename

    # dump the data to a file

    now = datetime.now()
    datestamp = '{now.day:0>2}{now.month:0>2}{now.year}'.format(now = now) 

    output_filepath = re.sub('\.csv$', '_summarystats.{date}.json'.format(date = datestamp), input_filename)

    if args.output_dir is not None:
      output_dir = args.output_dir
    else:
      output_dir = os.path.split(input_filepath)[0]

    with open(os.path.join(output_dir, output_filepath), 'w') as output:
      json.dump(summary, output, indent=2)

    print('summary_stats: successfully output to {output_filepath}'.format(output_filepath = output_filepath))
    exit(0)

  else:
    print('--input argument missing: please specify a csv file to process')
    exit(1)





