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
  # check if any samples to ignore
  #

  ignore_indexes = []
  ignore = {}

  try:
    ignore_sam_codes = config['exclude']['ignore_sam_codes'].split(',')
    ignore['sam_codes'] = ignore_sam_codes
    
    for index, sam_code in enumerate(samples['sam_code']):
      if sam_code in ignore_sam_codes:
        ignore_indexes.append(index)
  except (KeyError, TypeError):
    pass

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
  for index, str_time in enumerate(raw_proc_times):
    if index not in ignore_indexes:
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
        '95_cent': proc_95_cent,
        'exclude': ignore
      }
    }

# split sample set by study_no col
# return dict of {study_no: [sample1, sample2, ...], ...}
def splitByStudy(samples):
  if 'study_no' not in samples.keys():
    raise KeyError('study_no must be in samples data')

  samples_bystudy = {}

  for index, study_no in enumerate(samples['study_no']):
    if study_no not in samples_bystudy.keys():
      samples_bystudy[study_no] = {}
    for key in samples.keys():
      if key not in samples_bystudy[study_no].keys():
        samples_bystudy[study_no][key] = []
      samples_bystudy[study_no][key].append(samples[key][index])

  return samples_bystudy

def load_config(config_filename = '', config = None):
  root_dir = os.getcwd()
  if config is None:
    config = configparser.ConfigParser()
  config.read(os.path.join(root_dir, config_filename))
  return config

if __name__ == '__main__':
  path_to_script = os.path.abspath(sys.argv[0])
  root_dir = os.path.dirname(os.path.dirname(path_to_script))

  parser = argparse.ArgumentParser(description='Generate summary stats from sample processing times etc')
  parser.add_argument('--input', dest='input_filename', action='store', help='input csv filename')
  parser.add_argument('--outdir', dest='output_dir', action='store', help='output directory')
  parser.add_argument('--config', dest='config_filename', action='store', help='optional config .ini file')
  global args
  args = parser.parse_args()
  global config 
  config = None
  try:
    config = load_config(args.config_filename, config)
  except:
    pass

  if args.input_filename is not None:
    input_filepath = os.path.abspath(args.input_filename)
    input_filename = os.path.split(os.path.abspath(input_filepath))[1]
    samples = processCsv(input_filepath)
    samples = splitByStudy(samples)
    summary = {}
    for study_no, sampleset in samples.items():
      summary[study_no] = summaryStats(samples[study_no])

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





