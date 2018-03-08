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

  ##
  # generate the data structure
  #
  # dict of {study_no: {subset1: {sam_codes: [123, 456, ...], samples: {samplesdict}, ...}}
  # 

  # get a unique list (set) of all the study_no's

  study_nos = set(samples['study_no'])

  # get a unique list (set) of all the sam_code's

  sam_codes = set(samples['sam_code'])

  # get a unique set of tuples of (sam_code, study_no)

  sam_codes_bystudy = set(zip(samples['sam_code'], samples['study_no']))
  
  # generate the initial data structure of {study_no_1: {}, study_no_2: {}...}

  samples_bystudy = {study_no: {} for study_no in study_nos}

  ##
  # check if some sam_codes should be separated from bulk of study data
  #

  # if there is a config setting for separating a group of sam_codes
  # from the rest of the study data, make sure we do this
  # if not, use the key 'all_relevant'

  subset_structure = {
    'sam_codes': [],
    'samples': {field_name: [] for field_name in samples.keys()},
  }

  # set up lookup for sam_code to point to relevant samples array 
  sam_codes_lookup = {}

  ##
  # for each study we need to build a target data structure and
  # lookups for quickly assigning samples to sam_code/study_no 
  # locations
  #

  for study_no in study_nos:

    this_study_sam_codes = [study_code for study_code in sam_codes_bystudy if study_code[1] == study_no]  

    # see if we can get any subset structure specified in the config
    try:
      # add any subsetted sam_code/study_no combinations to sam_codes_lookup
      separate_sam_codes = json.loads(config['study_subsets'][study_no])
      
      for subset_name, subset_codes in separate_sam_codes.items():
        samples_bystudy[study_no][subset_name] = deepcopy(subset_structure)

        for subset_code in subset_codes:
          samples_bystudy[study_no][subset_name]['sam_codes'].append(subset_code)
          sam_codes_lookup[(subset_code, study_no)] = samples_bystudy[study_no][subset_name]['samples']

    except (KeyError, TypeError) as e:
      samples_bystudy[study_no]['all_relevant'] = deepcopy(subset_structure)

    # now add any unused sam_code/study_no combinations to sam_codes_lookup
    # which point to the 'all_relevant' (i.e. default) subset

    samples_bystudy[study_no]['all_relevant'] = deepcopy(subset_structure)

    for sam_code_study in this_study_sam_codes:
      if sam_code_study not in sam_codes_lookup.keys():
        samples_bystudy[study_no]['all_relevant']['sam_codes'].append(sam_code_study[0])
        sam_codes_lookup[sam_code_study] = samples_bystudy[study_no]['all_relevant']['samples']

  ##
  # split the samples 
  #

  for index, sam_code in enumerate(samples['sam_code']):

    study_no = samples['study_no'][index]

    for key in samples.keys():
      if key not in sam_codes_lookup[(sam_code, study_no)].keys():
        sam_codes_lookup[(sam_code, study_no)][key] = []
      sam_codes_lookup[(sam_code, study_no)][key].append(samples[key][index])

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
    
    # process the csv into dict of {field_name_1: [data1, data2, etc], ...}
    samples = processCsv(input_filepath)

    # split the dataset into dict 
    # of {study_no_1: {field_name_1: [data1, etc], ...}, study_no_2: {...}}
    samples = splitByStudy(samples)
    summary = {}

    # generate summary stats by study
    for study_no, sampleset in samples.items():
      for subset_name, subset_data in sampleset.items():
        if len(subset_data['samples']['sam_code']) > 0:
          summary['{study_no}_{subset_name}'.format(study_no=study_no, subset_name=subset_name)] = summaryStats(subset_data['samples'])
        else:
          summary['{study_no}_{subset_name}'.format(study_no=study_no, subset_name=subset_name)] = 0

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

    # generate some graphs of mean value over time
    try:
      create_js.createLoadDataJs(output_dir, '^all\-studies\-sample\-processing\-time.*?\.json$', 'load_data.js.template', './')
      print('summary_stats: created load_data.js'
    except:
      print('summary_stats: error creating load_data.js')
      exit(1)

    exit(0)
  else:
    print('--input argument missing: please specify a csv file to process')
    exit(1)





