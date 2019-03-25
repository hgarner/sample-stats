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
import create_js
from shutil import copy
import pandas as pd
import numpy as np

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
  except Exception as e:
    print(e)
    print('processCsv: Error processing csv file')
    exit(1)

# generate pivot table of summary stats from dict of samples
# @param dict samples dict of samples data with keys inc 'sample_id',
# 'time_taken', etc. each key is a list of sample data
# @param int target_time in minutes for sample processing
# @return DataFrame processing time stats by month/yr for samples
def pivotSummaryStats(samples, target_time = 90):
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
    samples['raw_proc_times'] = raw_proc_times
  except KeyError:
    print('summaryStats: Error, unable to find processing_time key')
    exit(1)

  ##
  # create a pandas dataframe from the dataset
  ##
  samples_frame = pd.DataFrame(samples)

  # drop the ignore_indexes (sam_codes to be excluded) from the df
  samples_frame = samples_frame.drop(ignore_indexes)

  # convert raw_proc_times to total seconds
  samples_frame['raw_proc_times'] = samples_frame['raw_proc_times'].map(lambda str_time: [int(t) for t in str_time.split(':')])
  samples_frame['raw_proc_times'] = samples_frame['raw_proc_times'].map(lambda time: (timedelta(hours = time[0], minutes = time[1], seconds = time[2])).total_seconds())

  # get the target processing time in seconds and add column with 0/1
  # if sample exceeds this
  target_time_seconds = timedelta(minutes = target_time).total_seconds()
  samples_frame['target_time'] = target_time_seconds
  samples_frame['above_target'] = samples_frame['raw_proc_times'].map(lambda proc_time: 1 if proc_time > target_time_seconds else 0)
  
  # add a year_month_taken column to dataframe
  # n.b. bit crude here? surely a better way?
  samples_frame['year_month_taken'] = pd.to_datetime(samples_frame['date_taken'], format = '%Y-%m-%d')
  samples_frame['year_month_taken'] = samples_frame['year_month_taken'].dt.strftime('%Y-%m')

  # create a pivot table from the dataframe, aggregating by month/yr
  mean_table_monthly = pd.pivot_table(samples_frame, values = ['sample_id', 'raw_proc_times', 'above_target'], index = ['year_month_taken'], aggfunc = {'sample_id': len, 'raw_proc_times': average, 'above_target': sum}, margins = True)
  mean_table_monthly.rename(columns = {'raw_proc_times': 'avg_proc_time_seconds', 'sample_id': 'sample_count'}, inplace = True)

  mean_table_monthly['avg_proc_time_hhmmss'] = pd.to_timedelta(mean_table_monthly['avg_proc_time_seconds'], unit = 's')

  return mean_table_monthly
  
# generate summary stats from dict of samples
# n.b. largely superceded by pivotSummaryStats, kept in for use by js graph
# output
# @param dict samples dict of samples data with keys inc 'sample_id',
# 'time_taken', etc. each key is a list of sample data
# @param int target_time in minutes for sample processing
# @return dict processing time stats for this subset of samples
def summaryStats(samples, target_time = 90):
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
    samples['raw_proc_times'] = raw_proc_times
  except KeyError:
    print('summaryStats: Error, unable to find processing_time key')
    exit(1)
  
  # convert to timedeltas
  target_time_seconds = timedelta(minutes = target_time).total_seconds()
  # list for processing times
  proc_times = []
  # counter for samples above target_time cutoff
  above_target = 0
  # counter for total samples
  sample_count = 0
  for index, str_time in enumerate(raw_proc_times):
    # work out the processing time
    time = [int(t) for t in str_time.split(':')]
    samples['raw_proc_times'][index] = ((timedelta(hours = time[0], minutes = time[1], seconds = time[2])).total_seconds())
    if index not in ignore_indexes:
      # sample should be included as not in ignore_indexes
      # increment sample_count
      sample_count += 1
      # work out the processing time
      time = [int(t) for t in str_time.split(':')]
      proc_times.append((timedelta(hours = time[0], minutes = time[1], seconds = time[2])).total_seconds())
      # increment above_target if processing time > target_time

      if proc_times[-1] > target_time_seconds:
        above_target += 1

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
        'count': sample_count,
        'above_target': above_target,
        'target_time': target_time,
        'units': 'seconds',
        'mean': proc_avg,
        '95_cent': proc_95_cent,
        'exclude': ignore
      }
    }

# split sample set by short_code col
# return dict of {short_code: [sample1, sample2, ...], ...}
def splitByStudy(samples):
  if 'short_code' not in samples.keys():
    raise KeyError('short_code must be in samples data')

  samples_bystudy = {}

  ##
  # generate the data structure
  #
  # dict of {short_code: {subset1: {sam_codes: [123, 456, ...], samples: {samplesdict}, ...}}
  # 

  # get a unique list (set) of all the short_code's

  short_codes = set(samples['short_code'])

  # get a unique list (set) of all the sam_code's

  sam_codes = set(samples['sam_code'])

  # get a unique set of tuples of (sam_code, short_code)

  sam_codes_bystudy = set(zip(samples['sam_code'], samples['short_code']))
  
  # generate the initial data structure of {short_code_1: {}, short_code_2: {}...}

  samples_bystudy = {short_code: {} for short_code in short_codes}

  ##
  # check if some sam_codes should be separated from bulk of study data
  #

  # set up lookup for sam_code to point to relevant samples array 
  sam_codes_lookup = {}

  ##
  # for each study we need to build a target data structure and
  # lookups for quickly assigning samples to sam_code/short_code 
  # locations
  #

  for short_code in short_codes:

    this_study_sam_codes = [study_code for study_code in sam_codes_bystudy if study_code[1] == short_code]  

    # see if config has a default target_time for this study
    target_time = 90
    try:
      study_config = json.loads(config['studies'][short_code])
      target_time = int(study_config['target_time'])
    except KeyError:
      pass

    # if there is a config setting for separating a group of sam_codes
    # from the rest of the study data, make sure we do this
    # if not, use the key 'all_relevant'

    subset_structure = {
      'sam_codes': [],
      'target_time': target_time,
      'samples': {field_name: [] for field_name in samples.keys()},
    }

    # see if we can get any subset structure specified in the config
    try:
      # add any subsetted sam_code/short_code combinations to sam_codes_lookup
      # we expect there to be an object with keys for each subset name
      # each subset name key contains an object with keys:
      #  - 'sam_codes' : list of sam_codes in this subset
      #  - 'target_time' (optional) : target processing time for subset
      separate_sam_codes = json.loads(config['study_subsets'][short_code])
      
      for subset_name, subset_data in separate_sam_codes.items():

        samples_bystudy[short_code][subset_name] = deepcopy(subset_structure)

        # try to add the target_time for this subset if present
        try:
          samples_bystudy[short_code][subset_name]['target_time'] = int(subset_data['target_time'])
        except KeyError:
          pass

        # add the sam_codes for this subset
        for subset_code in subset_data['sam_codes']:
          samples_bystudy[short_code][subset_name]['sam_codes'].append(subset_code)
          sam_codes_lookup[(subset_code, short_code)] = samples_bystudy[short_code][subset_name]['samples']

    except (KeyError, TypeError) as e:
      samples_bystudy[short_code]['all_relevant'] = deepcopy(subset_structure)

    # now add any unused sam_code/short_code combinations to sam_codes_lookup
    # which point to the 'all_relevant' (i.e. default) subset

    samples_bystudy[short_code]['all_relevant'] = deepcopy(subset_structure)

    for sam_code_study in this_study_sam_codes:
      if sam_code_study not in sam_codes_lookup.keys():
        samples_bystudy[short_code]['all_relevant']['sam_codes'].append(sam_code_study[0])
        sam_codes_lookup[sam_code_study] = samples_bystudy[short_code]['all_relevant']['samples']


  ##
  # split the samples 
  #

  for index, sam_code in enumerate(samples['sam_code']):

    short_code = samples['short_code'][index]

    for key in samples.keys():
      if key not in sam_codes_lookup[(sam_code, short_code)].keys():
        sam_codes_lookup[(sam_code, short_code)][key] = []
      sam_codes_lookup[(sam_code, short_code)][key].append(samples[key][index])

  return samples_bystudy

def load_config(config_filename = '', config = None):
  root_dir = os.getcwd()
  if config is None:
    config = configparser.ConfigParser()
  config.read(os.path.join(root_dir, config_filename))
  return config

# run from cmd line
# get the data file containing processing time for all core samples 
# (from the 'input' arg)
# load a config describing how to show the output (split by sam_code etc)
# process the data by study and config settings to get avg time etc
# save to outdir arg
# dumps a json file of summary stats
# outputs html that produces graphs of summary stats
if __name__ == '__main__':
  # get the root path we're running from
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
    # of {short_code_1: {field_name_1: [data1, etc], ...}, short_code_2: {...}}
    samples = splitByStudy(samples)
    summary = {}
    pivot_summary = pd.DataFrame()

    # generate summary stats by study and month/year

    # dump a logfile of samples object for debugging
    with open('samples_dump', 'w') as samples_dump:
      pprint(samples, samples_dump)

    for short_code, sampleset in samples.items():
      for subset_name, subset_data in sampleset.items():
        if len(subset_data['samples']['sam_code']) > 0:
          summary['{short_code}_{subset_name}'.format(short_code=short_code, subset_name=subset_name)] = summaryStats(subset_data['samples'], target_time = subset_data['target_time'])
          pivot = pivotSummaryStats(subset_data['samples'], target_time = subset_data['target_time'])
          pivot['study_subset'] = '{short_code}_{subset_name}'.format(short_code=short_code, subset_name=subset_name)
          pivot_summary = pd.concat([pivot, pivot_summary])
          #pivot_summary['{short_code}_{subset_name}'.format(short_code=short_code, subset_name=subset_name)] = pivotSummaryStats(subset_data['samples'], target_time = subset_data['target_time'])
        else:
          summary['{short_code}_{subset_name}'.format(short_code=short_code, subset_name=subset_name)] = 0
          #pivot_summary['{short_code}_{subset_name}'.format(short_code=short_code, subset_name=subset_name)] = 0

    summary['input_filename'] = input_filename
    pivot_summary['input_filename'] = input_filename
    pprint(pivot_summary)

    # dump the data to a file 

    now = datetime.now()
    datestamp = '{now.day:0>2}{now.month:0>2}{now.year}'.format(now = now) 

    # summary_stats to json
    output_filepath = re.sub('\.csv$', '_summarystats.{date}.json'.format(date = datestamp), input_filename)

    if args.output_dir is not None:
      output_dir = args.output_dir
    else:
      output_dir = os.path.split(input_filepath)[0]

    with open(os.path.join(output_dir, output_filepath), 'w') as output:
      json.dump(summary, output, indent=2)

    print('summary_stats: successfully output to {output_filepath}'.format(output_filepath = output_filepath))

    # pivot_summary to csv 
    output_filepath = re.sub('\.csv$', '_pivot_summarystats.{date}.csv'.format(date = datestamp), input_filename)

    if args.output_dir is not None:
      output_dir = args.output_dir
    else:
      output_dir = os.path.split(input_filepath)[0]

    with open(os.path.join(output_dir, output_filepath), 'w') as output:
      pivot_summary.to_csv(output)

    print('pivot_summary: successfully output to {output_filepath}'.format(output_filepath = output_filepath))

    # generate some graphs of mean value over time
    try:
      create_js.createLoadDataJs(output_dir, '^all\-studies\-sample\-processing\-time.*?\.json$', 'load_data.js.template', os.path.split(os.path.abspath(output_dir))[0])
      print('summary_stats: created load_data.js')
    except:
      print('summary_stats: error creating load_data.js')
      raise
      exit(1)

    # copy processing_time_graph.html and render_stats.js 
    # to dir above output_dir
    try:
      copy('./processing_time_graph.html', os.path.join(os.path.split(os.path.abspath(output_dir))[0], 'processing_time_graph.html'))
      copy('./render_stats.js', os.path.join(os.path.split(os.path.abspath(output_dir))[0], 'render_stats.js'))
      print('summary_stats: copied processing_time_graph.html and render_stats.js to output_dir')
    except:
      print('summary_stats: error copying processing_time_graph.html/render_stats.js to output_dir')
      raise
      exit(1)

    exit(0)
  else:
    print('--input argument missing: please specify a csv file to process')
    exit(1)
