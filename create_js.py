import sys, os
import re

def createLoadDataJs(datapath, filepattern, templatefile, outputpath):
  if not os.path.exists(datapath):
    raise valueerror('datapath does not appear to exist')
  if not os.path.exists(outputpath):
    raise valueerror('outputpath does not appear to exist')

  matched_files = []
  for root, dirs, files in os.walk(datapath):
    for filename in files:
      if re.search(filepattern, filename):
        matched_files.append(filename)

  with open(templatefile, 'r') as tempfile:
    template = tempfile.read()
  
  file_template_string = '''.defer(d3.json, '{filepath}')'''
  files_string = ''

  for filename in matched_files:
    files_string += file_template_string.format(filepath = os.path.join(datapath, filename))

  files_output = template.format(include_files = files_string)

  with open(os.path.join(outputpath, 'load_data.js'), 'w') as output:
    output.write(files_output)

