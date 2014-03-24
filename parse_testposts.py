"""

This module will perform the necessary pre-processing by taking a StackOverflow post and converting the title and body into tokens. It will then write the title tokens, body tokens, and the tags into a new file, separated by a tab

This is mainly used to make it work on EMR. (in the future)

"""

import csv
import re
import sys
import os
import lxml.html
from multiprocessing import Pool

TOKEN_RE = re.compile(r"[A-Za-z#][\w'+#]+[A-Za-z]")

with open('/home/jason/Kaggle/Facebook/Code/stop_words.txt') as f:
  stop_words = [line.strip() for line in f]

def clean_file(input_file):
  f = open('/home/jason/Kaggle/Facebook/Data/Training/Nodupes/' + input_file, 'r')
  output_file = input_file.split('.')[0] + '_simple.txt'
  g = open('/home/jason/Kaggle/Facebook/Data/Training/NewParsed/' + output_file, 'w')
  csv_reader = csv.reader(f)
  
  counter = 0
  for post in csv_reader:
    title_html = post[1].strip()
    title = ''
    if title_html:
			try:
				title = lxml.html.fromstring(title_html).text_content()
			except:
			  pass
    
    body_html = post[2].strip()
    body = ''
    if body_html:
			try:
				body = lxml.html.fromstring(body_html).text_content()
			except:
				pass
				
    post_id = post[0]
    title_tokens = [token.lower() for token in re.findall(TOKEN_RE, title) if validate_token(token)]
    body_tokens = [token.lower() for token in re.findall(TOKEN_RE, body) if validate_token(token)]
    tags = post[3].strip().split()
    
    g.write(post_id + '\t' + ','.join(title_tokens) + '\t' + ','.join(body_tokens) + '\t' + ','.join(tags) + '\n' )
    counter += 1
    sys.stdout.write('\r' + str(counter))
    sys.stdout.flush()
    
#    if counter == 1000:
#    	break
  g.close()
  f.close()
  
  
def validate_token(token):
  if len(token) == 1:
    if token.lower() in ('c', 'r'):
      # C and R are valid tokens with length 1
      return True
    else:
      return False
      
  if token.lower() in stop_words:
    return False
    
  return True
  
  
if __name__ == '__main__':
	pool = Pool(3)
	input_folder = sys.argv[1]
	input_files = os.listdir(input_folder)
	print input_files
	output = pool.map(clean_file, input_files)	
