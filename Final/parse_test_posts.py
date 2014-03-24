"""

This module will perform the necessary pre-processing by taking a StackOverflow post and converting the title and body into tokens, and extracting code tokens. It will then write the title tokens, body tokens, and the tags into a new file as json.

"""

import csv
import re
import sys
import os
import json
import lxml.html
import random
from itertools import combinations
from multiprocessing import Pool
from operator import itemgetter

TEXT_TOKEN_RE = re.compile(r"([A-Za-z#][-\w'+]*[A-Za-z+]|[A-Za-z]+#|[A-Za-z]{1})")
CODE_TOKEN_RE = re.compile(r"[A-Za-z#]{1}[-\w'+]+[A-Za-z+]")

stop_words = {}
with open('/home/jason/Kaggle/Facebook/Code/stop_words.txt') as f:
  for line in f:
    stop_words[line.strip()] = 1
  
def parse_file(input_file):
  f = open('/home/jason/Kaggle/Facebook/Data/Testing/Splitfiles/' + input_file, 'r')
  
  # output_file will contain the parsed training data
  output_file = input_file.split('.')[0] + '_parsed.txt'
  g = open('/home/jason/Kaggle/Facebook/Data/Testing/Parsed/' + output_file, 'w')
  
  csv_reader = csv.reader(f)
  counter = 0
  for post in csv_reader:
    post_id = int(post[0])
        
    #tags = post[3].strip().split()
    
    # if title is empty, default is 'a' as it will later be removed as a stop word
    title_html = post[1].strip()
    if not title_html:
      title = 'a'
    else:
      try:
			  title = title_html
      except:
        print input_file, post_id
        print title_html
        continue
    
    # if body is empty, default is 'a' as it will later be removed as a stop word
    body_html = post[2].strip()
    if not body_html:
      body = 'a'
    else:
      try:
        body = lxml.html.fromstring(body_html.decode('utf-8', 'ignore'))
      except:
        print input_file, post_id
        print 'BODY: ', body_html
        continue
   
    code = ''
    for element in body.iterdescendants():
      # check for <code> elements, which has different parsing rules
      if element.tag == 'code':
        code += ' ' + element.text_content() # makes sure there is a space so regex can split
        element.drop_tree()
    main_body = body.text_content()

    title_tokens = []
    for token in re.findall(TEXT_TOKEN_RE, title):
      token = validate_token(token.lower())
      if token:
        title_tokens.append(token)
 
    body_tokens = [] 
    for token in re.findall(TEXT_TOKEN_RE, main_body):
      token = validate_token(token.lower())
      if token:
        body_tokens.append(token)
       
    code_tokens = [] 
    for token in re.findall(CODE_TOKEN_RE, code):
      token = validate_token(token.lower())
      if token:
        code_tokens.append(token)
        
    # here we create bigram combinations
    
    title_bigrams = list(combinations(title_tokens, 2))
    
    #for body bigrams, we use sections of 5 tokens
    body_bigrams = []
    body_sections = split_tokens(body_tokens, 5)
    for section in body_sections:
      body_bigrams += list(combinations(section, 2))
  
    # randomly choose 2% of the training data for testing
    outfile = g
    try: 
      outfile.write(json.dumps({'id' : post_id, 
                          'title': title_tokens, 
                          'title_bigrams': title_bigrams,
                          'body' : body_tokens,
                          'body_bigrams' : body_bigrams,
                          'code' : code_tokens
                          }) + '\n')
    except:
      print 'Bad Post: ', input_file, post_id
      pass
                        
    counter += 1
    sys.stdout.write('\r' + str(counter))
    sys.stdout.flush()
  g.close()
  f.close()
  
# split tokens in sections of n

def split_tokens(token_list, n):
  t_list = token_list[:]
  i = 0
  sections = []
  s = []
  while t_list:
    token = t_list.pop(0)
    s.append(token)
    i += 1
    if i == n:
      sections.append(s)
      i = 0
      s = []
  sections.append(s)  
  return sections
      
      

# do some additional cleaning of tokens
def validate_token(token):
  if len(token) > 35:
    return None
  if len(token) > 2 and token[-2:] == "'s":
      token = token[:-2]
      
  if len(token) == 1:
    if token.lower() in ('c', 'r'):
      # C and R are valid tokens with length 1
      return token
    else:
      return False
      
  if token.lower() in stop_words:
    return False
  return token
  
  
def debug(input_file):
  f = open('/home/jason/Kaggle/Facebook/Data/Training/Nodupes/' + input_file, 'r')
  
  csv_reader = csv.reader(f)
  counter = 0
  prev_id = -1
  for post in csv_reader:
    post_id = int(post[0])
    counter += 1
    sys.stdout.write('\r' + str(counter))
    sys.stdout.flush()
    if prev_id == 1501096:
     
      f.close()
      return post
    prev_id = post_id
  
  
if __name__ == '__main__':
	pool = Pool(3)
	input_folder = sys.argv[1]
	input_files = os.listdir(input_folder)
	print input_files
	output = pool.map(parse_file, input_files)	
