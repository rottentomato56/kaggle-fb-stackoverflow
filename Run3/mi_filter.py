from __future__ import division
import json
import math
import sys
from collections import defaultdict
from operator import itemgetter

from mrjob.job import MRJob

 
TOTAL_POSTS = 559944

def mutual_information(tag, term, total_tag_post_count, term_tag_post_count, term_post_count):
  """ Calculates the mutual information score for the term and the tag. """
  # n11 is the number of posts where tag and term occur together
  N_1_1 = term_tag_post_count
  #print n11

  # n01 is the number of posts where tag occurs and term doesn't
  N_0_1 = total_tag_post_count - N_1_1
  #print n01

  # n10 is the number of posts where tag doesn't occur and term does
  N_1_0 = term_post_count - N_1_1 
  #print n10

  # n00 is the number of posts where tag doesn't occur and term doesn't
  N_0_0 = TOTAL_POSTS - N_1_1 - N_1_0 - N_0_1
  #print n00
  

  N_1_A = N_1_1 + N_1_0 # N_1_A is the number of posts that contain the token	
  N_A_1 = N_1_1 + N_0_1 
  N_0_A = N_0_1 + N_0_0
  N_A_0 = N_0_0 + N_1_0 # N_0_A is the number of posts that don't contain the token

  if N_1_A == 0 or N_A_1 == 0 or N_0_A == 0 or N_A_0 == 0:
    return 0
    
  part1 = (N_1_1 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_1_1) / (N_1_A * N_A_1), 2)
  if N_0_1 == 0:
    part2 = 0
  else:
    part2 = (N_0_1 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_0_1) / (N_0_A * N_A_1), 2)
  if N_1_0 == 0:
    part3 = 0
  else:
    part3 = (N_1_0 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_1_0) / (N_1_A * N_A_0), 2)

  if N_0_0 == 0:
    part4 = 0
  else:
    part4 = (N_0_0 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_0_0) / (N_A_0 * N_0_A), 2)

  mi_score = part1 + part2 + part3 + part4
  return mi_score
    
    
def mi_filter(r):

  text_term_counts = {}
  code_term_counts = {}
  
  outfile = open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/Mid20/classindex_10_filter.txt', 'w')

  with open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/text_term_counts.txt') as g:
      for line in g:
        term = line.strip().split('\t')[0]
        count = int(line.strip().split('\t')[1])
        text_term_counts[term] = count
        
  with open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/code_term_counts.txt') as g:
      for line in g:
        term = line.strip().split('\t')[0]
        count = int(line.strip().split('\t')[1])
        code_term_counts[term] = count
        
        
  with open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/Mid20/classindex_preMIfilter.txt') as f:
    counter = 0
    for line in f:
      counter += 1
      tag = json.loads(line.strip().split('\t')[0])
      data = json.loads(line.strip().split('\t')[1])
      
      total_post_count = data['post_count']
      total_textterm_count = data['text_term_count']
      total_codeterm_count = data['code_term_count']
      
      text_term_list = data['text_terms']
      code_term_list = data['code_terms']
      
      textterms_scores = []
      for text_term_tuple in text_term_list:
        term = text_term_tuple[0]
        pf = text_term_tuple[1]
        mi_score = mutual_information(tag, term, total_post_count, pf, text_term_counts.get(term, 0))
        entry = text_term_tuple + [round(mi_score * 10000, 6)]
        textterms_scores.append(entry)
      limit = int(len(textterms_scores) * r)
      
      if len(textterms_scores) <= 30:
        sorted_textterms = sorted(textterms_scores, key=itemgetter(3), reverse=True)
      if limit < 30:
        limit = 30
      
      sorted_textterms = sorted(textterms_scores, key=itemgetter(3), reverse=True)[:limit]
      
      codeterms_scores = []
      for code_term_tuple in code_term_list:
        term = code_term_tuple[0]
        pf = code_term_tuple[1]
        mi_score = mutual_information(tag, term, total_post_count, pf, code_term_counts.get(term, 0))
        entry = code_term_tuple + [round(mi_score * 10000, 6)]
        codeterms_scores.append(entry)
      limit = int(len(codeterms_scores) * r)
      
      if len(codeterms_scores) <= 30:
        sorted_codeterms = sorted(codeterms_scores, key=itemgetter(3), reverse=True)
      if limit < 30:
        limit = 30
      
      sorted_codeterms = sorted(textterms_scores, key=itemgetter(3), reverse=True)[:limit]
      
      sys.stdout.write('\r' + str(counter))
      sys.stdout.flush()
      
      outfile.write(json.dumps({
                    'tag' : tag,
                    'post_count' : total_post_count, 
                    'text_term_count' : total_textterm_count,
                    'code_term_count' : total_codeterm_count,
                    'text_terms' : sorted_textterms,
                    'code_terms' : sorted_codeterms
                    }) + '\n')
                    
  outfile.close()
    

