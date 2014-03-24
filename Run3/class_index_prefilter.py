"""

This module takes indexed posts and

DON'T CALL THIS MODULE PRE_FILTER!!!

"""
from __future__ import division
import json
import math
import sys
from collections import defaultdict
from operator import itemgetter

from mrjob.job import MRJob

 
text_term_counts = {}
code_term_counts = {}

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
       
      
class Indexer(MRJob):
    
#  def mutual_information(self, tag, token_tag_post_count, token_count):
#	  """ Calculates the mutual information score for the term and the tag. """
#	  # n11 is the number of posts where tag and term occur together
#	  N_1_1 = token_tag_post_count
#	  #print n11
#	
#	  # n01 is the number of posts where tag occurs and term doesn't
#	  N_0_1 = tag_counts.get(tag, 0) - N_1_1
#	  #print n01

#	  # n10 is the number of posts where tag doesn't occur and term does
#	  N_1_0 = token_count - N_1_1 
#	  #print n10
#	
#	  # n00 is the number of posts where tag doesn't occur and term doesn't
#	  N_0_0 = TOTAL_POSTS - N_1_1 - N_1_0 - N_0_1
#	  #print n00
#	
#	  N_1_A = N_1_1 + N_1_0 # N_1_A is the number of posts that contain the token	
#	  N_A_1 = N_1_1 + N_0_1 
#	  N_0_A = N_0_1 + N_0_0
#	  N_A_0 = N_0_0 + N_1_0 # N_0_A is the number of posts that don't contain the token
#	
#	  if N_1_A == 0 or N_A_1 == 0 or N_0_A == 0 or N_A_0 == 0:
#	    return 0
#	    
#	  part1 = (N_1_1 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_1_1) / (N_1_A * N_A_1), 2)
#	  if N_0_1 == 0:
#	    part2 = 0
#	  else:
#	    part2 = (N_0_1 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_0_1) / (N_0_A * N_A_1), 2)
#	  if N_1_0 == 0:
#	    part3 = 0
#	  else:
#	    part3 = (N_1_0 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_1_0) / (N_1_A * N_A_0), 2)
#	  if N_0_0 == 0:
#	    part4 = 0
#	  else:
#	    part4 = (N_0_0 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_0_0) / (N_A_0 * N_0_A), 2)

#	  mi_score = part1 + part2 + part3 + part4
#	  return mi_score
#    
    
  def mapper(self, _, line):
    self.increment_counter('counter', 'posts', 1)
      
    posttext_termcounts = defaultdict(int)
    postcode_termcounts = defaultdict(int)
    
    post = json.loads(line.strip())
    body_terms = post['body']
    body_bigrams = post['body_bigrams']
    title_terms = post['title']
    title_bigrams = post['title_bigrams']
    code_terms = post['code']
    actual_tags = post['tags']
    
    all_text_terms = body_terms + title_terms
    for bigram in title_bigrams:
      b = sorted(bigram)  # keep sorted so we can be consistent
    
      all_text_terms.append(b[0] + ' ' + b[1])
     
    for term in all_text_terms:
      posttext_termcounts[term] += 1
      
    for term in code_terms:
      postcode_termcounts[term] += 1
      
    text_term_list = [(term, 1, tf) for (term, tf) in posttext_termcounts.iteritems() if term in text_term_counts]
    code_term_list = [(term, 1, tf) for (term, tf) in postcode_termcounts.iteritems() if term in code_term_counts]
    
    
    for tag in actual_tags:
      
      yield (tag, (
                    {
                      'post_count' : 1, 
                      'text_term_count' : sum([x[2] for x in text_term_list]), 
                      'code_term_count' : sum([y[2] for y in code_term_list]),
                      'text_terms' : text_term_list,
                      'code_terms' : code_term_list
                    }
                  ))
      
  def reducer(self, tag, data):
  
    total_post_count = 0
    total_textterm_count = 0
    total_codeterm_count = 0
    
    posttext_termcounts = defaultdict(int)
    posttext_postcounts = defaultdict(int)
    
    postcode_termcounts = defaultdict(int)
    postcode_postcounts = defaultdict(int)
    
    for values in data:
      total_post_count += values['post_count']
      total_textterm_count += values['text_term_count']
      total_codeterm_count += values['code_term_count']
      
      text_term_list = values['text_terms']
      code_term_list = values['code_terms']
      
      for textterm_tuple in text_term_list:
        term = textterm_tuple[0]
        posttext_postcounts[term] += 1 # post freq.
        posttext_termcounts[term] += textterm_tuple[2] # term freq.
        
      for codeterm_tuple in code_term_list:
        term = codeterm_tuple[0]
        postcode_postcounts[term] += 1 # post freq.
        postcode_termcounts[term] += codeterm_tuple[2] # term freq.
  
    
    text_term_list = [(term, posttext_postcounts[term], tf) for (term, tf) in posttext_termcounts.iteritems()]
    code_term_list = [(term, postcode_postcounts[term], tf) for (term, tf) in postcode_termcounts.iteritems()]
  
      
    yield (tag, (
                  {
                    'post_count' : total_post_count, 
                    'text_term_count' : sum([x[2] for x in text_term_list]), 
                    'code_term_count' : sum([y[2] for y in code_term_list]),
                    'text_terms' : text_term_list,
                    'code_terms' : code_term_list
                    }
                  ))
  	    
if __name__ == '__main__':
  Indexer.run()
