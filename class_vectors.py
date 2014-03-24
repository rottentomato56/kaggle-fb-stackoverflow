"""

This module takes indexed posts and:

"""
from __future__ import division
import json
import math
import sys
from collections import defaultdict

from mrjob.job import MRJob

TOTAL_POSTS = 4353614 # number of posts in the created index this time
with open('/home/jason/Kaggle/Facebook/Data/Training/tag_counts.txt') as g:
      tag_counts = {}

      for line in g:
        tag = line.strip().split()[0]
        count = int(line.strip().split()[1])
        tag_counts[tag] = count
      
class Indexer(MRJob):
    
  def mutual_information(self, tag, token_tag_post_count, token_count):
	  """ Calculates the mutual information score for the term and the tag. """
	  # n11 is the number of posts where tag and term occur together
	  N_1_1 = token_tag_post_count
	  #print n11
	
	  # n01 is the number of posts where tag occurs and term doesn't
	  N_0_1 = tag_counts.get(tag, 0) - N_1_1
	  #print n01

	  # n10 is the number of posts where tag doesn't occur and term does
	  N_1_0 = token_count - N_1_1 
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
	  
#	    sys.stderr.write(tag + ' ' + str(N_1_1) + ' ' + str(N_1_A) + ' ' + str(N_A_1))
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
  
  def mapper(self, _, line):
    self.increment_counter('counter', 'counter', 1)
    token = line.strip().split('\t')[0]
    token = json.loads(token)
    
    values = line.strip().split('\t')[1]
    values = json.loads(values)
    
    total_token_post_count = values[0] #total_token_post_count is the number of posts that contain the token
    total_token_term_count = values[1] #total_token_term_count is the number of times token occurred overall
    
    postings = values[2]
    tag_list = []
    for tag_tuple in postings:
      score = round(self.mutual_information(tag_tuple[0], tag_tuple[1], total_token_post_count) * 10000, 5)    
      tag = tag_tuple[0]
      # round here to save space
      
      yield (tag, (token, score, tag_tuple[1]))
      
  def reducer(self, tag, values):
    output = []
    for entry in values:
      output.append(entry)
    yield (tag, output)
  	    
if __name__ == '__main__':
  Indexer.run()
