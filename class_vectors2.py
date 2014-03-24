"""

This module takes indexed posts and:

"""
from __future__ import division
import json
import math
import sys
from collections import defaultdict
from operator import itemgetter

from mrjob.job import MRJob

TOTAL_POSTS = 4353544 # number of posts in the created index this time
VOCABULARY_SIZE = 430,000
with open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/tag_counts.txt') as g:
      tag_counts = {}

      for line in g:
        tag = line.strip().split('\t')[0]
        count = int(line.strip().split('\t')[1])
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
    self.increment_counter('counter', 'unique_tokens', 1)
    
    index_entry = json.loads(line)
    token = index_entry['token']
    total_token_post_count = index_entry['post_count'] #total_token_post_count is the number of posts that contain the token
    total_token_term_count = index_entry['term_count'] #total_token_term_count is the number of times token occurred overall
    
    self.increment_counter('counter', 'num_total_tokens', total_token_term_count)
    
    postings = index_entry['tags']
    tag_list = []
    for tag_tuple in postings:
      tag = tag_tuple[0]
      token_tag_post_count = tag_tuple[1] # number of posts token and tag occur together
      token_tag_term_count = tag_tuple[2] # number of times token occurs in tag posts
      score = round(self.mutual_information(tag, token_tag_post_count, total_token_post_count) * 10000, 4)
      # round here to save space
      
      yield (tag, (token, score, token_tag_post_count, token_tag_term_count))
      
  def reducer(self, tag, values):
    output = []
    total_term_count = 0
    for entry in values:
      token_tag_term_count = entry[3]
      total_term_count += token_tag_term_count
      output.append(entry)
      #sorted_output = sorted(output, key=itemgetter(1), reverse=True)
    yield (tag, (total_term_count, output))
  	    
if __name__ == '__main__':
  Indexer.run()
