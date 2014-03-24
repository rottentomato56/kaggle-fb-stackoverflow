"""

This module takes the parsed posts and:

1. Uses MRJob to sort, aggregate, and calculate the doc and term frequencies for each term
2. Write an inverted index to an output file, which will then be loaded into MongoDB with another module

"""
import random
import json
from collections import defaultdict
from operator import itemgetter
from mrjob.job import MRJob

with open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/token_counts.txt') as g:
  token_counts = json.loads(g.readline())

class Indexer(MRJob):
    
  def mapper(self, _, line):
    tag = json.loads(line.strip().split('\t')[0])
    values = json.loads(line.strip().split('\t')[1])
    
    total_term_count = values[0]
    token_list = values[1]
    sorted_by_MI = sorted(token_list, key=itemgetter(1), reverse=True)
    top_ten_percent_terms = sorted_by_MI[:int(len(sorted_by_MI))]
    
#    if len(sorted_by_MI) <= 50:
#      # if there are less than or equal to 30 terms in the entire class, just use all terms
#      to_insert = sorted_by_MI
#    elif len(top_ten_percent_terms) < 50:
#      # if the top 10% of terms is less than 30, use 30 terms
#      to_insert = sorted_by_MI[:min(50, len(sorted_by_MI))]
#    else:
#      # the rest of the time use the top 10%
#      to_insert = top_ten_percent_terms
  
    to_insert = top_ten_percent_terms
    
    for token_tuple in to_insert:
      token = token_tuple[0]
      mi_score = token_tuple[1]
      token_class_count = token_tuple[2]
      token_term_count = token_tuple[3]
      
      yield (token, (tag, mi_score, token_class_count, token_term_count))
    	# output value will be of the form (a, b) where:
    	# a = token
    	# b = list of classes
  	    
  	    
  def reducer(self, token, values):
    self.increment_counter('token', 'unique_count', 1)
  
    # load token data
    token_data = token_counts[token]
    total_class_count = token_data[0]
    total_term_count = token_data[1]
    
    tag_list = [x for x in values]
    yield (token, (total_class_count, total_term_count, tag_list))
  	    
if __name__ == '__main__':
  Indexer.run()
