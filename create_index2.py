"""

This module takes the parsed posts and:

1. Uses MRJob to sort, aggregate, and calculate the doc and term frequencies for each term
2. Write an inverted index to an output file, which will then be loaded into MongoDB with another module
3. Additionally, it will randomly select 1% of the entire training set to be used for testing

"""
import random
import json
from collections import defaultdict
from mrjob.job import MRJob

class Indexer(MRJob):
    
  def mapper_init(self):
    self.intesting_file = open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/intesting2.txt' ,'a')
  
  def mapper_final(self):
    self.intesting_file.close()
    
  def mapper(self, _, line):
    self.increment_counter('total', 'post_count', 1)
    if random.random() <= .01:
      self.increment_counter('testing', 'post_count', 1)
      # this generates a random subset of testing data to judge performance
      self.intesting_file.write(line + '\n')
      yield ('emptylineplaceholder', (1, 1, [('emptytag', 1, 1)]))
    
    else:
      self.increment_counter('training', 'post_count', 1)
      post = json.loads(line.strip())
      
      title_tokens = post['title']
      body_tokens = post['body']
      post_id = post['id']
      tags = post['tags']
      
      token_counts = defaultdict(int)
      
      # sum the number of times a term occurs in the post
      for token in title_tokens + body_tokens:
        token_counts[token] += 1
    
      for token_count_pair in token_counts.iteritems():
        token = token_count_pair[0]
        token_count = token_count_pair[1]
        postings_list = []
        for tag in tags:
          
          posting_tuple = (tag, 1, token_count)
          postings_list.append(posting_tuple)
          # posting tuple will be (a, b, c), where:
          # a = tag
          # b = 1 (doc frequency)
          # c = term frequency
         
        yield (token, (1, token_count, postings_list))
      	# output value will be of the form (a, (b, c, d)) where:
      	# a = token
      	# b = doc frequency
      	# c = term frequency
      	# d = list of postings
          
  def combiner(self, token, values):
  
    total_doc_frequency = 0
    total_term_frequency = 0
    post_doc_frequency = defaultdict(int)
    post_term_frequency = defaultdict(int)
    for entry in values:
      total_doc_frequency += entry[0]
      total_term_frequency += entry[1]
     
      postings_list = entry[2]
      
    
      for posting in postings_list:
        tag = posting[0]
        post_doc_frequency[tag] += posting[1]
        post_term_frequency[tag] += posting[2]
    
    postings_output_list = []
    for tag in post_doc_frequency:
      posting_tuple = (tag, post_doc_frequency[tag], post_term_frequency[tag])
      postings_output_list.append(posting_tuple)
        
    yield (token, (total_doc_frequency, total_term_frequency, postings_output_list))
  	    
  	    
  def reducer(self, token, values):
    total_doc_frequency = 0
    total_term_frequency = 0
    post_doc_frequency = defaultdict(int)
    post_term_frequency = defaultdict(int)
    
    for entry in values:
      total_doc_frequency += entry[0]
      total_term_frequency += entry[1]
     
      postings_list = entry[2]
      
     
      for posting in postings_list:
        tag = posting[0]
        post_doc_frequency[tag] += posting[1]
        post_term_frequency[tag] += posting[2]
        
    postings_output_list = []
    for tag in post_doc_frequency:
      posting_tuple = (tag, post_doc_frequency[tag], post_term_frequency[tag])
      postings_output_list.append(posting_tuple)
        
    yield (token, (total_doc_frequency, total_term_frequency, postings_output_list))
  	    
if __name__ == '__main__':
  Indexer.run()
