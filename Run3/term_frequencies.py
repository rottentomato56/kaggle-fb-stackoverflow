import json
import sys
import os
from collections import defaultdict
from operator import add
from mrjob.job import MRJob

class TermCounter(MRJob):
  def mapper(self, _, line):
    self.increment_counter('posts', 'count', 1)
    
    text_term_counts = defaultdict(int)
    code_term_counts = defaultdict(int)
    
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
      text_term_counts[term] += 1
      
    for term in code_terms:
      code_term_counts[term] += 1
      
    for (term, tf) in text_term_counts.iteritems():
      yield ((term, 'text'), (1, tf))
    
    for (term, tf) in code_term_counts.iteritems():
      yield ((term, 'code'), (1, tf))
    
  def combiner(self, term_tuple, values):
    
    term = term_tuple[0]
    term_type = term_tuple[1]
    total_post_frequency = 0
    total_term_frequency = 0

    for value in values:
      post_frequency = value[0]
      term_frequency = value[1]
      
      total_post_frequency += post_frequency
      total_term_frequency += term_frequency
    
    yield ((term, term_type), (total_post_frequency, total_term_frequency))
      
  
  def reducer(self, term_tuple, values):
  
    term = term_tuple[0]
    term_type = term_tuple[1]
    total_post_frequency = 0
    total_term_frequency = 0
  
    if term_type == 'text':
      self.increment_counter('unique terms', 'text', 1)
    else:
      self.increment_counter('unique terms', 'code', 1)
      
    for value in values:
      post_frequency = value[0]
      term_frequency = value[1]
      
      total_post_frequency += post_frequency
      total_term_frequency += term_frequency
    
    yield ((term, term_type), (total_post_frequency, total_term_frequency))
        
if __name__ == '__main__':
  TermCounter.run()


