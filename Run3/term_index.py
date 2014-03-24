import json
import sys
import os
from collections import defaultdict
from operator import add
from mrjob.job import MRJob

class TermIndex(MRJob):
  def mapper(self, _, line):
  
    self.increment_counter('posts', 'count', 1)
    
    text_term_counts = defaultdict(int)
    code_term_counts = defaultdict(int)
    
        
    post = json.loads(line.strip())
    body_terms = post['body']
    # body_bigrams = post['body_bigrams']
    # we will ignore bigrams in the body for now, as it is too much to process 12/15/2013
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
      tag_postings = [(tag, 1, tf) for tag in actual_tags]
      yield ((term, 'text'), (1, tf, tag_postings))
    
    for (term, tf) in code_term_counts.iteritems():
      tag_postings = [(tag, 1, tf) for tag in actual_tags]
      yield ((term, 'code'), (1, tf, tag_postings))
    
  def combiner(self, term_tuple, values):
    
    term = term_tuple[0]
    term_type = term_tuple[1]
    total_post_frequency = 0
    total_term_frequency = 0
    
    total_tag_post_frequencies = defaultdict(int)
    total_tag_term_frequencies = defaultdict(int)
    for value in values:
      post_frequency = value[0]
      term_frequency = value[1]
      
      total_post_frequency += post_frequency
      total_term_frequency += term_frequency
      
      tag_postings = value[2]
      
      
      for tag_entry in tag_postings:
        tag = tag_entry[0]
        tag_post_frequency = tag_entry[1]
        tag_term_frequency = tag_entry[2]
        
        total_tag_post_frequencies[tag] += tag_post_frequency
        total_tag_term_frequencies[tag] += tag_term_frequency
    
    final_tag_postings = [(tag, pf, total_tag_term_frequencies[tag]) for (tag, pf) in total_tag_post_frequencies.iteritems()] 
    
    yield ((term, term_type), (total_post_frequency, total_term_frequency, final_tag_postings))
      
  
  def reducer(self, term_tuple, values):
  
    self.increment_counter('unique terms', 'total_count', 1)
    
    term = term_tuple[0]
    term_type = term_tuple[1]
    total_post_frequency = 0
    total_term_frequency = 0
    
    total_tag_post_frequencies = defaultdict(int)
    total_tag_term_frequencies = defaultdict(int)
    for value in values:
      post_frequency = value[0]
      term_frequency = value[1]
      
      total_post_frequency += post_frequency
      total_term_frequency += term_frequency
      
      tag_postings = value[2]
      
      
      for tag_entry in tag_postings:
        tag = tag_entry[0]
        tag_post_frequency = tag_entry[1]
        tag_term_frequency = tag_entry[2]
        
        total_tag_post_frequencies[tag] += tag_post_frequency
        total_tag_term_frequencies[tag] += tag_term_frequency
    
    final_tag_postings = [(tag, pf, total_tag_term_frequencies[tag]) for (tag, pf) in total_tag_post_frequencies.iteritems()] 
    if pf > 2:
      self.increment_counter('unique terms', 'filtered_count', 1)
      yield ((term, term_type), (total_post_frequency, total_term_frequency, final_tag_postings))
        
      
if __name__ == '__main__':
  TermIndex.run()


