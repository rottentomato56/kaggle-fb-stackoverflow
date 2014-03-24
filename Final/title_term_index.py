import json
import sys
import os
from collections import defaultdict
from itertools import combinations
from operator import add
from mrjob.job import MRJob

class TitleIndex(MRJob):
  def mapper(self, _, line):
  
    self.increment_counter('posts', 'count', 1)
    
    text_term_counts = defaultdict(int)
    
    post = json.loads(line.strip())
    post_id = int(post['id'])
    title_bigrams = post['title_bigrams']
      
    for bigram in title_bigrams:
      text_term_counts[' '.join(sorted(bigram))] += 1
      
    for (term, tf) in text_term_counts.iteritems():
      yield (term, post_id)
    
  def combiner(self, term, values):
    yield (term, [post_id[0] for post_id in values])
      
  
  def reducer(self, term, values):
    self.increment_counter('title_bigrams', 'count', 1)
    yield (term, [post_id for post_id in values])
           
           
if __name__ == '__main__':
  TitleIndex.run()
