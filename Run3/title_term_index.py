import json
import sys
import os
from collections import defaultdict
from operator import add
from mrjob.job import MRJob

class TitleIndex(MRJob):
  def mapper(self, _, line):
  
    self.increment_counter('posts', 'count', 1)
    
    text_term_counts = defaultdict(int)
    
    post = json.loads(line.strip())
    post_id = int(post['id'])
    title_terms = post['title']
    
    for term in title_terms:
      text_term_counts[term] += 1
      
    for (term, tf) in text_term_counts.iteritems():
        yield (term, post_id)
    
  def combiner(self, term_tuple, values):
    yield (term, [post_id for post_id in values])
      
  
  def reducer(self, term_tuple, values):
    yield (term, [post_id for post_id in values])
           
if __name__ == '__main__':
  TitleIndex.run()
