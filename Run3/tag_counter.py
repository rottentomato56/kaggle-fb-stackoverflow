import json
import sys
import os
from collections import defaultdict
from operator import add
from mrjob.job import MRJob

class TagCounter(MRJob):
  def mapper(self, _, line):
    self.increment_counter('posts', 'count', 1)
    post = json.loads(line.strip())
    actual_tags = post['tags']
    
    for tag in actual_tags:
      yield (tag, 1)
    
  def combiner(self, tag, values):
    count = sum(values)
    
    yield (tag, count) 
  
  def reducer(self, tag, values):
    self.increment_counter('tags', 'count', 1)
    count = sum(values)
    yield (tag, count)
        
      
if __name__ == '__main__':
  TagCounter.run()


