""" trained on 1,3,4, 5,7,9"""

from collections import defaultdict
from mrjob.job import MRJob


class TagCount(MRJob):
  def mapper(self, _, line):
    parts = line.strip().split('\t')
  
    post_id = parts[0]
    title_tokens = parts[1].split(',')
    body_tokens = parts[2].split(',')

    if len(parts) != 4:
      tags = ['c#']
    else:
      tags = parts[3].split(',')
      
    for tag in tags:
      yield (tag, 1)
      
  def combiner(self, tag, counts):
    yield (tag, sum(counts))
    
  def reducer(self, tag, counts):
    yield (tag, sum(counts))
    
    
if __name__ == '__main__':
  TagCount.run()
