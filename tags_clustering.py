"""

This module takes the parsed posts and:

1. Uses MRJob to sort, aggregate, and calculate the number of words appearing in all posts for each tag
2. Also calculates the number of times each tag is seen with another tag

"""
import random
from collections import defaultdict
from mrjob.job import MRJob
from operator import itemgetter

class Indexer(MRJob):
  def mapper(self, _, line):

    post_sections = line.strip().split('\t')
    
    post_id = post_sections[0]
    if len(post_sections) < 4:
      # if the post_sections list is missing parts of the post, then most likely it does not have title tokens
      title_tokens = []
      body_tokens = post_sections[1].split(',')
      tags = ['python'] #post_sections[2].split(',')
      
    else:
      title_tokens = post_sections[1].split(',')
      body_tokens = post_sections[2].split(',')
      tags = post_sections[3].split(',')
    
    num_tokens = len(title_tokens + body_tokens)
      
    for main_tag in tags:
        # get all the tags not equal to the main tag with a count of 1
        tag_cluster = [(tag, 1) for tag in tags if tag != main_tag]
        yield (main_tag, (num_tokens, tag_cluster))
      
  def reducer(self, token, values):
    total_tokens = 0
    all_tag_cluster = defaultdict(int)
    for entry in values:
      num_tokens = entry[0]
      tag_cluster = entry[1]
      
      total_tokens += num_tokens
      for tag_pair in tag_cluster:
        all_tag_cluster[tag_pair[0]] += tag_pair[1]
        
    yield (token, (total_tokens, sorted(all_tag_cluster.items(), key=itemgetter(1), reverse=True)))
  	    
if __name__ == '__main__':
  Indexer.run()
