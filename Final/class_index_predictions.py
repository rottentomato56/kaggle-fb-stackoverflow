from __future__ import division
import pymongo
import json
import sys
import time
import os
import math
from operator import itemgetter
from itertools import combinations
from collections import defaultdict


client = pymongo.MongoClient('10.0.2.2', 27017)
db = client.KaggleFacebook
collection = db.class_index_final

def predict(post):
  all_terms = defaultdict(int)
  title_terms = post['title']
  body_terms = post['body']
  code_terms = post['code']
  
  for term in title_terms + body_terms + code_terms:
    all_terms[term] += 1
    
  c = collection.find({'indexed_terms' : {'$in' : all_terms.keys()}}, {'tag' : 1, 'terms' : 1, 'norm' : 1})
  
  tag_scores = {}
  for p in c:
    terms = p['terms']
    tag = p['tag']
    norm = p['norm']
    dot_product = 0
    for term in terms:
      weight = 1
      if term in title_terms:
        weight += 1
      if term in code_terms:
        weight += 1
        
      score = weight * term[1]
      dot_product += score
      tag_scores[tag] = dot_product / norm
  
  sorted_tags = sorted([(score, tag) for (tag, score) in tag_scores.iteritems()], reverse=True)
      
  predicted_tags = sorted_tags[:min(3, len(sorted_tags))]
  
  if not predicted_tags:
    predicted_tags = ['c#', 'php', 'java']
  return predicted_tags    
  
