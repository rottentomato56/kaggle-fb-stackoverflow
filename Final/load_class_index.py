from __future__ import division
import pymongo
import json
import sys
import time
import os
import math
from operator import itemgetter
from itertools import combinations
from multiprocessing import Pool
from collections import defaultdict


client = pymongo.MongoClient('10.0.2.2', 27017)
db = client.KaggleFacebook
collection = db.class_index_final

collection.drop()
collection.ensure_index('tag')
collection.ensure_index('indexed_terms')
counter = 0

with open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/class_index.txt') as f:
  to_insert = []
  for line in f:
    counter += 1
    sys.stdout.write('\r' + str(counter))
    sys.stdout.flush()
    tag = json.loads(line.strip().split('\t')[0])
    data = json.loads(line.strip().split('\t')[1])
    
    term_list = data[1]
    
    sorted_term_list = [(x[0], x[1]) for x in sorted(term_list, key=itemgetter(1), reverse=True)[:30]]  # only use top 20 terms
    
    class_length = math.sqrt(sum([weight ** 2 for (term, weight) in sorted_term_list]))
    
    indexed_terms = [t[0] for t in sorted_term_list]
    
    to_insert.append({'tag' : tag, 'indexed_terms' : indexed_terms, 'norm' : class_length, 'terms' : sorted_term_list})
    
    if len(to_insert) == 5000:
      collection.insert(to_insert)
      to_insert = []

if to_insert:
  collection.insert(to_insert)
