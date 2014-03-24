from __future__ import division
import json
import pymongo
import sys
from operator import itemgetter

# connect to db and insert

c = pymongo.MongoClient('10.0.2.2', 27017)
db = c.KaggleFacebook
collection = db.class_index

collection.drop()
collection.ensure_index('tag')
collection.ensure_index('tokens')


with open('/home/jason/Kaggle/Facebook/Data/Training/tag_counts.txt') as g:
  tag_counts = {}

  for line in g:
    tag = line.strip().split()[0]
    count = int(line.strip().split()[1])
    tag_counts[tag] = count

def main(r):
  f = open('/home/jason/Kaggle/Facebook/Data/Training/classvectors.txt')
  num_terms_total = []
  num_terms_inserted = []
  insertions = []
  for line in f:
    tag = json.loads(line.strip().split('\t')[0])
    values = json.loads(line.strip().split('\t')[1])
    
    if tag == 'emptytag':
      continue
      
    sorted_by_MI = sorted(values, key=itemgetter(1), reverse=True)
    top_ten = sorted_by_MI[:min(10, len(sorted_by_MI))]
    
    avg_score = sum([t[1] for t in top_ten]) / len(top_ten)
    to_insert = [s for s in sorted_by_MI if s[1] > avg_score * r and sys.getsizeof(s[0]) < 100]
    
    terms_to_insert = [s[0] for s in to_insert] # for Mongodb indexing, we need to split the terms and the values up
    values_to_insert = [(s[0], s[1], s[2]) for s in to_insert]
    
    num_terms_total.append(len(values))
    num_terms_inserted.append(len(to_insert))

    sys.stdout.write('\r' + str(len(num_terms_total)))
    sys.stdout.flush()
    
    tag_count = tag_counts[tag]
    
    entry = {'tag' : tag, 'count' : tag_count, 'tokens' : terms_to_insert, 'data' : values_to_insert}
    insertions.append(entry)
    
    if len(insertions) == 2000:
      collection.insert(insertions)
      insertions = []
  f.close()
  collection.insert(insertions)
  return num_terms_total, num_terms_inserted
