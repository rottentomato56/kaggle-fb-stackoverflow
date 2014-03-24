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
collection.ensure_index('tokens.token')


with open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/tag_counts.txt') as g:
  tag_counts = {}

  for line in g:
    tag = line.strip().split()[0]
    count = int(line.strip().split()[1])
    tag_counts[tag] = count

def main():
  f = open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/class_index.txt')
  num_terms_total = []
  num_terms_inserted = []
  insertions = []
  for line in f:
    tag = json.loads(line.strip().split('\t')[0])
    values = json.loads(line.strip().split('\t')[1])
    
    if tag == 'emptytag':
      continue
      
    total_term_count = values[0]
    token_list = values[1]
    sorted_by_MI = sorted(token_list, key=itemgetter(1), reverse=True)
    top_ten_percent_terms = sorted_by_MI[:int(len(sorted_by_MI) * .10)]
    
    if len(sorted_by_MI) <= 30:
      # if there are less than or equal to 30 terms in the entire class, just use all terms
      to_insert = sorted_by_MI
    elif len(top_ten_percent_terms) < 30:
      # if the top 10% of terms is less than 30, use 30 terms
      to_insert = sorted_by_MI[:min(30, len(sorted_by_MI))]
    else:
      # the rest of the time use the top 10%
      to_insert = top_ten_percent_terms
    
    #terms_to_insert = [s[0] for s in to_insert] # for Mongodb indexing, we need to split the terms and the values up
    values_to_insert = [{'token': s[0], 'mi' : s[1], 'token_count' : s[2], 'tag_count': s[3]} for s in to_insert]
    
    num_terms_total.append(len(token_list))
    num_terms_inserted.append(len(values_to_insert))

    sys.stdout.write('\r' + str(len(num_terms_total)))
    sys.stdout.flush()
    
    tag_count = tag_counts[tag]
    
    entry = {'tag' : tag, 'count' : tag_count, 'tokens' : values_to_insert}
    insertions.append(entry)
    
    if len(insertions) == 2500:
      collection.insert(insertions)
      insertions = []
  f.close()
  collection.insert(insertions)
  return num_terms_total, num_terms_inserted
