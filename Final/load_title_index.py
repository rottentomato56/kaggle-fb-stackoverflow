import pymongo
import json
import sys

client = pymongo.MongoClient('10.0.2.2', 27017)
db = client.KaggleFacebook
collection = db.title_bigram_index
collection.drop()

collection.ensure_index('term')

counter = 0
with open('title_index.txt') as f:
  to_insert = []
  for line in f:
    counter += 1
    sys.stdout.write('\r' + str(counter))
    sys.stdout.flush()
    title_bigram = json.loads(line.strip().split('\t')[0])
    posts = json.loads(line.strip().split('\t')[1])
    insert_posts = []
    for post_list in posts:
      for t in post_list:
        insert_posts.append(t)
    to_insert.append({'term' : title_bigram, 'posts' : insert_posts})
    if len(to_insert) == 200000: 
      collection.insert(to_insert)
      to_insert = []
      
if to_insert:
  collection.insert(to_insert)  
    
      
    
