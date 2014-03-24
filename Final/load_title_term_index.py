import json
import pymongo

client = pymongo.MongoClient('10.0.2.2', 27017)
db = client.KaggleFacebook
collection = db.title_term_index
collection.drop()
collection.ensure_index('term')

to_insert = []
with open(title_term_index_file) as f:
  for line in f:
    term = json.loads(line.strip().split()[0])
    posts = json.loads(line.strip().split()[1])
    to_insert.append({'term' : term, 'posts' : posts})
    
    if len(to_insert) == 10000:
      collection.insert(to_insert)
      to_insert = []
      
  collection.insert(to_insert)
