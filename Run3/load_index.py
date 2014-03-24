import json
import pymongo
import sys
import math

c = pymongo.MongoClient('10.0.2.2', 27017)
db = c.KaggleFacebook
collection = db.class_index_top10

collection.drop()
collection.ensure_index('tag')
collection.ensure_index('text_index')
collection.ensure_index('code_index')

with open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/Mid20/classindex_10_filter.txt') as f:
  to_insert = []
  counter = 0
  for line in f:
    class_vector = json.loads(line.strip())
    textterm_scores = class_vector['text_terms']
    codeterm_scores = class_vector['code_terms']
    term_index = [t[0] for t in textterm_scores]
    code_index = [t[0] for t in codeterm_scores]
    
    text_vector_length = sum([t[3]**2 for t in textterm_scores])
    code_vector_length = sum([t[3]**2 for t in codeterm_scores])
    
    class_vector['text_index'] = term_index
    class_vector['code_index'] = code_index
    class_vector['text_norm'] = math.sqrt(text_vector_length)
    class_vector['code_norm'] = math.sqrt(code_vector_length)
    
    to_insert.append(class_vector)
    
    if len(to_insert) == 2000:
      collection.insert(to_insert)
      to_insert = []
      
    counter += 1
    sys.stdout.write('\r' + str(counter))
    sys.stdout.flush()
    
collection.insert(to_insert)
