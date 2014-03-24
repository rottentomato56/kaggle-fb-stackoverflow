"""

This module loads the postings into MongoDB. We need to do some sort of weighting/ranking here, 
since Mongodb will take too long to retrieve all posts, so we can only retrieve the top 10 or so for each term.

"""

from __future__ import division
import pymongo
import json
import sys
from operator import itemgetter



def prep_insert(line):

  """ 
  
  Initial prep of the input index record before inserting into MongoDB. 
  
  This function takes care of the mutual information calculation, which orders the postings before insertion.
  This allows us to later retrieve top N, and receive the N highest scoring postings.
  
  Index entries will look like this:
  
  [total_token_post_count, total_token_count, [ (tag, token_tag_post_count, token_tag_term_count), ....] ]
  """
  
  token = line.strip().split('\t')[0]
  token = json.loads(token)
  
  values = line.strip().split('\t')[1]
  values = json.loads(values)
  
  total_token_post_count = values[0] #total_token_post_count is the number of posts that contain the token
  total_token_term_count = values[1] #total_token_term_count is the number of times token occurred overall
  
  inserted_tag_list = sorted(values[2], key=itemgetter(1), reverse=True)
  output = {'token': token, 
            'post_count': total_token_post_count, 
            'term_count': total_token_term_count, 
            'tags': inserted_tag_list}
  return output
  
  
  
# connect to db and insert

c = pymongo.MongoClient('10.0.2.2', 27017)
db = c.KaggleFacebook
collection = db.term_index_topten

collection.drop()
collection.ensure_index('token')
    
to_insert = []
f = open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/token_index_topten.txt')
counter = 0
for line in f:
  sys.stdout.write('\r' + str(counter))
  sys.stdout.flush()
  counter += 1
  output = prep_insert(line)
  if output:
    to_insert.append(output)
  if len(to_insert) == 5000:
    collection.insert(to_insert)
    to_insert = []
       
f.close()
# insert the remaining
if to_insert:    
  collection.insert(to_insert)
  


   
                
