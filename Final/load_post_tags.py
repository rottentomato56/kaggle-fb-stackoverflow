import json
import pymongo
import sys
import os
client = pymongo.MongoClient('10.0.2.2', 27017)
db = client.KaggleFacebook
collection = db.post_index_final
collection.drop()
collection.ensure_index('tags')
collection.ensure_index('title')
collection.ensure_index('id')


def main(input_file):
  to_insert = []
  counter = 0
  with open('/home/jason/Kaggle/Facebook/Data/Training/Final/' + input_file) as f:
    for line in f:
      post = json.loads(line.strip())
      post_id = post['id']
      tags = post['tags']
      title_terms = post['title']
      to_insert.append({'id': post_id, 'title' : title_terms, 'tags' : tags})
      counter += 1
      sys.stdout.write('\r' + str(counter))
      sys.stdout.flush()
      if len(to_insert) == 100000:
        collection.insert(to_insert)
        to_insert = []
  if to_insert:    
    collection.insert(to_insert)
      

if __name__ == '__main__':
  input_file_folder = sys.argv[1]
  input_file_list = os.listdir(input_file_folder)
  print input_file_list
  for input_file in input_file_list:
    main(input_file)
    
    
  
  
      
