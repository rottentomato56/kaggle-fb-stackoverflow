import json
import pymongo
import sys
import os
client = pymongo.MongoClient('10.0.2.2', 27017)
db = client.KaggleFacebook
collection = db.post_index
collection.drop()
collection.ensure_index('tags')


def main(input_file):
  to_insert = []
  counter = 0
  with open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/Mid20/' + input_file) as f:
    for line in f:
      post = json.loads(line.strip())
      post_id = post['id']
      tags = post['tags']
      to_insert.append({'id': post_id, 'tags' : tags})
      counter += 1
      sys.stdout.write('\r' + str(counter))
      sys.stdout.flush()
      if len(to_insert) == 50000:
        collection.insert(to_insert)
        to_insert = []
      
  collection.insert(to_insert)
  
if __name__ == '__main__':
  input_file_list = sys.argv[1].split()
  for input_file in input_file_list:
    main(input_file)
  
      
