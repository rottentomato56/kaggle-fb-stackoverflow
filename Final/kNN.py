from __future__ import division
import pymongo
import json
import sys
import time
import os
from operator import itemgetter
from itertools import combinations
from multiprocessing import Pool
from collections import defaultdict

client = pymongo.MongoClient('10.0.2.2', 27017)
db = client.KaggleFacebook
collection = db.title_bigram_index


def run_prediction(test_file):

  prediction_file = open('/home/jason/Kaggle/Facebook/Data/Testing/Predictions/' + test_file, 'w')
  counter = 0
  
  with open('/home/jason/Kaggle/Facebook/Data/Training/Final/' + test_file) as f:
  #with open('/home/jason/Kaggle/Facebook/Data/Testing/Parsed/' + test_file) as f:
    for line in f:
     
      kNN_scores = defaultdict(int)
      post = json.loads(line.strip())
      test_post_id = post['id']
      title_query = post['title']
      actual_tags = post['tags']
      print sorted(actual_tags), title_query
      # create the bigrams from the title
      title_bigrams = [' '.join(sorted(b)) for b in list(combinations(title_query, 2))]
      # the total number of combinations equals the total number of possible matches
      # if the number of matches equal this, than that means the entire test post title was found in the training post title
      total_possible_matches = len(title_bigrams)

      c = collection.find({'term' : {'$in' : title_bigrams}}, {'_id' : 0})
      
      for p in c:
        posts = p['posts']
        for post_id in posts:
          kNN_scores[post_id] += 1
          
      sorted_scores = sorted([(matches / total_possible_matches, post_id) for (post_id, matches) in kNN_scores.iteritems()], reverse=True)
     
      if not sorted_scores:
        predicted_tags = ['c#']
        
      else:
        best_post = sorted_scores[0]
        
        best_score = best_post[0]

        if len(sorted_scores) == 1:
          p = db.post_index.find_one({'id' : best_post[1]})
          predicted_tags = p['tags']
 
        else:
          possible_posts = []
          for t in sorted_scores:
            if t[0] >= best_score:
              possible_posts.append(t[1])
            else:
              break
              
          c = db.post_index.find({'id' : {'$in' : possible_posts}}, {'id' : 1, 'tags' : 1, 'title' : 1})
          post_scores = defaultdict(int)
          tagp = {}
          
          for p in c:
            tags = p['tags']
            if 'design-patterns' in tags:
              print p
              x = raw_input()
            p_title = p['title']
            post_id = p['id']
            tagp[post_id] = tags
           
            s = p_title[:]
            for term in title_query:
              if term in s:
                post_scores[post_id] += 1
                index = s.index(term)
                del s[index]
                
            post_scores[post_id] = post_scores[post_id] / len(p_title)
             
              
          predicted_tags = sorted([(score, tagp[post_id]) for (post_id, score) in post_scores.iteritems()],  reverse=True)[0][1]
            
            
#      counter += 1
#      sys.stdout.write('\r' + str(counter))
#      sys.stdout.flush()  
      print sorted(predicted_tags), '\n'
     
      #x = raw_input()
      #prediction_file.write(str(test_post_id) + ',' + ' '.join(predicted_tags) + '\n')
      
  prediction_file.close()
  
  
def load(input_file):
  title_index = {}
  counter = 0
  with open('/home/jason/Kaggle/Facebook/Data/Training/Final/' + input_file) as f:
    for line in f:
      post = json.loads(line.strip())
      post_id = post['id']
      tags = post['tags']
      title_terms = post['title']
      
      title_index[' '.join(sorted(title_terms))] = [post_id, tags]
      counter += 1
      sys.stdout.write('\r' + str(counter))
      sys.stdout.flush()
      
    return title_index
      
if __name__ == '__main__':
  pool = Pool(3)
  input_folder = sys.argv[1]
  test_files = os.listdir(input_folder)
  #print test_files
  
  pool.map(run_prediction, test_files)
  
        
        
      
        
