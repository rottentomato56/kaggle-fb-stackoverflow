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
collection = db.title_bigram_index
collection2 = db.class_index_final


def predict(post, final_index):
         
  all_terms = defaultdict(int)
  all_title_terms = defaultdict(int)
  all_code_terms = defaultdict(int)
  title_terms = post['title']
  body_terms = post['body']
  code_terms = post['code']
  for term in title_terms  + code_terms: #+ body_terms:
    all_terms[term] += 1
  for term in title_terms:
    all_title_terms[term] += 1
  for term in code_terms:
    all_code_terms[term] += 1
    
  c = collection2.find({'indexed_terms' : {'$in' : all_terms.keys()}}, {'_id' : 0, 'terms' : 0, 'indexed_terms' : 0, 'norm' : 0})
  c.batch_size(200)
  c.distinct('tag')

  tag_scores = {}
  pots = []
  for p in c:
    pots.append(p)
  for p in pots:
    tag = p['tag']
    terms = final_index[tag][1]
    norm = final_index[tag][0]
    dot_product = 0
    for (term, value) in terms:
      score = 0
      weight = 1
      if term in all_title_terms:
        weight += 1
      if term in all_code_terms:
        weight += 1
      score += all_terms[term] * value * weight
      
      dot_product += score
    tag_scores[tag] = dot_product / norm
  
  sorted_tags = sorted([(score, tag) for (tag, score) in tag_scores.iteritems()], reverse=True)
      
  predicted_tags = [x[1] for x in sorted_tags[:min(3, len(sorted_tags))]]
  
  if not predicted_tags:
    predicted_tags = ['c#', 'php', 'java']
  return predicted_tags    
  
  
def run_prediction(test_file):
   
        
  prediction_file = open('/home/jason/Kaggle/Facebook/Data/Testing/Predictions/' + test_file, 'w')
  counter = 0
  
  #with open('/home/jason/Kaggle/Facebook/Data/Training/Final/' + test_file) as f:
  with open('/home/jason/Kaggle/Facebook/Data/Testing/Split/' + test_file) as f:
    for line in f:
     
      kNN_scores = defaultdict(int)
      post = json.loads(line.strip())
      test_post_id = post['id']
      title_query = post['title']
      #actual_tags = post['tags']
      #print sorted(actual_tags), title_query
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
        predicted_tags = ['c#', 'php']
        
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
            
            
      counter += 1
      sys.stdout.write('\r' + str(counter))
      sys.stdout.flush()  
      #print sorted(predicted_tags), '\n'
     
      #x = raw_input()
      prediction_file.write(str(test_post_id) + ',' + ' '.join(predicted_tags) + '\n')
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
  print test_files
  
  pool.map(run_prediction, test_files)
  
        
        
      
        
        
