from __future__ import division
import json
import sys
import math
import pymongo
from collections import defaultdict
from operator import itemgetter

c = pymongo.MongoClient('10.0.2.2', 27017)
db = c.KaggleFacebook
collection = db.class_index_top10


def f1_score(predicted_tags, actual_tags):
  """ 
  This function calculates the f1 score given a set of predicted tags and a set
  of actual tags
  """
  
  num_correct = 0
  for tag in predicted_tags:
    if tag in actual_tags:
      num_correct += 1
  precision = num_correct / len(predicted_tags)
  recall = num_correct / len(actual_tags)
  if precision == 0 or recall == 0:
    score = 0
  else:
    score = (2 * precision * recall) / (precision + recall)
  return precision, recall, score
  
  
def load_index():
  c = collection.find({}, {'tag' : 1, 'text_terms' : 1, 'code_terms' : 1, 'text_norm' : 1, 'code_norm' : 1})

  class_lengths = {}
  text_index = defaultdict(list)
  code_index = defaultdict(list)
  for p in c:
    tag = p['tag']
    class_lengths[tag] = (p['text_norm'], p['code_norm'])
    text_terms = p['text_terms']
    code_terms = p['code_terms']
    for term in text_terms:
      text_index[term[0]].append((tag, term[3]))
    for term in code_terms:
      code_index[term[0]].append((tag, term[3]))
 

  return class_lengths, text_index, code_index
  
  
def load_index2():
  """ no bigrams """
  c = collection.find({}, {'tag' : 1, 'text_terms' : 1, 'code_terms' : 1, 'text_norm' : 1, 'code_norm' : 1})

  class_lengths = {}
  text_index = defaultdict(list)
  code_index = defaultdict(list)
  for p in c:
    tag = p['tag']
    #class_lengths[tag] = (p['text_norm'], p['code_norm'])
    text_terms = p['text_terms']
    code_terms = p['code_terms']
    for term in text_terms:
      if len(term.split()) == 2:
        continue
      text_index[term[0]].append((tag, term[3]))
      class_lengths[tag] = class_lengths.get(tag, 0) + term[3]**2 
      
    for term in code_terms:
      if len(term.split()) == 2:
        continue
      code_index[term[0]].append((tag, term[3]))
 
  class_lengths_n = {}
  for (tag, pre_norm) in class_lengths.iteritems():
    class_lengths_n[tag] = math.sqrt(pre_norm)
  return class_lengths_n, text_index, code_index
  
def test(class_lengths, text_index, code_index):

  with open('/home/jason/Kaggle/Facebook/Data/Training/tag_counts.txt', 'r') as h:
    tag_counts = {}
    for line in h:
      tag = line.strip().split()[0]
      count = int(line.strip().split()[1])
    
      tag_counts[tag] = count
      
    mid500_tags = [tag for (tag, count) in sorted(tag_counts.items(), key=itemgetter(1), reverse=True)[1000:1500]]
    
    mid500_dict = {}
    for tag in mid500_tags:
      mid500_dict[tag] = 1
  
  f1_scores = []
  precision_scores = []
  recall_scores = []
  counter = 0
  
  with open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/Testing/intesting.txt') as f:
    for line in f:
      text_term_counts = defaultdict(int)
      code_term_counts = defaultdict(int)
      
      post = json.loads(line.strip())
      body_terms = post['body']
      body_bigrams = [' '.join(sorted(t)) for t in post['body_bigrams']]
      title_terms = post['title']
      title_bigrams = [' '.join(sorted(t)) for t in post['title_bigrams']]
      code_terms = post['code']
      
      for term in body_terms + body_bigrams + title_terms + title_bigrams:
        text_term_counts[term] += 1
        
      for term in code_terms:
        code_term_counts[term] += 1
      
      actual_tags = post['tags']
      
      tag_text_scores = {}
      tag_code_scores = {}
     
      for (term, count) in text_term_counts.iteritems():
        tag_list = text_index[term]
        for tag_pair in tag_list:
          tag = tag_pair[0]
          score = tag_pair[1]
          weight = 1
          if term in title_terms + title_bigrams:
            weight = 2
          tag_text_scores[tag] = tag_text_scores.get(tag, 0) + score * weight
          
      for (term, count) in code_term_counts.iteritems():
        tag_list = code_index[term]
        for tag_pair in tag_list:
          tag = tag_pair[0]
          score = tag_pair[1]
          tag_code_scores[tag] = tag_code_scores.get(tag, 0) + score
      
      scores = {}
      for (tag, text_score) in tag_text_scores.iteritems():
        scores[tag] = text_score / class_lengths[tag][0]
        
      for (tag, code_score) in tag_code_scores.iteritems():
        scores[tag] = scores.get(tag, 0) + (2*code_score / class_lengths[tag][1])  # add in the code scores
      
      sorted_scores = sorted(scores.iteritems(), reverse=True, key=itemgetter(1))
      filtered_scores = [(tag, score) for (tag, score) in sorted_scores if tag in mid500_dict]
      predicted_tags = [tag for (tag, score) in filtered_scores][:1]
      
      if not predicted_tags:
        predicted_tags = [filtered_scores[0][0]]  # pick first one if none
      
      actual_filtered_tags = [tag for tag in actual_tags if tag in mid500_dict]
      
      p, r, s = f1_score(predicted_tags, actual_filtered_tags)
      
      f1_scores.append(s)
      precision_scores.append(p)
      recall_scores.append(r)
      
      counter += 1
      sys.stdout.write('\r' + str(counter) + '\t' + str(sum(precision_scores) / len(precision_scores)) + '\t' + str(sum(recall_scores) / len(recall_scores)) + '\t' + str(sum(f1_scores) / len(f1_scores)))

      sys.stdout.flush()
      
    print sum(f1_scores) / len(f1_scores)
    return f1_scores, precision_scores, recall_scores

#      print filtered_scores[:10]
#      print [tag for tag in actual_tags if tag in mid500_tags]
#      
#      x= raw_input()
        
        
#      c = collection.find({'$or' : 
#                            [ 
#                              {'term_index' : {'$in' : text_term_counts.keys()}}, 
#                              {'code_index' : {'$in' : code_term_counts.keys()}}
#                            ]
#                          },
#                          {'tag' : 1}
#                          )
#      print c.count()            
#      c.distinct('tag')
#      c.batch_size(1000)
#      print c.count() 
#      
#      tags = []
#      for p in c:
#        tags.append(p['tag'])
#        
#      print len(tags)
        
        #, 'text_terms' : 1, 'code_terms' : 1, 'text_norm' : 1, 'code_norm' : 1
    
    
    
