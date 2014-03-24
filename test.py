""" 

this module is used for testing on the testing subset of the training data
Number of posts in the testing subset: 

"""
from __future__ import division
import math
import pymongo
import time
import sys
import json
from operator import itemgetter
from collections import defaultdict

import test_methods
reload(test_methods)
from test_methods import test_method3, naive_bayes


	
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
  
  
def search(line, collection, tag_counts):
  parts = line.strip().split('\t')
  
  # some error occured in parsing, as one testing post doesn't seem to have an id
  if len(parts) == 4:
    post_id = int(parts[0])
    title_tokens = parts[1].split(',')
    body_tokens = parts[2].split(',')
    actual_tags = parts[3].split(',')
  else:
    try:
      post_id = int(parts[0])
      title_tokens = []
      body_tokens = parts[1].split(',')
      actual_tags = parts[2].split(',')
    except:
      title_tokens = []
      body_tokens = parts[0].split(',')
      actual_tags = parts[1].split(',')

  print 'Actual', actual_tags
  
  predicted_tags = test_method3(collection, title_tokens, body_tokens, tag_counts, actual_tags)
  result = f1_score(predicted_tags, actual_tags)
  p, r, total_score = result[0], result[1], result[2]
  
  return p, r, total_score
  
  
  
def run():
  tag_data = 'a'
#  g = open('/home/jason/Kaggle/Facebook/Data/Training/tag_counts.txt')
#  tag_counts = {}
#  for line in g:
#    tag = line.strip().split()[0]
#    count = int(line.strip().split()[1])
#    tag_counts[tag] = count
#  g.close()
#  
#  tag_data = {}
#  # load the term counts for each tag and co-occurences
#  with open('/home/jason/Kaggle/Facebook/Data/Training/tag_cooccurences.txt') as f:
#    for line in f:
#      tag = json.loads(line.strip().split('\t')[0])
#      data = json.loads(line.strip().split('\t')[1])
#      num_tokens = data[0]
#      cotag_list = data[1]
#      
#      if tag_counts.get(tag):
#        tag_data[tag] = [tag_counts[tag], num_tokens, cotag_list]
#  return tag_data
    
  c = pymongo.MongoClient('10.0.2.2', 27017)
  db = c.KaggleFacebook
  collection = db.class_index

  
  f = open('/home/jason/Kaggle/Facebook/Data/Training/intesting.txt')
  
  precision, recall, scores = [], [], []
  counter = 0
  for line in f:
    result = search(line, collection, tag_data)
    p, r, score = result[0], result[1], result[2]
    precision.append(p)
    recall.append(r)
    scores.append(score)
    counter += 1
#    sys.stdout.write('\r' + str(counter) + '\t' + str(sum(scores) / len(scores)) + '\t' + str(sum(precision) / len(precision)) + '\t' + str(sum(recall) / len(recall)))
#    sys.stdout.flush()
    if counter == 5000:
      break
  f.close()
  c.close()
  print '\nAvg precision', sum(precision) / len(precision)
  print 'Avg recall', sum(recall) / len(recall)
  print 'Avg F1 score', sum(scores) / len(scores)
  return precision, recall, scores
  

  
