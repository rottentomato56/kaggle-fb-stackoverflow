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
from test_methods import test_method4, naive_bayes2


	
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
  
  
def search(line, collection):
  post = json.loads(line)
  body_tokens = post['body']
  title_tokens = post['title']
  actual_tags = post['tags']
  
  #print 'Actual', actual_tags
  
  predicted_tags = naive_bayes2(collection, title_tokens, body_tokens, actual_tags)
  result = f1_score(predicted_tags, actual_tags)
  p, r, total_score = result[0], result[1], result[2]
  return p, r, total_score
  
  
  
def run():
    
  c = pymongo.MongoClient('10.0.2.2', 27017)
  db = c.KaggleFacebook
  collection = db.term_index_top25

  f = open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/intesting.txt')
  
  precision, recall, scores = [], [], []
  counter = 0
  for line in f:
    result = search(line, collection)
    p, r, score = result[0], result[1], result[2]
    precision.append(p)
    recall.append(r)
    scores.append(score)
    counter += 1
    sys.stdout.write('\r' + str(counter) + '\t' + str(sum(scores) / len(scores)) + '\t' + str(sum(precision) / len(precision)) + '\t' + str(sum(recall) / len(recall)))
    sys.stdout.flush()
    if counter == 5000:
      break
  f.close()
  c.close()
  print '\nAvg precision', sum(precision) / len(precision)
  print 'Avg recall', sum(recall) / len(recall)
  print 'Avg F1 score', sum(scores) / len(scores)
  return precision, recall, scores
  

  
