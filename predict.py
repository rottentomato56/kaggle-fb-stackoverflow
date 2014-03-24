from __future__ import division
import json
import math
import pymongo
import time
import sys
import os
from multiprocessing import Pool
from operator import itemgetter


def search(line, db):
  parts = line.strip().split('\t')
  post_id = parts[0]
  
  if len(parts) != 3:
    title_tokens = parts[1]
    body_tokens = parts[1]
  else:
    title_tokens = parts[1].split(',')
    body_tokens = parts[2].split(',')

  tag_scores = {}
  tokens = list(set(title_tokens + body_tokens))
  
  c = db.trained_index.find({'token': {'$in' : tokens}}, {'_id': 0, 'tags': {'$slice': 10}})
  for p in c:
    if not p:
      continue
    token_count = p['count']
    tag_list = p['tags']
    token = p['token']
    
    mi_scores = []
    for tag_pair in tag_list:
      tag = tag_pair[0]
      score = tag_pair[1]
      if score >= .00005:
        mi_scores.append((score, tag))
        tag_scores[tag] = tag_scores.get(tag, 0) + score
 
  sorted_tags = sorted(tag_scores.items(), key=itemgetter(1), reverse=True)
  predicted_tags = [tag_pair[0] for tag_pair in sorted_tags[:min(3, len(sorted_tags))]]
  return post_id, predicted_tags
  
def run(in_out_pair):
  c = pymongo.MongoClient('10.0.2.2', 27017)
  db = c.KaggleFacebook

  f = open('/home/jason/Kaggle/Facebook/Data/Testing/Simplified/' + in_out_pair[0])
  outfile = open('/home/jason/Kaggle/Facebook/Data/Testing/Results/' + in_out_pair[1], 'w')
  counter = 0
  for line in f:
    post_id, predicted_tags = search(line, db)
    outfile.write(post_id + ',' + ' '.join(predicted_tags) + '\n') 
    counter += 1
    sys.stdout.write('\r' + str(counter))
    sys.stdout.flush()
    
  f.close()
  outfile.close()
  c.close()
  return
  
  
  
if __name__ == '__main__':
	pool = Pool(3)
	input_folder = sys.argv[1]
	input_files = os.listdir(input_folder)
	output_files = ['predict_' + input_file for input_file in input_files]
	in_out_files = zip(input_files, output_files)
	print in_out_files
	#output = pool.map(run, in_out_files)
	
	h = open('Predictions.csv', 'w')
	h.write('Id,Tags\n')
	for outputfile in output_files:
	  g = open('/home/jason/Kaggle/Facebook/Data/Testing/Results/' + outputfile, 'r')
	  for line in g:
	    h.write(line)
	  g.close()
	h.close()
	  
	
