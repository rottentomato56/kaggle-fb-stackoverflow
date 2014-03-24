"""

This module loads the postings into MongoDB. We need to do some sort of weighting/ranking here, 
since Mongodb will take too long to retrieve all posts, so we can only retrieve the top 10 or so for each term.

"""

from __future__ import division
import pymongo
import json
import sys
import math
from operator import itemgetter

TOTAL_POSTS = 4353614 # number of posts in the created index this time

with open('/home/jason/Kaggle/Facebook/Data/Training/tag_counts.txt') as g:
  tag_counts = {}

  for line in g:
    tag = line.strip().split()[0]
    count = int(line.strip().split()[1])
    tag_counts[tag] = count


def mutual_information(tag, token_tag_post_count, token_count):
	""" Calculates the mutual information score for the term and the tag. """
	# n11 is the number of posts where tag and term occur together
	N_1_1 = token_tag_post_count
	#print n11
	
	# n01 is the number of posts where tag occurs and term doesn't
	N_0_1 = tag_counts.get(tag, 0) - N_1_1
	#print n01

	# n10 is the number of posts where tag doesn't occur and term does
	N_1_0 = token_count - N_1_1 
	#print n10
	
	# n00 is the number of posts where tag doesn't occur and term doesn't
	N_0_0 = TOTAL_POSTS - N_1_1 - N_1_0 - N_0_1
	#print n00
	
	N_1_A = N_1_1 + N_1_0 # N_1_A is the number of posts that contain the token	
	N_A_1 = N_1_1 + N_0_1 
	N_0_A = N_0_1 + N_0_0
	N_A_0 = N_0_0 + N_1_0 # N_0_A is the number of posts that don't contain the token
	
	part1 = (N_1_1 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_1_1) / (N_1_A * N_A_1), 2)
	if N_0_1 == 0:
	  part2 = 0
	else:
	  part2 = (N_0_1 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_0_1) / (N_0_A * N_A_1), 2)
	if N_1_0 == 0:
	  part3 = 0
	else:
	  part3 = (N_1_0 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_1_0) / (N_1_A * N_A_0), 2)
	if N_0_0 == 0:
	  part4 = 0
	else:
	  part4 = (N_0_0 / TOTAL_POSTS) * math.log((TOTAL_POSTS * N_0_0) / (N_A_0 * N_0_A), 2)

	mi_score = part1 + part2 + part3 + part4
	return mi_score


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
  
  if sys.getsizeof(token) > 100 or total_token_term_count < 5:
    # remove any tokens that only occur less than 5 times overall
    return None
    
  postings = values[2]
  tag_list = []
  for tag_tuple in postings:
    score = round(mutual_information(tag_tuple[0], tag_tuple[1], total_token_post_count) * 10000, 5) # to lessen memory used
    tag_list.append((tag_tuple[0], score, tag_tuple[2]))
    
  tag_list = sorted(tag_list, key=itemgetter(1), reverse=True)
  inserted_tag_list = tag_list[:min(50, len(tag_list))] # limit to top 50 tags to be more memory efficient. Generally will not be pulling more than 50 tags at one time.
  
  output = {'token': token, 
            'post_count': total_token_post_count, 
            'term_count': total_token_term_count, 
            'tags': inserted_tag_list}
  return output
  
  
  
# connect to db and insert

c = pymongo.MongoClient('10.0.2.2', 27017)
db = c.KaggleFacebook
collection = db.trained_index

collection.drop()
collection.ensure_index('token')  
    
to_insert = []
f = open('/home/jason/Kaggle/Facebook/Data/Training/indexed.txt')
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
  


   
                
