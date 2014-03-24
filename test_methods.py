""" 

this module is used for testing on the testing subset of the training data
Number of posts in the testing subset: 

"""
from __future__ import division
import math
import sys
import time
from operator import itemgetter
from collections import defaultdict
TOTAL_POSTS = 4353544


#load tag_counts

with open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/tag_counts.txt') as f:
  tag_counts = {}
  for line in f:
    tag = line.strip().split()[0]
    count = int(line.strip().split()[1])
    
    tag_counts[tag] = count
    
    
def test_method(collection, title_tokens, body_tokens, tag_data, actual_tags):
  
  c = collection.find({'token': {'$in' : list(set(title_tokens + body_tokens))}}, {'_id': 0, 'tags': {'$slice': 15}})
  tag_scores = {} # this will keep track of the scores of the possible predicted tags
  
  token_counts = defaultdict(int)
  # for now, we will not weight by title
  for token in title_tokens + body_tokens:
    token_counts[token] += 1
    
  if not c:
    # if none of the tokens can be found, predict top 3 tags
    predicted_tags = ['c#', 'java', 'php']
    result = f1_score(predicted_tags, actual_tags)
    p, r, total_score = result[0], result[1], result[2]
    return p, r, total_score

  for p in c:
  
    token = p['token']
    total_token_post_count = p['post_count']
    total_token_term_count = p['term_count']
    tag_list = p['tags']
    
#    if total_token_term_count / total_token_post_count > 5:
#      continue
    
    if any(tag[0] in actual_tags for tag in tag_list):
      print token
      print tag_list, '\n'
    for tag_tuple in tag_list:
      tag = tag_tuple[0]
      score = tag_tuple[1]
      token_tag_term_count = tag_tuple[2]
      
      tag_count, tag_num_tokens, cotag_list = tag_data[tag][0], tag_data[tag][1], tag_data[tag][2]
      
#      if total_token_post_count > 100000:
#        continue
      title_weight = 1
      if token in title_tokens:
        title_weight = 5
      
      test_weight = (token_counts[token] / len(title_tokens + body_tokens)) / (total_token_term_count / 360000000)  
      # weight the mutual information score by how often that token occured in test post?
      tag_scores[tag] = tag_scores.get(tag, 0) + score * title_weight * test_weight 
      
  sorted_tag_scores = sorted(tag_scores.items(), key=itemgetter(1), reverse=True)
  if not sorted_tag_scores:
    predicted_tags = ['c#', 'java', 'php']
  else:     
#    max_tag, max_score = sorted_tag_scores[0][0], sorted_tag_scores[0][1]
#    for cotag in tag_data[max_tag][2]:
#      if tag_scores.get(cotag[0]):
#        tag_scores[cotag[0]] = tag_scores[cotag[0]] * cotag[1] / tag_data[cotag[0]][0]
#        
#    # re-sort after bumping up scores
#    sorted_tag_scores = sorted(tag_scores.items(), key=itemgetter(1), reverse=True)    
    sorted_tags = [tag[0] for tag in sorted_tag_scores]
    
    baseline_num = min(3, len(sorted_tags))
    predicted_tags = sorted_tags[:baseline_num]
    
  print sorted_tag_scores[:20]
  x = raw_input()
  return predicted_tags
  
  

def naive_bayes(collection, title_tokens, body_tokens, actual_tags):
  
  c = collection.find({'token': {'$in' : list(set(title_tokens + body_tokens))}}, {'_id': 0, 'tags': {'$slice': 20}})
  tag_scores = {} # this will keep track of the scores of the possible predicted tags
  
  token_counts = defaultdict(int)
  
  for token in title_tokens + body_tokens:
    token_counts[token] += 1
    
  if not c:
    # if none of the tokens can be found, predict top 3 tags
    predicted_tags = ['c#', 'java', 'php']
    result = f1_score(predicted_tags, actual_tags)
    p, r, total_score = result[0], result[1], result[2]
    return p, r, total_score

  tag_index = {}
  token_total_counts = {}
  for p in c:
    token = p['token']
    total_token_post_count = p['post_count']
    total_token_term_count = p['term_count']
    tag_list = p['tags']
    
    token_total_counts[token] = total_token_term_count      
    title_weight = 1
#    if token in title_tokens:
#      title_weight = 5
      
    for tag_tuple in tag_list:
      tag = tag_tuple[0]
      mi_score = tag_tuple[1]
      token_tag_term_count = tag_tuple[2]
      tag_index[tag] = {token : (mi_score, token_tag_term_count)}
      
      #tag_count, tag_num_tokens, cotag_list = tag_data[tag][0], tag_data[tag][1], tag_data[tag][2]

  tag_probabilities = {}
  
  for tag in tag_index:
    for token in title_tokens + body_tokens:
      if token in tag_index[tag]:
        
        token_tag_term_count = tag_index[tag][token][1]
        mi_score = tag_index[tag][token][1]
        tag_num_tokens = tag_data[tag][1] # number of tokens in the tag class
      
        # conditional probability P(token|tag)
        # approx. 1 million unique terms in our index
        P_token_given_tag = math.log((token_tag_term_count + 1) / (tag_num_tokens + 1000000))
        
        # conditional probability P(token|tag)
        # approx. 360 million terms in total
        P_token_given_not_tag = math.log((token_total_counts[token] - token_tag_term_count + 1) / (360000000 - tag_num_tokens + 1000000))
        
        probs = tag_probabilities.get(tag, [0,0])
        probs = [probs[0] + P_token_given_tag, probs[0] + P_token_given_not_tag]
        tag_probabilities[tag] = probs
  
  tag_score_list = []  
  for entry in tag_probabilities.iteritems():
    tag = entry[0]
    p_inclass = entry[1][0]
    p_outclass = entry[1][1]
    P_class = p_inclass + math.log(tag_data[tag][0] / TOTAL_POSTS)
    P_notclass = p_outclass + math.log( 1 - tag_data[tag][0] / TOTAL_POSTS)
    
    tag_score_list.append((tag, P_class - P_notclass))
    
  sorted_tag_scores = sorted(tag_score_list , key=itemgetter(1), reverse=True)

  sorted_tags = [tag[0] for tag in sorted_tag_scores]
  
  baseline_num = min(3, len(sorted_tags))
  predicted_tags = sorted_tags[:baseline_num]
  
  print sorted_tag_scores[:5]
  x = raw_input()
  return predicted_tags
  
  
  
def test_method2(collection, title_tokens, body_tokens, tag_data, actual_tags):
  
  c = []
  start = time.time()
  
  for token in list(set(title_tokens + body_tokens)):
    class_with_token = collection.find(
                                        {'tokens.token' : token,
                                          'tokens': {
                                          '$elemMatch' : {'mi' : {'$gt' : .01}}
                                          }
                                        }, 
                                        {'tokens': 
                                          {'$elemMatch' : 
                                            {'token' : {'$in': ['python', 'java']}}
                                          },
                                          'tag' : 1,
                                          '_id' : 0,
                                        }
                                        )
  
    print token
    for p in class_with_token:
      print p
      
      c.append(p)
  print time.time() - start
  x = raw_input()
    
  
  if not c:
    # if none of the tokens can be found, predict top 3 tags
    predicted_tags = ['c#', 'java', 'php']
    result = f1_score(predicted_tags, actual_tags)
    p, r, total_score = result[0], result[1], result[2]
    return p, r, total_score

  tag_scores = {}
  print len(c)
  for p in c:
    
    tag = p['tag']
    token_data = p['tokens'][0]
   
    token = token_data['token']
    mi_score = token_data['mi']
    tag_scores[tag] = tag_scores.get(tag, 0) + mi_score
  
  sorted_tag_scores = sorted(tag_scores.items(), key=itemgetter(1), reverse=True)    
  sorted_tags = [tag[0] for tag in sorted_tag_scores]
    
  baseline_num = min(3, len(sorted_tags))
  predicted_tags = sorted_tags[:baseline_num]
    
  print sorted_tag_scores[:10]
  x = raw_input()
  return predicted_tags
  
  
  
def test_method3(collection, title_tokens, body_tokens, tag_data, actual_tags):
  
  c = []
  start = time.time()
  
  token_counts = defaultdict(int)
  for token in title_tokens + body_tokens:
    token_counts[token] += 1
    
  classes_with_tokens = collection.find(
                                      {'tokens.token' : 
                                        {'$in' : token_counts.keys()}
                                      }, 
                                      {'tokens': 
                                        {'$elemMatch' : 
                                          {'token' : {'$in': token_counts.keys()}}
                                        },
                                        'tag' : 1,
                                        'count' : 1,
                                        '_id' : 0,
                                      }
                                      )
                                      
  print 'done find', time.time() - start                                    
  for p in classes_with_tokens:
    c.append(p)
    
  print len(c)
  print time.time() - start
  
  x = raw_input()
    
  
  if not c:
    return .1, .1, 4
    # if none of the tokens can be found, predict top 3 tags
    predicted_tags = ['c#', 'java', 'php']
    result = f1_score(predicted_tags, actual_tags)
    p, r, total_score = result[0], result[1], result[2]
    return p, r, total_score

  tag_scores = {}
  for p in c:
    
    tag = p['tag']
    tag_num = p['count']
    token_data = p['tokens'][0]
   
    token = token_data['token']
    mi_score = token_data['mi']
    token_num = token_data['token_count']
    weight = 1
    if token in title_tokens:
      weight = 1.5
    
    tag_scores[tag] = tag_scores.get(tag, 0) + mi_score * weight
  
  sorted_tag_scores = sorted(tag_scores.items(), key=itemgetter(1), reverse=True)    
  sorted_tags = [tag[0] for tag in sorted_tag_scores]
    
  baseline_num = min(3, len(sorted_tags))
  predicted_tags = sorted_tags[:baseline_num]
    
  print sorted_tag_scores[:20]
  if all(a in tag_scores for a in actual_tags):
    print 'All', [(a, tag_scores[a]) for a in actual_tags]
  print '\nTitle', title_tokens
  x = raw_input()
  return predicted_tags
  
  
  
  
def test_method4(collection, title_tokens, body_tokens, actual_tags):
  
  c = []
  start = time.time()
  
  token_counts = defaultdict(int)
  for token in title_tokens + body_tokens:
    token_counts[token] += 1
    
  entries = collection.find({'token' : {'$in' : token_counts.keys()}}, {'tags' : {'$slice' : 1000}} )
                                      
                                     
  for p in entries:
    print p
    c.append(p)
  print 'done loading', time.time() - start    
#  print len(c)
#  print time.time() - start
  
  x = raw_input()
    
  
  if not c:
    # if none of the tokens can be found, predict top 3 tags
    predicted_tags = ['c#', 'java', 'php']
    result = f1_score(predicted_tags, actual_tags)
    p, r, total_score = result[0], result[1], result[2]
    return p, r, total_score

  tag_scores = {}
  for p in c:
    tags = p['tags']
    total_token_post_count = p['post_count']
    total_token_term_count = p['term_count']
   
    weight = 1
    if token in title_tokens:
      weight = 1.5

    for tag_posting in tags:
      tag = tag_posting[0]
      mi_score = tag_posting[1]
      token_tag_post_count = tag_posting[2]
      token_tag_term_count = tag_posting[3]
      tag_scores[tag] = tag_scores.get(tag, 0) + mi_score * weight * token_counts[token]
  
  if all(actual_tag in tag_scores.keys() for actual_tag in actual_tags):
    print 1
  else:
    print 0
  sorted_tag_scores = sorted(tag_scores.items(), key=itemgetter(1), reverse=True)    
  sorted_tags = [tag[0] for tag in sorted_tag_scores]
    
  baseline_num = min(10, len(sorted_tags))
  predicted_tags = sorted_tag_scores[:baseline_num]
    
  print 'Length of post:', len(token_counts.keys())
  print 'All tags:', len(sorted_tags)
  print 'Actual:', actual_tags, '\n'
  print 'Predicted:', predicted_tags, '\n'
  print [(actual_tag, tag_scores.get(actual_tag)) for actual_tag in actual_tags]
#  print sorted_tag_scores[:20]
#  if all(a in tag_scores for a in actual_tags):
#    print 'All', [(a, tag_scores[a]) for a in actual_tags]

  return predicted_tags
  
  
def naive_bayes2(collection, title_tokens, body_tokens, actual_tags):
  token_counts = defaultdict(int)
  for token in title_tokens + body_tokens:
    token_counts[token] += 1
    
  entries = collection.find({'token' : {'$in' : token_counts.keys()}}, {'_id' : 0, 'tags' : {'$slice' : 20}} )
 
    
  if not entries:
    # if none of the tokens can be found, predict top 3 tags
    predicted_tags = ['c#', 'java', 'php']
    result = f1_score(predicted_tags, actual_tags)
    p, r, total_score = result[0], result[1], result[2]
    return p, r, total_score

  token_total_counts = {}
  tag_scores = {}
  tag_index = {}
  for p in entries:
    tags = p['tags']
    total_token_post_count = p['post_count']
    total_token_term_count = p['term_count']
    token = p['token']
    weight = 1
    if token in title_tokens:
      weight = 5
    for tag_tuple in tags:
      tag = tag_tuple[0]
      mi_score = tag_tuple[1]
      token_tag_post_count = tag_tuple[2]
      token_tag_term_count = tag_tuple[3]
      tag_index[tag] = {token : (total_token_post_count, token_tag_post_count)}
      tag_scores[tag] = tag_scores.get(tag, 0) + mi_score * weight
      
  tag_probabilities = {}
  #print 'Actual Tags: ', actual_tags
  for tag in tag_index:
    inclass_score = 0
    outclass_score = 0
#    if tag not in actual_tags and tag != 'uiprintformatter':
#      continue
#    print 'TAG:', tag
#    print 'Tag count: ', tag_counts[tag]
    for token in token_counts.keys():
      # conditional probability P(token|tag)
      P_token_given_tag = math.log(tag_index[tag].get(token, [1,1])[1] / (tag_counts[tag] + 420000))
      
      inclass_score += P_token_given_tag
      
      # conditional probability P(token| not tag)
      P_token_given_not_tag = math.log( (tag_index[tag].get(token, [1,1])[0] - tag_index[tag].get(token, [1,1])[1] + 1) / (TOTAL_POSTS - tag_counts[tag] - 42000) )
      outclass_score += P_token_given_not_tag
      
      
      
      
#      print token
#      print 'total_token_post_count:' , tag_index[tag].get(token, [1,1])[0]
#      print 'token_tag_post_count:' , tag_index[tag].get(token, [1,1])[1]
#      print 'P_token_given_tag: ', P_token_given_tag
#      print 'P_token_given_not_tag: ', P_token_given_not_tag
#      print inclass_score, outclass_score
#      x = raw_input()
    tag_probabilities[tag] = (inclass_score, outclass_score)


  tag_score_list = []  
  for entry in tag_probabilities.iteritems():

    tag = entry[0]
    inclass_score = entry[1][0]
    outclass_score = entry[1][1]
    P_inclass = inclass_score + math.log((tag_counts[tag] + 1) / TOTAL_POSTS)
    P_notclass = outclass_score + math.log( 1 - ((tag_counts[tag] + 1) / TOTAL_POSTS))
    
#    if tag in actual_tags or tag == 'uiprintformatter':
#      print tag
#      print inclass_score, P_inclass
#      print outclass_score, P_notclass
#      x = raw_input()
      
    
    tag_score_list.append((tag, (P_inclass - P_notclass) * tag_scores[tag]**2 / math.sqrt(tag_counts[tag])))
    
  sorted_tag_scores = sorted(tag_score_list , key=itemgetter(1), reverse=True)

  sorted_tags = [tag[0] for tag in sorted_tag_scores]
  
  baseline_num = min(3, len(sorted_tags))
  predicted_tags = sorted_tags[:baseline_num]
  
 
#  print 'Actual Tags: ', actual_tags
#  print sorted_tag_scores[:10]
#  x = raw_input()
  return predicted_tags
  
  
  
def run(r):
    tag_scores = defaultdict(int)
    post = json.loads(r.strip())
    token_counts = defaultdict(int)
    for token in post['title'] + post['body'] :
        token_counts[token] += 1
    for class_data in tag_index.iteritems():
        if len(class_data[1][2]) < 20:
            continue
        tag = class_data[0]
        class_vector_length = 0
        for class_token in class_data[1][2]:
            token = class_token[0]
            mi_score = class_token[1]
            class_vector_length += mi_score**2
            weight = 1
            if token in post['title']:
                weight = 5
            tag_scores[tag] += mi_score * weight * token_counts.get(token, 0) / (len(post['title'] + post['body'])) / math.sqrt(class_vector_length)
        if class_vector_length == 0:
            tag_scores[tag] = 0
        else:
            tag_scores[tag] = tag_scores[tag]
    print 'Actual Tags: ', post['tags']
    return tag_scores
    
