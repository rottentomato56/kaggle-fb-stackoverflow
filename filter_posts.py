import json
import sys

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
  
  if sys.getsizeof(token) > 100 or total_token_post_count <= 5:
    # remove any tokens that only occur less than 5 posts overall
    return None
    
  postings = values[2]
  
  output = {'token': token, 
            'post_count': total_token_post_count, 
            'term_count': total_token_term_count, 
            'tags': postings}
            
  return output
  
  
if __name__ == '__main__':
  indexed_file = sys.argv[1]
  with open(indexed_file) as f:
    counter = 0
    for line in f:
      output = prep_insert(line)
      if output:
        print json.dumps(output)
      
    
  
  
