import pymongo
import re
from operator import itemgetter

c = pymongo.MongoClient('192.168.1.71', 27017)
db = c.KaggleFacebook

TOKEN_RE = re.compile(r"[A-Za-z'+#]+")

with open('/home/jason/Kaggle/Facebook/Code/stop_words.txt') as f:
  stop_words = [line.strip() for line in f]

def validate_token(token):
  if len(token) == 1:
    if token.lower() in ('c', 'r'):
      # C and R are valid tokens with length 1
      return True
    else:
      return False
      
  if token.lower() in stop_words:
    return False
    
  return True
  
def search(query_string):
  tokens = [token.lower() for token in re.findall(TOKEN_RE, query_string) if validate_token(token)]
  
  for token in list(set(tokens)):
    p = db.test_index.find_one({'token': token})
    
    print p['token'], p['count'], sorted(p['tags'], key=itemgetter(1), reverse=True)[:5]
