from collections import defaultdict
from mrjob.job import MRJob


#c = pymongo.MongoClient('192.168.1.71', 27017)
#db = c.KaggleFacebook
#db.test_index.drop()

#db.test_index.ensure_index('token')

class Indexer(MRJob):
  def mapper(self, _, line):
  
    parts = line.strip().split('\t')
    
    post_id = parts[0]
    title_tokens = parts[1].split(',')
    body_tokens = parts[2].split(',')
    
    if len(parts) != 4:
    	tags = ['c#']
    else:
    	tags = parts[3].split(',')
    
    tag_counts = [(tag, 1) for tag in tags]
    for token in list(set(title_tokens + body_tokens)):
  	  yield (token, (1, tag_counts))
  	    
  def combiner(self, token, values):
    partial_count = 0
    tag_counts = defaultdict(int)
    for tag_count_pairs in values:
      partial_count += tag_count_pairs[0]
      
      for pair in tag_count_pairs[1]:
        tag_counts[pair[0]] += pair[1]
        
    yield (token, (partial_count, tag_counts.items()))
  	    
  	    
  def reducer(self, token, counts_and_tags):
     tag_counts = defaultdict(int)
     total_count = 0
     for pair in counts_and_tags:
       total_count += pair[0]
       for tag in pair[1]:
         tag_counts[tag[0]] += tag[1]
      
     yield (token, (total_count, tag_counts.items()))
     
#     db.test_index.insert({'token': token,
#                'count': total_count,
#                'tags': tag_counts.items()
#                })
#                
     
       
if __name__ == '__main__':
  Indexer.run()
