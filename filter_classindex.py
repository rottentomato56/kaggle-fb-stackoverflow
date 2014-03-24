import json
import sys
from operator import itemgetter



tag_counts = {}
with open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/tag_counts.txt') as g:
  for line in g:
    tag = line.strip().split()[0]
    count = int(line.strip().split()[1])
    tag_counts[tag] = count
  

def mapper(line):
  tag = json.loads(line.strip().split('\t')[0])
  values = json.loads(line.strip().split('\t')[1])
  
  total_term_count = values[0]
  token_list = values[1]
  sorted_by_MI = sorted(token_list, key=itemgetter(1), reverse=True)
  top_ten_percent_terms = sorted_by_MI[:int(len(sorted_by_MI) * .10)]
  
  if len(sorted_by_MI) <= 50:
    # if there are less than or equal to 30 terms in the entire class, just use all terms
    to_insert = sorted_by_MI
  elif len(top_ten_percent_terms) < 50:
    # if the top 10% of terms is less than 30, use 30 terms
    to_insert = sorted_by_MI[:min(50, len(sorted_by_MI))]
  else:
    # the rest of the time use the top 10%
    to_insert = top_ten_percent_terms

  to_insert = top_ten_percent_terms
  
  output = [tag, tag_counts.get(tag, 0), total_term_count, to_insert]
  return json.dumps(output)
  
  
  
f = open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/class_index.txt')
g = open('/home/jason/Kaggle/Facebook/Data/Training/NewParsedOutput/class_index_top10.txt', 'w')
counter = 0
for line in f:
  counter += 1
  g.write(mapper(line) + '\n')
  sys.stdout.write('\r' + str(counter))
  sys.stdout.flush()
  
g.close()
f.close
