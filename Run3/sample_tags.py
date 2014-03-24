import sys, os, json
from operator import itemgetter
from collections import defaultdict

f = open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/tag_counts.txt')

tag_counts = {}

for line in f:
  tag = line.strip().split()[0]
  count = int(line.strip().split()[1])
  
  tag_counts[tag] = count
  
f.close()


top1000_tags = sorted(tag_counts.items(), key=itemgetter(1), reverse=True)[:1000]


g = open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/sample_training.txt', 'w')
term_counts = {}
tag_counts2 = defaultdict(int)
      
input_file_folder = '/home/jason/Kaggle/Facebook/Data/Training/Parsed3/Training'
file_list = os.listdir(input_file_folder)
input_file_list = [os.path.join(input_file_folder, f) for f in file_list]
counter = 0
for train_file in input_file_list:
  f = open(train_file)
  for line in f:
    temp_term_count = defaultdict(int)
    post = json.loads(line.strip())
    actual_tags = post['tags']
    
    for tag in actual_tags:
      if tag in top1000_tags: 
        g.write(line)
        break

    counter += 1
    
    sys.stdout.write('\r' + str(counter)+ '\t' + str(len(tag_counts)) + '\t' + str(len(term_counts)))
    sys.stdout.flush()
    
g.close()
      
