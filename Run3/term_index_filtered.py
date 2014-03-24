import json
import sys
import os
from collections import defaultdict
from operator import add
from mrjob.job import MRJob

text_term_counts = {}
code_term_counts = {}

outfile = open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/Mid20/classindex_10_filter.txt', 'w')

with open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/text_term_counts.txt') as g:
    for line in g:
      term = line.strip().split('\t')[0]
      count = int(line.strip().split('\t')[1])
      text_term_counts[term] = count
      
with open('/home/jason/Kaggle/Facebook/Data/Training/Parsed3/code_term_counts.txt') as g:
    for line in g:
      term = line.strip().split('\t')[0]
      count = int(line.strip().split('\t')[1])
      code_term_counts[term] = count
      
        
class TermIndex(MRJob):
  def mapper(self, _, line):
  
    self.increment_counter('classes', 'count', 1)
    
    text_term_counts = defaultdict(int)
    code_term_counts = defaultdict(int)

    class_vector = json.loads(line.strip())
    tag = class_vector['tag']
   
    text_terms = class_vector['text_terms']
    code_terms = class_vector['code_terms']
   
    
    for textterm_tuple in text_terms:
      term = textterm_tuple[0]
      inclass_post_frequency = textterm_tuple[1]
      inclass_term_frequency = textterm_tuple[2]
      mi_score = textterm_tuple[3]
      yield(term, ('text', tag, mi_score, inclass_post_frequency, inclass_term_frequency))
      
    for codeterm_tuple in code_terms:
      term = codeterm_tuple[0]
      inclass_post_frequency = textterm_tuple[1]
      inclass_term_frequency = textterm_tuple[2]
      mi_score = textterm_tuple[3]
      yield(term, ('code', tag, mi_score, inclass_post_frequency, inclass_term_frequency))
      
  
  def reducer(self, term, values):
  
    self.increment_counter('unique terms', 'total_count', 1)
    
    posting = defaultdict(list)
    for value in values:
      if value[0] == 'text':
        posting['text'].append(value[1:])
      else:
        posting['code'].append(value[1:])
    
    total_text_pf = text_term_counts.get(term, 0)
    total_code_pf = code_term_counts.get(term, 0) 
    
    yield(term, {
                  'code_post_count' : total_code_pf,
                  'text_post_count' : total_text_pf,
                  'text_tags' : posting.get('text', []),
                  'code_tags' : posting.get('code', [])
                 }
      
if __name__ == '__main__':
  TermIndex.run()


