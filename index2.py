from __future__ import division
import re
import math
import time
import lxml.html
import pymongo
from operator import itemgetter
from collections import defaultdict


def mapper(self, line):
  parts = line.split('\t')
  post_id, title_tokens, body_tokens, tags = parts[0], parts[1].split(','), parts[2].split(','), parts[3].split(',')
  for token in list(set(title_tokens + body_tokens)):
	    yield (token, (1, tags))
  	    
