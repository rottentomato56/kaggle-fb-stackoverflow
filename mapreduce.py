# first map reduce attempt

import re
from collections import defaultdict

token_regex = r'\w+'

def mapper(textstr):

	""" yields a list of (token, 1) pairs """
	tokens = re.findall(token_regex, textstr)
	
	
	for token in tokens:
		yield (token.lower(), 1)
		
		

def partitioner(mappings):
	""" takes a LIST of mapper outputs (list of generators in fact) as its input, so [
											[(token1, 1), (token2, 1), (token3, 1)],
											[(token4, 1), (token5, 1), (token6, 1)],
											....
											]
	
	
	and sorts them by the token. Returns dictionary with values as list of 1's
	

	"""
	token_counts = defaultdict(list)
	for sublist in mappings:
		for t, c in sublist:
			token_counts[t].append(c)
			
	return token_counts
	
	
	
s1 = 'hello there jason my name is ron'
s2 = 'the big dog chased the cat down to the river.'
s3 = 'The FAT MAN ATE ALL THE SANDWICHES FROM THE DOG HOUSE'

sentences = [s1, s2, s3]

token_counts = partitioner([mapper(s) for s in sentences])
print token_counts

def reducer(token_pair):

	""" takes a tuple of (token, [1, 1, ...]) and returns a tuple of (token, frequency) """
	return (token_pair[0], sum(token_pair[1]))
	
	
for token_pair in token_counts.iteritems():
	print reducer(token_pair)
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
		
