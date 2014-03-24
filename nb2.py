"""	naive bayes implementation using an inverted index instead of forward, should be more efficien """


from __future__ import division
import csv, re, math, time, lxml.html
from operator import itemgetter
from collections import defaultdict


## there are 6,034,195 posts total

with open('stop_words.txt', 'r') as s:
	stop_words = s.read().splitlines()



f = open('/home/jason/Kaggle/Facebook/Data/train.csv', 'r')
csv_reader = csv.reader(f)
csv_reader.next()


def tokenizer(str):
	token_regex = r'[a-zA-Z]+'
	matched = re.findall(token_regex, str)
	
	tokens = [s.lower() for s in matched if (s.lower() not in stop_words and len(s) != 1)]
	
	return tokens
	
	
def create_histogram(tokens):
	""" creates a frequency distribution for the tokens """
	
	histogram = defaultdict(int)
	
	for token in tokens:
		histogram[token] += 1
		
	return histogram
	
	
	
def index(num_posts):
	
	index, tag_gram = {}, {}
	count = 0
	times = []
	for question in csv_reader:
		start = time.clock()
		tags, tokens = get_tokens_tags(question)
		count += 1
			
		histogram = create_histogram(tokens)
		
		for tag in tags:
			
			if tag not in tag_gram:
				tag_gram[tag] = (0,0)  # first number is the number of unique occurences of the tag, second number is number of tokens total in all posts with that tag
			tag_gram[tag] = (tag_gram[tag][0] + 1, tag_gram[tag][1])
			
			
			for token in histogram:
				if token not in index:
					index[token] = {'counts': (0,0), 'tags' : {}}
				index[token]['counts'] = (index[token]['counts'][0] + 1, index[token]['counts'][1]+ histogram[token]) # add all occurences of token
				
				if tag not in index[token]['tags']:
					index[token]['tags'][tag] = (0, 0)
				
				index[token]['tags'][tag] = (index[token]['tags'][tag][0] + 1, index[token]['tags'][tag][1] + histogram[token])
			
				tag_gram[tag] = (tag_gram[tag][0], tag_gram[tag][1] + histogram[token])	
		
			
				# we count up each occurence of a token, including multiple occurences.
		
		if count == num_posts:
			break
			
		end = time.clock()
		times.append(end-start)
		
	f.close()
	
	print 'Average execution:', sum(times) / len(times)
	
	return index, tag_gram
	

def mutual_information(index, tag_gram, tag, token, total_posts):
	""" Calculates the mutual information score for the term and the tag. """

	if token not in index:
		return -1 # not sure how to handle unseen terms just yet
	
	# e1 is the number of posts where tag and term occur together

	e1 = index[token]['tags'].get(tag, [1,1])[0]
	print e1

	# e2 is the number of posts where tag occurs and term doesn't
	e2 = tag_gram[tag][0] - e1
	print e2

	# e3 is the number of posts where tag doesn't occur and term does
	e3 = index[token]['counts'][0] - e1
	print e3
	
	# e4 is the number of posts where tag doesn't occur and term doesn't
	e4 = total_posts - e1 - e2 - e3
	print e4
	
	
	
	mi_score = \
	(e1 / total_posts) * math.log((total_posts * e1) / ((e1 + e3) * (e1 + e2)), 2) + \
	(e2 / total_posts) * math.log((total_posts * e2) / ((e2 + e4) * (e2 + e1)), 2) + \
	(e3 / total_posts) * math.log((total_posts * e3) / ((e3 + e1) * (e3 + e4)), 2) + \
	(e4 / total_posts) * math.log((total_posts * e4) / ((e4 + e2) * (e4 + e3)), 2)
	
	
	return mi_score
	

	
	 
	 
	
	
		
def get_tokens_tags(post):

	tags = post[3].split()
	
	# parse xhtml, and extract only text and filter out xhtml tags.
	body = lxml.html.fromstring((post[2].strip())).text_content()  
	
	# we will combine the title and body for now. MAY CONSIDER WEIGHTING TITLE MORE LATER.
	tokens = tokenizer(post[1]) + tokenizer(body)
	
	return tags, tokens
	
#	Need to know three things:
#	1. p(tag) = number of posts with tag / total number of posts
#	2. p(word) = number of posts with word / total number of words
#	3. p(word|tag) = number of posts with word and tag / number of posts with tag
	
	
def probability(tag, tokens, trained_tags, trained_tokens):

	tag_count = trained_tags[tag]['count']
	num_posts = sum([trained_tags[tag]['count'] for tag in trained_tags])
	p_tag = tag_count / num_posts
	
	unique_tokens = list(set(tokens))
	
	# token_probs = [trained_tokens.get(token, 1) / num_posts for token in unique_tokens]
	
	conditional_probs = []
	for token in unique_tokens:
	
		if token not in trained_tags[tag]['tokens']:
			numerator = 1
			denom = sum([b for (a,b) in trained_tags[tag]['tokens'].values()]) + len(trained_tokens)
		else:
			numerator = trained_tags[tag]['tokens'][token][1]
			denom = sum([b for (a,b) in trained_tags[tag]['tokens'].values()])
			
	conditional_probs.append(numerator / denom)
	
	# print p_tag
	# print conditional_probs
	# print token_probs

	
	# print '=================='
	
	
	#ln_token_probs = [math.log(p) for p in token_probs]
	
	ln_conditional_probs = [math.log(p) for p in conditional_probs]
	
	ln_P = math.log(p_tag) + sum(ln_conditional_probs) # - sum(ln_token_probs) 			(we don't need this for now, since it's the same for all tags)
	
	# print math.log(p_tag)
	# print ln_conditional_probs
	# print ln_token_probs
	
	# print ln_P
	
	
	
	return math.exp(ln_P)
	
def predict_tags(post, trained_tags, trained_tokens, num_posts):
	tokens = tokenizer(post[1]) + tokenizer(soup.text)
	tag_set = []
	for tag in trained_tags:
		p = probability(tag, tokens, trained_tags, trained_tokens, num_posts)
		tag_set.append((tag, p))
		
	sorted_tags = sorted(tag_set, key=itemgetter(1), reverse=True)
	
	return sorted_tags
	
	
	
	
def train_unique(num_posts):
	""" count up each occurence of a token,
	disregarding multiple occurences"""
	
	trained_tags, trained_tokens = {}, {}
	count = 0
	times = []
	for question in csv_reader:
		start = time.clock()
		tags, tokens = get_tokens_tags(question)
		
		count += 1
		
		unique_tokens = list(set(tokens))
		
		for tag in tags:
			if tag not in trained_tags:
				trained_tags[tag] = {'count': 1, 'tokens': {}}
			else:
				trained_tags[tag]['count'] += 1
				
			for token in unique_tokens:
				trained_tags[tag]['tokens'][token] = trained_tags[tag]['tokens'].get(token, 0) + 1
				
			# we count up each occurence of a token, disregarding multiple occurrences.
		
		# for token in unique_tokens:	
			# trained_tokens[token] = 1
		
		if count == num_posts:
			break
			
		end = time.clock()
		times.append(end-start)
		
	f.close()
	# print 'Runtime: ', str(time.clock() - start)
	print sum(times) / len(times)
	return trained_tags
	

	
	
