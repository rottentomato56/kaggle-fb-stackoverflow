#	naive bayes implementation
from __future__ import division
import csv, re, math, time, lxml.html
from operator import itemgetter


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
	
	
def train(num_posts):
	
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
				
			for token in tokens:
				if token not in trained_tokens:
					trained_tokens[token] = (1,1)
				else:
					trained_tokens[token] = (trained_tokens[token][0], trained_tokens[token][1] + 1)
					
					
				if token not in trained_tags[tag]['tokens']:
					trained_tags[tag]['tokens'][token] = (1, 1) # two counts, one is the unique count, the second is the overall frequency
				else:
					trained_tags[tag]['tokens'][token] = (trained_tags[tag]['tokens'][token][0], trained_tags[tag]['tokens'][token][1] + 1)
				
			# we count up each occurence of a token, including multiple occurences.
		
		for token in unique_tokens:	
			trained_tags[tag]['tokens'][token] = (trained_tags[tag]['tokens'][token][0] + 1, trained_tags[tag]['tokens'][token][1])
			trained_tokens[token] = (trained_tokens[token][0] + 1, trained_tokens[token][1])
		
		if count == num_posts:
			break
			
		end = time.clock()
		times.append(end-start)
		
	f.close()
	# print 'Runtime: ', str(time.clock() - start)
	print sum(times) / len(times)
	return trained_tags, trained_tokens
	
	
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
	soup = BeautifulSoup(post[2])
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
	

	
def mutual_information(trained_tags, tag, term):
	""" Calculates the mutual information score for the term and the tag. Given that the max occurence of a tag
	doesn't surpass 5% or so (C#), we can just use the total number of unique tags as one of the denominators"""
	
	# e1 is the number of posts where tag and term occur together
	
	e1 = trained_tags[tag]['tokens'][term][0]
	
	# e2 is the number of posts where tag occurs but term doesn't
	
	e2 = total_posts - trained_tags[tag]['count']
