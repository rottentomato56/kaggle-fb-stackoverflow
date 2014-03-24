""" 

MapReduce word count for SO posts """

import re, simple_mr, sys, time, os, csv, lxml.html
reload(simple_mr)
from collections import defaultdict


token_regex = r'[a-zA-Z]+'  # only match character strings as words

def mapper(filename):

	""" yields a list of (token, count) pairs """
	i = 0
	# parse xhtml, and extract only text and filter out xhtml tags.
	f = open(filename, 'r')
	csv_reader = csv.reader(f)
	
	token_counts = defaultdict(int)
	for post in csv_reader:
		title_html = post[1].strip()
		if title_html:
			try:
				title = lxml.html.fromstring(title_html).text_content()
			except:
				title = ''
		else:
			title = ''
		
		body_html = post[2].strip()
		if body_html:
			try:
				body = lxml.html.fromstring(body_html).text_content()
			except:
				body = ''
		else:
			body = ''
			
		textstr = title + ' ' + body
		
		tokens = re.findall(token_regex, textstr)
		
		for token in tokens:
			
			token_counts[token.lower()] += 1
			
		i += 1	
		
		sys.stdout.write('\r' + str(i))
		sys.stdout.flush()	
	f.close()
	
	
	return token_counts.items()
		
		

def partitioner(mappings):
	""" takes a LIST of mapper outputs (list of generators in fact) as its input, so [
											[(token1, 1), (token2, 1), (token3, 1)],
											[(token4, 1), (token5, 1), (token6, 1)],
											....
											]
	
	
	and sorts them by the token. Returns dictionary with values as list of individual counts """
	
	token_counts = defaultdict(list)
	
	for sublist in mappings:
		for t, c in sublist:
			token_counts[t].append(c)
			
	return token_counts
	
	
def reducer(token_pair):
	""" takes a tuple of (token, [1, 1, ...]) and returns a tuple of (token, frequency) """
	return (token_pair[0], sum(token_pair[1]))
		
		
def run(inputs, processes):
	
	word_counter = simple_mr.SimpleMR(mapper=mapper, reducer=reducer)
	output = word_counter(inputs, processes)
	
	return output

	

if __name__ == '__main__':
	input_folder = sys.argv[1]
	input_files = [os.path.join(input_folder, input_file) for input_file in os.listdir(input_folder)]
	
	print input_files

	
	processes = int(sys.argv[2])
	start = time.time()

	run(input_files, processes)
	
	print 'total runtime', time.time() - start
	
	
	
	
	
	
	
	
	
	
	
	
	
		
