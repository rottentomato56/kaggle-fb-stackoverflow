""" first map reduce word count attempt """

import re, simple_mr, sys, time, os, operator
reload(simple_mr)
from collections import defaultdict

from multiprocessing import Pool

token_regex = r'\w+'

def mapper(textstr):

	""" yields a list of (token, 1) pairs """

	tokens = re.findall(token_regex, textstr)
	return [(token.lower(), 1) for token in tokens]
		
		

def partitioner(mappings):
	""" takes a LIST of mapper outputs (list of generators in fact) as its input, so [
											[(token1, 1), (token2, 1), (token3, 1)],
											[(token4, 1), (token5, 1), (token6, 1)],
											....
											]
	
	
	and sorts them by the token. Returns dictionary with values as list of 1's """
	
	token_counts = defaultdict(list)
	
	for sublist in mappings:
		for t, c in sublist:
			token_counts[t].append(c)
			
	return token_counts
	
	

def reducer(token_pair):

	""" takes a tuple of (token, [1, 1, ...]) and returns a tuple of (token, frequency) """
	return (token_pair[0], sum(token_pair[1]))



def load_inputs(input_folder, chunks):
	chunks = chunks - 1
	lines = []
	input_files =  [os.path.join(input_folder, filename) for filename in os.listdir(input_folder)]
	
	for input_file in input_files:
		with open(input_file, 'r') as f:
			lines += f.readlines()
			
	text_chunks = ['\n'.join(lines[i:i+len(lines) / chunks]) for i in xrange(0, len(lines), len(lines) / chunks)]
	
	return text_chunks
		
		
def run(inputs, processes):
	
	word_counter = simple_mr.SimpleMR(mapper=mapper, reducer=reducer)
	output = word_counter(inputs, processes)
	
	return output
	
	
	

def run_single(input_folder):
	""" to compare runtimes for non-MapReduce method """
	start = time.clock()
	input_files = [os.path.join(input_folder, filename) for filename in os.listdir(input_folder)]
	frequencies = defaultdict(int)
	
	for input_file in input_files:
		with open(input_file, 'r') as f:
			
			textstr = f.read()
		
		tokens = re.findall(token_regex, textstr)
		for token in tokens:
			frequencies[token.lower()] += 1
		
	print 'Non-MR runtime:', time.clock() - start
	return frequencies
	
	




def mapper2(input_file):

	""" yields a list of (token, 1) pairs """
	start2 = time.time()
	frequencies = defaultdict(int)
	with open(input_file) as f:
		textstr = f.read()
		
	tokens = re.findall(token_regex, textstr)
	
	for token in tokens:
		frequencies[token.lower()] += 1
		
	sorted_results = sorted(frequencies.items(), key=operator.itemgetter(1), reverse=True)
	print 'runtime', time.time() - start2
	return sorted_results[0]
	
	
	
if __name__ == '__main__':
	input_folder = sys.argv[1]
	processes = int(sys.argv[2])
	start = time.time()
	inputs = load_inputs(input_folder, chunks=processes)
	print len(inputs)
	run(inputs, processes)
	
	print 'total runtime', time.time() - start
	#run_single(input_folder)
	
	
	
#if __name__ == '__main__':
#	start = time.time()
#	input_folder = sys.argv[1]
#	processes = int(sys.argv[2])
#	
#	pool = Pool(processes)
#	
#	input_files =  [os.path.join(input_folder, filename) for filename in os.listdir(input_folder)]
#	
#	start2 = time.time()
#	
#	output = pool.map(mapper2, input_files)
#	print 'runtime', time.time() - start2
#	
#	
#	print 'Total runtime', time.time() - start
#	#run_single(input_folder)
#	
#	print output
#	
	
	
	
	
	
	
	
	
	
	
	
	
		
