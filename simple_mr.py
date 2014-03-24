from multiprocessing import Pool
from collections import defaultdict
import time, sys

""" Simple MapReduce implementation using Python's multiprocessing module to simulate Hadoop """

class SimpleMR(object):
	def __init__(self, mapper=None, reducer=None):
		self.Mapper = mapper
		self.Reducer = reducer
		
	def Partitioner(self, mappings):
		""" takes a LIST of mapper outputs as its input, so [
												[(key1, value1), (key2, value1), (key3, 1)],
												[(key4, value4), (key5, value5), (key6, value6)],
												....
												]
	
		and sorts them by the key. Returns dictionary with keys and list of values """
	
		output = defaultdict(list)
		
		for sublist in mappings:
			for t, c in sublist:
				sys.stdout.write('\r' + str(len(output)))
				sys.stdout.flush()
				output[t].append(c)
	
		return output
	
	
	
	def __call__(self, inputs, processes):
		
	
		""" creates processes and maps the Mapper to the inputs, combines the Mapper outputs with 
		Partitioner, and then maps the Reducers to the outputs of the Partitioner """
		
		pool = Pool(processes=processes)
		start = time.time()
		mapper_outputs = pool.map(self.Mapper, inputs)
		print 'Runtime', time.time() - start
		combined = self.Partitioner([mapping for mapping in mapper_outputs])
		print len(combined)
		reducer_outputs = pool.map(self.Reducer, combined.items())
		
		
		return reducer_outputs
		
		
