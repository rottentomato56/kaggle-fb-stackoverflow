""" 

Merge splitfiles together by first sorting the individual files, then merging them together

"""

import csv, sys, os
from operator import itemgetter

def sortfile(filename):
	f = open(filename, 'r')
	csv_reader = csv.reader(f)
	posts = [post for post in csv_reader]
	f.close()
	sorted_posts = sorted(posts, key=itemgetter(1))
	
	
	newfile = filename.split('.')[0] + '_sorted.csv'
	with open(newfile, 'w') as g:
		csv_writer = csv.writer(g)
		for post in sorted_posts:
			csv_writer.writerow(post)			
	return 

	
def mergefiles(filenames):

	# initialize the lists and file readers
	file_readers = []
	
	for filename in filenames:
		f = open(filename)
		file_readers.append(csv.reader(f))
			
	post_queue = []
	
	for i in range(len(file_readers)):
		post_queue.append(file_readers[i].next())
	

	j = 1 # counter for file
	n = 0 # counter for posts seen
			
	g = open('Training/train_nodupes_' + str(j) + '.csv' , 'w')
	csv_writer = csv.writer(g)	

	dupes = 0
	previous_post = ['1', '2', '3', '4']  # initialize the previous post

	while len(file_readers) > 0:
		next_post, min_index = post_queue[0], 0 # initialize the min post
		
		for i in range(len(file_readers)):
			if not post_queue[i]:
				continue
		
			if post_queue[i][1] < next_post[1]:
				min_index = i
				next_post = post_queue[min_index]
				
		next_reader = file_readers[min_index]
		
		while next_post[1] == previous_post[1]:
			dupes += 1
			try:
				post_queue[min_index] = next_reader.next()
			except:
				post_queue.pop(min_index)
				file_readers.pop(min_index)
					
			if not post_queue:
				break
				
			next_post, min_index = post_queue[0], 0
			
			for i in range(len(file_readers)):
				if not post_queue[i]:
					continue
					
				if post_queue[i][1] < next_post[1]:
					min_index = i
					next_post = post_queue[min_index]
					
		csv_writer.writerow(next_post)
		n += 1
		sys.stdout.write('\rLines: ' + str(n) + '\tFiles: ' + str(j) + '\tDupes:' + str(dupes))
		sys.stdout.flush()
		
		
		if not file_readers:
			break
			
		next_reader = file_readers[min_index]
		
		try:
			post_queue[min_index] = next_reader.next()  ### arrrghghhghgh forgot to replace the min with the next one in the file
		except:
			post_queue.pop(min_index)
			file_readers.pop(min_index)
			if not file_readers:
				break
			
		
			
		previous_post = next_post
	
		

		if n >= 500000:
			j += 1
			n = 0
			g.close()
			g = open('Training/train_nodupes_' + str(j) + '.csv' , 'w')
			csv_writer = csv.writer(g)
			
	print 'Dupes', str(dupes)
		
if __name__ == '__main__':
	input_folder = sys.argv[1]
	inputs = [os.path.join(input_folder, filename) for filename in os.listdir(input_folder)]
	
#	for file_input in inputs:
#		sortfile(file_input)

	mergefiles(inputs)
