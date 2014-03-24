import csv

def tagcount(num_q):
	f = open('train.csv', 'r')
	csv_reader = csv.reader(f)
	tags = {}
	count = 0
	for question in csv_reader:
		for tag in question[3].split():
			tags[tag] = tags.get(tag, 0) + 1
		count += 1
		if count > num_q:
			print count
			break
			
	f.close()	
	return tags
			
	