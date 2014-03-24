""" 

Splits training data into separate files, each one 500,000 rows (last will be a little more or less). 

Also removes duplicates by checking if the post already was processed 

"""

import csv, collections, sys

f = open('/home/jason/Kaggle/Facebook/Data/Testing/Test.csv', 'r')
csv_reader = csv.reader(f)

total_lines = 0

headers = csv_reader.next() # first line has header fields


i = 1 # counter for the number of files
csv_file = open('/home/jason/Kaggle/Facebook/Data/Testing/test_' + str(i) + '.csv', 'w')
csv_writer = csv.writer(csv_file)

j = 0 # counter for lines

for line in csv_reader:
	total_lines += 1
	
		
	sys.stdout.write('\rLines: ' + str(total_lines))
	sys.stdout.flush()


	csv_writer.writerow(line)
	j += 1
	
	if j >= 500000:
		i += 1
		j = 0

		csv_file.close()
		
		csv_file = open('/home/jason/Kaggle/Facebook/Data/Testing/test_' + str(i) + '.csv', 'w')
		csv_writer = csv.writer(csv_file)
			
csv_file.close()
f.close()
