# example:

# predictions: php python
# actual: sql python django

# true positives = 1 (python)
# false positives = 1 (php)
# false negatives = 2 (sql django)

from __future__ import division

def F1_score(predict_list, actual_list):
	true_positives = 0
	false_positives = 0
	false negatives = 0
	for prediction in predict_list:
		if prediction in actual_list:
			true_positives += 1
		else:
			false_positives += 1
	false_negatives = len(actual_list) - true_positives
		
	precision = true_positives / (true_positives + false_positives)
	recall = true_positives / (true_positives + false_negatives) # this is really just the length of actual_list
	return 	2 * precision * recall / (precision + recall)