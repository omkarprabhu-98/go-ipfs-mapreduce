#!/usr/bin/python

import sys
import math
from collections import defaultdict


def data_default_dict():
	return defaultdict(int)


tf_count = defaultdict(data_default_dict)
tf_total = defaultdict(data_default_dict)
document_frequency = defaultdict(int)
document_set = set()

for line in sys.stdin:
	try:
		line = line.split(';')
		# Read word, doc_id, count of specified word in the document, count of all words in the document
		(word, doc, count, word_count) = (line[0], int(line[1]), int(line[2]), int(line[3]))

		tf_count[word][doc] += count
		tf_total[word][doc] += word_count
		document_frequency[word] += 1
		document_set.add(doc)
	except Exception as e:
		pass

# Calculate td-idf for all word, doc_id pairs
for (word, doc_dict) in tf_count.items():
	for doc_id in doc_dict.keys():
		tf = tf_count[word][doc_id] / float(tf_total[word][doc_id])
		idf = math.log(len(document_set) / float(document_frequency[word]))
		tf_idf = float(tf) * idf
		print('%s;%d;%f' % (word, int(doc_id), tf_idf))
