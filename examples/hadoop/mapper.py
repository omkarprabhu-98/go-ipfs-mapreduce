#!/usr/bin/python

import sys
from collections import defaultdict

word_count = defaultdict(int)


# dictionary with default value being another dictionary, with its own default value = 0
def data_default_dict():
	return defaultdict(int)


data = defaultdict(data_default_dict)

for line in sys.stdin:
	try:
		if len(line.strip()) <= 5:
			continue

		# Split input into doc_id and words
		(doc, words) = line.split(',', 1)
		words = words.split()

		word_count[doc] = len(words)
		for word in words:
			data[word][doc] += 1
	except Exception as e:
		pass

for (word, doc_dict) in data.items():
	for doc_id in doc_dict.keys():
		# Output word, doc_id, count of specified word in the document, count of all words in the document
		print('%s;%s;%s;%s' % (str(word), str(doc_id), str(data[word][doc_id]), str(word_count[doc_id])))
