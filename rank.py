#!/usr/bin/python

import sys
import json
import math

USAGE = "rank.py <config-json> <file-path>"

WISHFUL_THINKING_EXPONENT = 0.1

class Ranker:
	def __init__(self, config):
		self._weight = json.load(open(config, 'r'))

	def rank(self, path):
		"""
		How does it work?
		It scans the file for positions of words, and if a word appears - 
		the distance (in words, 1+) from the other words is logged. 
		When the scan is over, the rank is calculated based on the distances.
		"""
		words = self._weight.keys()
		last_position = dict(zip(words, [None] * len(words)))
		distances = dict(zip(words, [dict(last_position) for x in range(len(words))]))

		# scan for words in the file, store distance between words
		counter = 0
		for line in open(path, 'r'):
			try:
				tokens = line.lower().split()
				for token in tokens:
					for word in words:
						if token == word:
							for reference in words:
								if last_position[reference] is not None:
									distances[word][reference] = counter - last_position[reference]
							last_position[word] = counter
					counter += 1
			except InvalidArgument:
				pass

		# calculate the final score
		score = 0.0
		max_score = 1.0
		for word in words:
			for reference in words:
				distance = distances[word][reference]
				if distance:
					score += self._weight[word] * self._weight[reference] / distance
					max_score += self._weight[word] * self._weight[reference]
		return 100 * math.pow(float(score) / max_score, WISHFUL_THINKING_EXPONENT)

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print USAGE
	else:
		print Ranker(sys.argv[1]).rank(sys.argv[2])
