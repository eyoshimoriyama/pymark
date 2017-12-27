import csv
import itertools
import os
import sys

class CSVManager():
	
	def __init__(self):
		return

	def writeToCSV(self, data, file_name):
		try:
			keys = data[0].keys()
		except IndexError:
			return
		with open(file_name, 'wb') as output:
			dict_writer = csv.DictWriter(output, keys)
			dict_writer.writeheader()
			dict_writer.writerows(data)
