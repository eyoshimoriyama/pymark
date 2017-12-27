import csv
import itertools
import os
import sys
from querymanager import *
from csvmanager import *
from emailmanager import *
from datetime import datetime

class ReportManager():
	
	def __init__(self, source):
		self.source = source
		self.today = str(datetime.now())[:10]
		return

	def sendReport(self, recepients):
		
		# Query to get data for report
		qm = QueryManager(self.source)
		spend_data = qm.getSpendData()

		# Write data to CSV file
		csv = CSVManager()
		file_name = self.createFileName()
		csv.writeToCSV(spend_data, file_name)
		
		# Send CSV file via email
		em = EmailManager()
		subject = self.createSubject()
		body ='Report attached.'
		em.sendEmail(recepients=recepients, subject=subject, body=body, file_name=file_name)

	def createFileName(self):
		directory = os.path.dirname(os.path.realpath("__file__"))
		file_name = directory + '/EmailReports/Reports/dots_ua_spend_' + self.today + '_.csv'
		return file_name

	def createSubject(self):
		subject = "Dots UA Spend Report " + self.today
		return subject
