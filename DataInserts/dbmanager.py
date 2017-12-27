import MySQLdb
import datetime
import sys
import os
import itertools
from datetime import timedelta
from collections import OrderedDict
from local_config import *
from DataSources.localyticsmanager import *
from DataSources.facebookmanager import *
from DataSources.tenjinmanager import *
from DataSources.tunemanager import *

class DBManager():
	
	def __init__ (self, destination):
		self.host = mysql_dbs[destination]['host']
		self.port = mysql_dbs[destination]['port']
		self.user = mysql_dbs[destination]['user']
		self.passwd = mysql_dbs[destination]['passwd']
		self.page_size = 5000

	def openMySQLConnection(self, db):
		self.db = MySQLdb.Connect(host=self.host, port=self.port, 
			user=self.user, passwd=self.passwd, db=db)
		self.cursor = self.db.cursor()
		self.db.set_character_set('utf8')
		self.cursor.execute('SET NAMES utf8;') 
		self.cursor.execute('SET CHARACTER SET utf8;')
		self.cursor.execute('SET character_set_connection=utf8;')

	def closeMySQLConnection(self):
		self.db.commit()
		self.db.close()

	def setDateRange(self, start_date, end_date):
		self.start_date = str(start_date)[:10]
		self.end_date = str(end_date)[:10]

	def dumpLocalyticsData(self, start_date, end_date):
		self.setDateRange(start_date, end_date)
		self.createLocalyticsInstances()
		self.dumpInstallData()
		self.dumpRevenueData()

	def createLocalyticsInstances(self):
		self.loc_instances = []
		for app, platforms in localytics_ids.items():
			for platform, app_id in platforms.items():
				self.loc_instances.append(LocalyticsAPI(app, platform, app_id))

	def dumpInstallData(self):
		for instance in self.loc_instances:
			install_data = instance.getInstallData(self.start_date, self.end_date)
			insert_query = self.createInsertQuery(install_data, table='installs')
			install_data = self.convertData(install_data)
			insert_query += """ ON DUPLICATE KEY 
				UPDATE installs = VALUES(installs) """
			self.insertData(insert_query, install_data, db='localytics')

	def dumpRevenueData(self):
		start_cohort = '2015-07-01' # Grab data for cohorts starting after purchase bug in June 2015
		for instance in self.loc_instances:
			revenue_data = instance.getRevenueData(start_cohort, self.start_date, self.end_date)
			insert_query = self.createInsertQuery(revenue_data, table='revenue')
			revenue_data = self.convertData(revenue_data)
			insert_query += """ ON DUPLICATE KEY
				UPDATE amount_usd = VALUES(amount_usd),
				occurrences = VALUES(occurrences),
				revenue = VALUES(revenue) """
			self.insertData(insert_query, revenue_data, db='localytics')

	def dumpSpendData(self, start_date, end_date):
		self.setDateRange(start_date, end_date)
		spend_accounts = [FacebookAPI(),
			TenjinAPI(ignore_networks=['Facebook'])]
		for account in spend_accounts:
			spend_data = account.getSpendData(self.start_date, self.end_date, breakdown='country')
			insert_query = self.createInsertQuery(spend_data, table='spending')
			spend_data = self.convertData(spend_data)
			insert_query += """ ON DUPLICATE KEY 
				UPDATE spend = VALUES(spend),
				clicks = VALUES(clicks),
				impressions = VALUES(impressions),
				unique_clicks = VALUES(unique_clicks),
				unique_impressions = VALUES(unique_impressions),
				installs = VALUES(installs) """
			self.insertData(insert_query, spend_data, db='spending')

	def dumpTuneData(self, start_date, end_date):
		self.setDateRange(start_date, end_date)
		tune = TuneAPI()
		tune_data = tune.getEventsData(start_date, end_date)
		insert_query = self.createInsertQuery(tune_data, table='tune_events')
		tune_data = self.convertData(tune_data)
		insert_query += """ ON DUPLICATE KEY 
				UPDATE mat_id = VALUES(mat_id) """
		self.insertData(insert_query, tune_data, db='localytics')

	def insertData(self, insert_query, data, db):
		start_page = 0
		total_pages = len(data) / self.page_size
		for page in range(1, total_pages+2):
			self.openMySQLConnection(db)
			insert_data = data[start_page:self.page_size*page]
			start_page += self.page_size
			self.cursor.executemany(insert_query, insert_data)
			self.closeMySQLConnection()

	def createInsertQuery(self, data, table):
		fields = data[0].keys()
		field_placeholders = ','.join(['%s'] * len(fields))
		insert_query = """ INSERT INTO """ + table + \
			str(tuple(fields)) + ' VALUES (' + field_placeholders + ')'
		insert_query = insert_query.replace("'", "")
		return insert_query

	def convertData(self, data):
		converted_data = []
		for row in data:
			converted_data.append(tuple(row.values()))
		return converted_data
