import MySQLdb
import datetime
import itertools
import numpy as np
import math
import matplotlib.pyplot as plt
from datetime import timedelta
from querymanager import *
from local_config import *


class CohortManager():

	def __init__(self, source, destination):
		self.today = datetime.datetime.now()
		self.query_manager = QueryManager(source)
		self.host = mysql_dbs[destination]['host']
		self.port = mysql_dbs[destination]['port']
		self.user = mysql_dbs[destination]['user']
		self.passwd = mysql_dbs[destination]['passwd']
		self.db = 'localytics'
		self.page_size = 5000

	def openMySQLConnection(self):
		self.db = MySQLdb.Connect(host=self.host, port=self.port, 
			user=self.user, passwd=self.passwd, db='localytics')
		self.cursor = self.db.cursor()
		self.db.set_character_set('utf8')
		self.cursor.execute('SET NAMES utf8;') 
		self.cursor.execute('SET CHARACTER SET utf8;')
		self.cursor.execute('SET character_set_connection=utf8;')

	def closeMySQLConnection(self):
		self.db.commit()
		self.db.close()

	def dumpCohortedCountryRev(self):
		self.cohort_type = 'Country'
		self.rev_type = 'IAP'
		install_data = self.query_manager.getCountryInstallData()
		revenue_data = self.query_manager.getCountryRevenueData()
		generic_data = self.query_manager.getGenericData()
		compiled_data = self.compileData(install_data, revenue_data, generic_data)
		self.insertData(compiled_data)

	def dumpCohortedAdsetRev(self):
		self.cohort_type = 'Adset'
		self.rev_type = 'IAP'
		install_data = self.query_manager.getAdsetInstallData()
		revenue_data = self.query_manager.getAdsetRevenueData()
		generic_data = self.query_manager.getGenericData()
		compiled_data = self.compileData(install_data, revenue_data, generic_data)
		self.insertData(compiled_data)

	def compileData(self, install_data, revenue_data, generic_data):
		compiled_data = []
		today = datetime.datetime.now()
		generic_arpu_list = self.getGenericArpuList(generic_data)
		for row in install_data:
			app = row['app']
			install_date = row['install_date']
			total_age = (today - install_date).days
			platform = row['platform']
			country = row['country']
			installs = float(row['installs'])
			source = row['source']
			row['cohort_type'] = self.cohort_type
			row['rev_type'] = self.rev_type
			try:
				acquire_source = row['acquire_source']
			except KeyError:
				acquire_source = None
			try: 
				campaign = row['campaign']
			except KeyError:
				campaign = None
			try:
				adset = row['adset']
			except KeyError:
				adset = None
			arpu_list = self.getArpuList(revenue_data, app, install_date, total_age,
				platform, country, acquire_source, source, campaign, adset, installs)
			row['total_revenue'] = arpu_list[-1] * installs
			if total_age < 60:
				rev_dic = self.projectRevGenericModel(generic_arpu_list, arpu_list, installs, total_age)
			if total_age >= 60:
				rev_dic = self.projectRevLogModel(arpu_list, installs)
			row.update(rev_dic)
			compiled_data.append(row)
		return compiled_data

	def compileAdsRevData(self, ads_impression_data, cpm_data):
		for row in ads_impression_data:
			install_date = row['install_date']
			date = row['date']
			platform = row['platform']
			country = row['country']
			impressions = float(row['impressions'])
			for next in cpm_data:
				if next['date'] == date and next['platform'] == platform and next['country'] == country:
					row['revenue'] = float(next['cpm']) * impressions
					break
		return ads_impression_data

	def getArpuList(self, revenue_data, app, install_date,  total_age,\
		platform,  country, acquire_source, source, campaign, adset, installs):
		global arpu_list, install_age, prev_arpu, total_revenue
		arpu_list = []
		install_age = 0
		prev_arpu = 0
		total_revenue = 0
		if self.cohort_type == 'Country':
			for row in revenue_data:
				if row['app'] == app and row['install_date'] == install_date and row['platform'] == platform \
					and row['country'] == country and row['source'] == source:
						self.appendArpu(row, installs)
		if self.cohort_type == 'Acquire Source':
			for row in revenue_data:
				if row['app'] == app and row['install_date'] == install_date and row['acquire_source'] == acquire_source \
				and row['platform'] == platform and row['country'] == country:
					self.appendArpu(row, installs)
		if self.cohort_type == 'Adset':
			for row in revenue_data:
				if row['app'] == app and row['install_date'] == install_date and row['platform'] == platform \
				and row['country'] == country and row['campaign'] == campaign and row['adset'] == adset:
						self.appendArpu(row, installs)
		for _ in range(install_age, total_age+1):
			arpu_list.append(prev_arpu)
		return arpu_list

	def appendArpu(self, row, installs):
		global arpu_list, install_age, prev_arpu, total_revenue
		while install_age < row['install_age']:
			arpu_list.append(prev_arpu)
			install_age += 1
		try: 
			total_revenue += float(row['revenue'])
		except TypeError:
			pass
		arpu = total_revenue /  installs
		arpu_list.append(arpu)
		prev_arpu = arpu
		install_age += 1

	def getGenericArpuList(self, generic_data, cohort_length=60):
		total_revenue = 0
		generic_arpu_list = []
		for row in generic_data:
			try:
				total_revenue += float(row['revenue'])
			except TypeError:
				pass
			installs = float(row['installs'])
			arpu = total_revenue / installs
			generic_arpu_list.append(arpu)
		generic_arpu_list = generic_arpu_list[:cohort_length]
		log_days = [np.log(i) for i in range(1, len(generic_arpu_list)+1)]
		w = [math.pow(i,50) for i in range(1,len(generic_arpu_list)+1)]	
		a = np.polyfit(log_days, generic_arpu_list, 1, w=w)[0]
		b = np.polyfit(log_days, generic_arpu_list, 1, w=w)[1]
		for day in range(cohort_length, 300):
			proj_arpu = (a * np.log(day)) + b
			generic_arpu_list.append(proj_arpu)
		return generic_arpu_list

	def projectRevGenericModel(self, generic_arpu_list, arpu_list, installs, total_age): 
		arpu = arpu_list[-2]
		rev_dic = {}
		rev_days = [1, 2, 3, 7, 14, 30, 45, 90, 180, 270]
		for day in rev_days:
			if generic_arpu_list[total_age-1] != 0:
				arpu = arpu_list[-1]
				ratio = generic_arpu_list[day] / generic_arpu_list[total_age-1]
			else:
				ratio = generic_arpu_list[day] / generic_arpu_list[total_age]
			try:
				obs_arpu = arpu_list[day]
			except IndexError:
				obs_arpu = ratio * arpu
			rev = round(obs_arpu * installs, 3)
			name = 'd' + str(day) + '_rev'	
			rev_dic[name] = rev
			if rev_days == 270 and len(arpu_list) <= 35:
				rev_dic['d270_rev'] = rev_dic['d270_rev'] * 1.7
		return rev_dic

	def projectRevLogModel(self, arpu_list, installs):
		log_days = [np.log(i) for i in range(1, len(arpu_list)+1)]
		w = [math.pow(i, 50) for i in range(1, len(arpu_list)+1)]	
		a = np.polyfit(log_days, arpu_list, 1, w=w)[0]
		b = np.polyfit(log_days, arpu_list, 1, w=w)[1]
		rev_dic = {}
		rev_days = [1, 2, 3, 7, 14, 30, 45, 90, 180, 270]
		for day in rev_days:
			try:
				rev = arpu_list[day] * installs
			except IndexError:
				rev = ((a * np.log(day)) + b) * installs
			rev = round(rev, 3)
			name = 'd' + str(day) + '_rev'	
			rev_dic[name] = rev
		return rev_dic

	def insertData(self, data):
		columns = self.convertDatatoColumns(data)
		data = self.convertDatatoTuples(data)
		place_holders = self.createPlaceHolders(data)
		insert_statement = "INSERT INTO cohorted_revenue " +\
			columns + " VALUES " + place_holders
		start_page = 0
		total_pages = len(data) / self.page_size
		self.deleteRows()
		for page in range(1, total_pages+2):
			self.openMySQLConnection()
			insert_data = data[start_page:self.page_size*page]
			start_page += self.page_size
			self.cursor.executemany(insert_statement, insert_data)
			self.closeMySQLConnection()

	def deleteRows(self):
		self.openMySQLConnection()
		self.cursor.execute(
			""" DELETE FROM cohorted_revenue
			WHERE cohort_type = '%s'
			AND rev_type = '%s' """ %
			(self.cohort_type, self.rev_type))
		self.closeMySQLConnection()

	def convertDatatoColumns(self, data):
		columns = "("
		for column in data[0].keys():
			columns += column
			columns += ","
		return columns[:-1] + ")"

	def convertDatatoTuples(self, data):
		data_tuples = []
		for row in data:
			data_tuples.append(row.values())
		return data_tuples

	def createPlaceHolders(self, data):
		place_holders = "("
		for _ in range(0, len(data[0])):
			place_holders += "%s,"
		return place_holders[:-1] + ")"
