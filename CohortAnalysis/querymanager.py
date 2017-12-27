import MySQLdb
import datetime
import itertools
from datetime import timedelta
from local_config import *

class QueryManager():

	def __init__(self, source):
		self.host = mysql_dbs[source]['host']
		self.port = mysql_dbs[source]['port']
		self.user = mysql_dbs[source]['user']
		self.passwd = mysql_dbs[source]['passwd']
		self.db = 'localytics'
		self.openMySQLConnection()

	def openMySQLConnection(self):
		self.db = MySQLdb.Connect(host=self.host, port=self.port, 
			user=self.user, passwd=self.passwd, db=self.db)
		self.cursor = self.db.cursor()

	def closeMySQLConnection(self):
		self.db.commit()
		self.db.close()

	def getCountryInstallData(self):
		self.cursor.execute(
		"""SELECT app, install_date, platform, source,
			CASE WHEN campaign like '%WW%' AND country != 'us' THEN 'ww' ELSE country END country,
			SUM(installs) as installs
			FROM installs
			WHERE install_date < CURRENT_DATE
			AND ((source = 'Organic' and country = 'us') or source = 'Paid')
			GROUP BY app, install_date, platform, source, 5
			HAVING installs > 10 """)
		column_names = [col[0] for col in self.cursor.description]
		install_data = [dict(itertools.izip(column_names, row))  
						for row in self.cursor.fetchall()]
		return install_data

	def getCountryRevenueData(self):
		self.cursor.execute(
		""" SELECT app, install_date, date, DATEDIFF(date, install_date) as install_age, platform, source,
			CASE WHEN campaign like '%WW%' AND country != 'us' THEN 'ww' ELSE country END country,
			SUM(CASE WHEN app = 'Two Dots' and date BETWEEN '2016-12-23' and '2016-12-25' and amount_usd != .99 
				THEN revenue * .5 
				WHEN app = 'Dots & Co' and date between '2016-12-25' and '2016-12-27' and amount_usd != .99
				THEN revenue * .6
				ELSE revenue END) * .7 as revenue
			FROM localytics.revenue
			WHERE install_date < CURRENT_DATE
			AND ((source = 'Organic' and country = 'us') or source = 'Paid')
			GROUP BY app, install_date, date, platform, source, 7 """)
		column_names = [col[0] for col in self.cursor.description]
		revenue_data = [dict(itertools.izip(column_names, row))  
						for row in self.cursor.fetchall()]
		return revenue_data

	def getAdsetInstallData(self):
		self.cursor.execute(
		""" SELECT app, source, install_date, platform,
			CASE WHEN campaign LIKE '%WW%' THEN 'ww' ELSE country END country,
			CASE WHEN acquire_source in ('Facebook', 'Instagram', 'Facebook-External') THEN 'Facebook' ELSE acquire_source END acquire_source,
			campaign, adset,
			SUM(installs) as installs
			FROM localytics.installs
			WHERE install_date >= DATE_SUB(CURRENT_DATE, INTERVAL 5 MONTH)
			AND install_date < CURRENT_DATE
			AND source = 'Paid'
			GROUP BY app, install_date, platform, 5, 6, campaign, adset 
			HAVING installs >= 5 """)
		column_names = [col[0] for col in self.cursor.description]
		install_data = [dict(itertools.izip(column_names, row))  
						for row in self.cursor.fetchall()]
		return install_data

	def getAdsetRevenueData(self):
		self.cursor.execute(
		""" SELECT app, source, install_date, date, DATEDIFF(date, install_date) as install_age, 
			platform,
			CASE WHEN campaign LIKE '%WW%' THEN 'ww' ELSE country END country,
			CASE WHEN acquire_source in ('Facebook', 'Instagram', 'Facebook-External') THEN 'Facebook' ELSE acquire_source END acquire_source,
			campaign, adset,
			SUM(CASE WHEN app = 'Two Dots' and date BETWEEN '2016-12-23' and '2016-12-25' and amount_usd != .99 
				THEN revenue * .5 
				WHEN app = 'Dots & Co' and date between '2016-12-25' and '2016-12-27' and amount_usd != .99
				THEN revenue * .6
				ELSE revenue END) * .7 as revenue
			FROM localytics.revenue
			WHERE install_date >= DATE_SUB(CURRENT_DATE, INTERVAL 5 MONTH)
			AND install_date < CURRENT_DATE
			AND source = 'Paid'
			GROUP BY app, install_date, date, platform, 7, 8, campaign, adset """)
		column_names = [col[0] for col in self.cursor.description]
		revenue_data = [dict(itertools.izip(column_names, row))  
						for row in self.cursor.fetchall()]
		return revenue_data

	def getGenericData(self, cohort_length=60, lookback_days=100):
		today = datetime.datetime.now()
		start_date = str(today - timedelta(days=lookback_days))[:10]
		end_date = str(today - timedelta(days=(lookback_days-cohort_length)))[:10]
		self.cursor.execute(
		""" SELECT rev.install_age, rev.revenue, installs.installs
			FROM (SELECT sum(installs) AS installs
			FROM localytics.installs
			WHERE install_date BETWEEN '%s' AND '%s'
			AND platform = 'iOS'
			AND country in ('us', 'gb', 'au', 'jp')) AS installs
			JOIN (SELECT DATEDIFF(date, install_date) AS install_age, sum(revenue)*.7 AS revenue
				FROM revenue
				WHERE date >= install_date
				AND install_date BETWEEN '%s' AND '%s'
				AND platform = 'iOS'
				AND country in ('us', 'gb', 'au', 'jp')
				GROUP BY 1) AS rev """ 
			% (start_date, end_date, start_date, end_date))
		column_names = [col[0] for col in self.cursor.description]
		generic_data = [dict(itertools.izip(column_names, row))  
						for row in self.cursor.fetchall()]
		return generic_data
