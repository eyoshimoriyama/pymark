import MySQLdb
import datetime
import itertools
from local_config import *
from datetime import timedelta
from collections import OrderedDict

class QueryManager():

	def __init__(self, source):
		self.host = mysql_dbs[source]['host']
		self.port = mysql_dbs[source]['port']
		self.user = mysql_dbs[source]['user']
		self.passwd = mysql_dbs[source]['passwd']
		self.openMySQLConnection()

	def openMySQLConnection(self):
		self.db = MySQLdb.Connect(host=self.host, port=self.port, 
			user=self.user, passwd=self.passwd)
		self.cursor = self.db.cursor()

	def getSpendData(self):
		self.cursor.execute(
		"""SELECT date(date) as date, 
			ROUND(SUM(CASE WHEN app = 'Two Dots' and platform = 'iOS' THEN spend else 0 END), 2) as twodots_ios_spend,
			ROUND(SUM(CASE WHEN app = 'Two Dots' and platform = 'Android' THEN spend else 0 END), 2) as twodots_android_spend,
			ROUND(SUM(CASE WHEN app = 'Two Dots' and platform = 'iOS' THEN spend else 0 END) /
				SUM(CASE WHEN app = 'Two Dots' and platform = 'iOS' THEN installs else 0 END), 2) as twodots_ios_cpi,
			ROUND(SUM(CASE WHEN app = 'Two Dots' and platform = 'Android' THEN spend else 0 END) /
				SUM(CASE WHEN app = 'Two Dots' and platform = 'Android' THEN installs else 0 END), 2) as twodots_android_cpi,
			ROUND(SUM(CASE WHEN app = 'Dots & Co' and platform = 'iOS' THEN spend else 0 END), 2) as dac_ios_spend,
			ROUND(SUM(CASE WHEN app = 'Dots & Co' and platform = 'Android' THEN spend else 0 END), 2) as dac_android_spend,
			ROUND(SUM(CASE WHEN app = 'Dots & Co' and platform = 'iOS' THEN spend else 0 END) /
				SUM(CASE WHEN app = 'Dots & Co' and platform = 'iOS' THEN installs else 0 END), 2) as dac_ios_cpi,
			ROUND(SUM(CASE WHEN app = 'Dots & Co' and platform = 'Android' THEN spend else 0 END) / 
				SUM(CASE WHEN app = 'Dots & Co' and platform = 'Android' THEN installs else 0 END), 2) as dac_android_cpi,
			ROUND(SUM(CASE WHEN app = 'Wilds' and platform = 'iOS' THEN spend else 0 END), 2) as wilds_ios_spend,
			ROUND(SUM(CASE WHEN app = 'Wilds' and platform = 'Android' THEN spend else 0 END), 2) as wilds_android_spend,
			ROUND(SUM(CASE WHEN app = 'Wilds' and platform = 'iOS' THEN spend else 0 END) /
				SUM(CASE WHEN app = 'Wilds' and platform = 'iOS' THEN installs else 0 END), 2) as wilds_ios_cpi,
			ROUND(SUM(CASE WHEN app = 'Wilds' and platform = 'Android' THEN spend else 0 END) / 
				SUM(CASE WHEN app = 'Wilds' and platform = 'Android' THEN installs else 0 END), 2) as wilds_android_cpi
			FROM spending.spending
			WHERE marketing_type not in ('Brand')
			AND date >= '2017-01-01'
			GROUP BY 1	""" )
		column_names = [col[0] for col in self.cursor.description]
		spend_data = [dict(itertools.izip(column_names, row))  
					for row in self.cursor.fetchall()]
		sort_order = ['date', 'twodots_ios_spend', 'twodots_android_spend', 'twodots_ios_cpi', 'twodots_android_cpi', \
			'dac_ios_spend', 'dac_android_spend', 'dac_ios_cpi', 'dac_android_cpi', 'wilds_ios_spend', 'wilds_android_spend',
			'wilds_ios_cpi', 'wilds_android_cpi']
		ordered_data = [OrderedDict(sorted(item.iteritems(), key=lambda (k, v): sort_order.index(k))) for item in spend_data]
		return ordered_data
