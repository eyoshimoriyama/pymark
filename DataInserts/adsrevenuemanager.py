import sys
import os
import datetime
import itertools
import MySQLdb
from slackmanager import *
from datetime import timedelta
from psycopg2.extras import execute_values
from local_config import *
from DataSources.redshiftmanager import cursor
from DataSources.supersonicmanager import *
from DataSources.localyticsmanager import *

class AdsRevenueManager():
	
	def __init__(self): 
		self.table_name = 'ads_revenue_tdi'

	def updateAdsRevenueData(self, start_date, end_date):
		start_date = str(start_date)[:10]
		end_date = str(end_date)[:10]
		supersonic = SupersonicAPI('Two Dots')
		ads_rev_data = supersonic.getAdsRevenueData(start_date, end_date)
		self.deleteAdsRevenueData(start_date, end_date)
		self.insertAdsRevenueData(ads_rev_data)
		slack = SlackManager()
		slack.sendAdsRevenueUpdate(['#data-scripts'], start_date, end_date, self.rows_deleted, self.rows_inserted)

	def insertAdsRevenueData(self, ads_rev_data):
		page_size = len(ads_rev_data)
		insert_query = "INSERT INTO ads_revenue_tdi VALUES %s"
		execute_values(cursor, insert_query, ads_rev_data, page_size=page_size)
		statusmessage = cursor.statusmessage
		self.rows_inserted = statusmessage[9:]
		print statusmessage

	def deleteAdsRevenueData(self, start_date, end_date):
		cursor.execute(
		 """ DELETE 
			FROM ads_revenue_tdi 
			WHERE date(date) BETWEEN '%s' AND '%s' 
			AND ad_source != 'Zeptolab' """ %
			(start_date, end_date))
		statusmessage = cursor.statusmessage
		self.rows_deleted = statusmessage[7:]
		print statusmessage
