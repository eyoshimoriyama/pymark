import requests
import sys
import MySQLdb
import datetime
import logging
from datetime import timedelta
from local_config import *
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TuneAPI():

	def __init__(self):
		self.api_key = tune_api_key
		self.ad_networks =  ['SupersonicAds', 'Applovin', 'Tapjoy', 'youAPPi', 'AdColony']#['AppLift'] #'SupersonicAds', 'Applovin', 'Tapjoy', 'youAPPi', 'AdColony']

	def createUrl(self):
		self.url = 'https://api.mobileapptracking.com/v3/logs/advertisers/19738/ENDPOINT?' + \
			'api_key=' + self.api_key + \
			'&start_date=START_DATE' + \
			'&end_date=END_DATE' + \
			'&timezone=America/New_York' + \
			'&filter=(publisher.name+=+"AD_NETWORK")' + \
			'&fields=site.name,sdk,created,publisher.name,campaign.name,' + \
				'publisher_sub_campaign.name,advertiser_sub_campaign.name,' + \
				'publisher_sub_publisher_id,publisher_sub_publisher.name,' + \
				'publisher_sub_site_id,publisher_sub_site.name,publisher_sub1,'+ \
				'ios_ifa,google_aid,mat_id'

	def getEventsData(self, start_date, end_date):
		self.start_date = start_date
		self.end_date = end_date
		tune_data = self.getInstallData()
		tune_data += self.getRevenueData()
		return tune_data

	def getInstallData(self):
		self.createUrl()
		self.url = self.url.replace('ENDPOINT', 'installs')
		self.url += '&limit=5000&response_format=json'
		result = self.makeCall()
		install_data = self.compileResult(result)
		return install_data

	def getRevenueData(self):
		self.createUrl()
		self.url = self.url.replace('ENDPOINT', 'events')
		self.url += ',install_created,revenue_usd,purchase_validation_status' + \
			'&limit=5000&response_format=json'
		result = self.makeCall()
		revenue_data = self.compileResult(result)
		return revenue_data
	
	def compileResult(self, result):
		data = []
		for row in result:
			data_dict = {}
			data_dict['app'] = row['site.name']
			data_dict['platform'] = row['sdk']
			data_dict['install_date'] = self.convertCreated(row['created'])['date']
			data_dict['install_time'] = self.convertCreated(row['created'])['time']
			data_dict['ad_network'] = row['publisher.name']
			data_dict['campaign'] = row['campaign.name']
			data_dict['publisher_campaign'] = row['publisher_sub_campaign.name']
			data_dict['my_campaign'] = row['advertiser_sub_campaign.name']
			data_dict['sub_publisher'] = row['publisher_sub_publisher.name']
			data_dict['sub_publisher_id'] = row['publisher_sub_publisher_id']
			data_dict['sub_site'] = row['publisher_sub_site.name']
			data_dict['sub_site_id'] = row['publisher_sub_site_id']
			data_dict['publisher_sub1'] = row['publisher_sub1']
			data_dict['ios_ifa'] = row['ios_ifa']
			data_dict['google_aid'] = row['google_aid']
			data_dict['mat_id'] = row['mat_id']
			try:
				data_dict['event_date'] = self.convertCreated(row['created'])['date']
				data_dict['event_time'] = self.convertCreated(row['created'])['time']
				data_dict['revenue_usd'] = row['revenue_usd']
				data_dict['purchase_validation_status'] = row['purchase_validation_status']
			except KeyError:
				data_dict['event_date'] = data_dict['install_date']
				data_dict['event_time'] = data_dict['install_time']
				data_dict['revenue_usd'] = 0
				data_dict['purchase_validation_status'] = 1 # Indicates invalid purchase
			data.append(data_dict)
		return data
	
	def convertCreated(self, created):
		try:
			created = created.split('T')
			date = created[0]
			time = created[1].split('-')[0]
		except AttributeError:
			date = None
			time = None
		return {'date': date, 'time': time}

	def makeCall(self):
		result = []
		days = (self.end_date - self.start_date).days + 1
		for ad_network in self.ad_networks:
			for day in range(0, days):
				for hour in (0, 12):
					url = self.url
					url = url.replace('AD_NETWORK', ad_network)
					date = self.start_date + timedelta(days=day)
					end_date = date + timedelta(hours=hour+12)
					date = date + timedelta(hours=hour)
					date = date.strftime('%Y-%m-%dT%H:%M:%SZ')
					end_date = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
					url = url.replace('START_DATE', date)
					url = url.replace('END_DATE', end_date)
					try:
						request = requests.get(url).json()
						result += request['data']
					except KeyError:
						logger.error('Tune API call failed with error: %s', request)
						sys.exit()
		return result
