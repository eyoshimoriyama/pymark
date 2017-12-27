import json
import datetime
import requests
import sys
from requests.auth import HTTPBasicAuth
from local_config import *
from campaignfunctions import *

class SupersonicAPI:
	
	def __init__(self, app):
		self.account_name = 'Supersonic'
		self.app = app
		self.today = datetime.datetime.now()
		self.acquire_source = 'Supersonic'
		self.marketing_type = 'UA'
		self.auth = HTTPBasicAuth(supersonic_email, supersonic_secret_key)
		self.base_url = 'https://platform.ironsrc.com/partners/'
		
	def getAdsRevenueData(self, start_date, end_date):
		url = self.base_url + 'publisher/mediation/applications/v2/stats?' + \
			'startDate=' + start_date + '&' + 'endDate=' + end_date + \
			'&adUnits=rewardedVideo&breakdowns=date,app,country,adSource' 
		ads_rev_data = self.makeCall(url)
		ads_rev_data = self.compileAdsRevenueData(ads_rev_data)
		return ads_rev_data

	def compileAdsRevenueData(self, ads_rev_data):
		data_tuples = []
		for row in ads_rev_data:
			date = datetime.datetime.strptime(row['date'], "%Y-%m-%d")
			ad_source = row['providerName']
			if row['appName'] == 'Two Dots (iOS)':
				app_name = 'twodots-ios'
			if row['appName'] == 'TwoDots (Android)':
				app_name = 'twodots-android'
			if len(ad_source) >= 30:
				ad_source = ad_source.split(' ', )[0][:30]
			for next in row['data']:
				impressions = next['impressions']
				country = next['countryCode'].lower()
				try:
					total_revenue = float(next['revenue'])
				except ValueError:
					total_revenue = 0
				try:
					requests = float(next['requests'])
				except KeyError:
					requests = 0
				try:
					ecpm = float(next['eCPM'])
				except ValueError:
					ecpm = 0
				try:
					fill_rate = float(next['fillRate'])
				except ValueError:
					fill_rate = 0
				data_tuples.append((date, app_name, country, ad_source, 
					total_revenue, ecpm, fill_rate, requests, impressions))
		return data_tuples

	def getCampaignData(self):
		campaigns = '/advertiser/campaigns'
		url =  self.base_url + campaigns
		campaign_data = self.makeCall(url)['data']
		return campaign_data

	def getSpendData(self, start_date, end_date):
		start_date = str(start_date)[:10]
		end_date = str(end_date)[:10]
		advertiser_stats = 'advertiser/campaigns/stats?'
		date_interval = 'start_date=' + start_date + '&' + 'end_date=' + end_date
		url =  self.base_url + advertiser_stats + date_interval
		r = requests.get(url, auth=self.auth)
		spend_data = self.makeCall(url)['data']
		spend_data = self.compileSpendData(spend_data)
		return spend_data

	def compileSpendData(self, spend_data):
		data_tuples = []
		campaign_data = self.getCampaignData()
		acquire_source = 'Supersonic'
		for row in spend_data:
			campaign_id = row['campaign_id']
			date = row['date']
			metrics = row['data'][0]
			country = metrics['country_code'].upper()
			account_id = 'None'
			adset_id = 'None'
			ad_id = 'None'
			adset = 'None'
			ad = 'None'
			age = 'None'
			gender = 'None'
			creative_type = 'None'
			creative_name = 'None'
			ad_copy = 'None'
			spend = float(metrics['expense']) / 100
			installs = float(metrics['conversions'])
			impressions = int(metrics['impressions'])
			clicks = int(metrics['clicks'])
			unique_impressions = None
			unique_clicks = None
			for next in campaign_data:
				if next['campaign_id'] == campaign_id:
					bundle_id = next['bundle_id']
					target_platform = next['target_platform']
					campaign = updateCampaignName(next['campaign_name'])
					app = parseCampaignName(campaign)['app']
					if bundle_id == 'com.weplaydots.twodots' or bundle_id == 'com.weplaydots.twodotsandroid':
						app = 'Two Dots' 
					if target_platform[0] == 'google_play':
						platform = 'Android'
					if target_platform[0] == 'apple_itunes':
						platform = 'iOS'
			if self.app == app:
				data_tuples.append((self.app, self.marketing_type, date, platform, country,\
					acquire_source, campaign, adset, age, gender, ad, creative_type, creative_name, ad_copy,\
					spend, clicks, impressions, unique_clicks, unique_impressions, installs,
					self.account_name, account_id, campaign_id, adset_id, ad_id))
		return data_tuples

	def makeCall(self, url):
		r = requests.get(url, auth=self.auth)	
		try:
			data = json.loads(r.text)
			try:
				data[0]
			except KeyError:
				print r.text
				sys.exit()
		except ValueError:
			print "ERROR: " + "No data returned from Supersonic"
			sys.exit()
		return data
