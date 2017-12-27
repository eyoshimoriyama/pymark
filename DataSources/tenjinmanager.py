import requests
import json
import datetime
from datetime import timedelta
from local_config import *
from campaignfunctions import *

class TenjinAPI:
	
	def __init__(self, ignore_networks):
		self.account_name = "Tenjin"
		self.ignore_networks = ignore_networks
		self.auth = (tenjin_email, tenjin_api_key)

	def getSpendData(self, start_date, end_date, breakdown=None):
		url = 'https://reports.tenjin.io/api/v0/campaign_aggregate?' + \
			'start_day=' + str(start_date)[:10] + \
			'&end_day=' + str(end_date)[:10] +\
			'&group_by=country'
		r = requests.get(url, auth=self.auth)
		total_pages = json.loads(r.text)['total_pages'] + 1
		result = []
		for page in range(1, total_pages):
			url = url+'&page='+str(page)
			r = requests.get(url, auth=self.auth)
			result += json.loads(r.text)['data']
		spend_data = self.compileResult(result)
		return spend_data

	def compileResult(self, result):
		spend_data = []
		for row in result:
			data_dict = {}
			data_dict['app_install_state'] = 'not_installed'
			data_dict['marketing_type'] = 'UA'
			data_dict['account_name'] = self.account_name
 			platform = row['platform']
			country = row['country']
			acquire_source = row['ad_network'].title()
			campaign = updateCampaignName(row['campaign_name'], acquire_source)
			bundle_id = row['bundle_id']
			data_dict['date'] = row['day']
			data_dict['country'] = country
			data_dict['acquire_source'] = acquire_source
			data_dict['campaign'] = campaign
			data_dict['campaign_id'] = row['remote_campaign_id']
			data_dict['adset_id'] = 'None'
			data_dict['ad_id']= 'None'
			data_dict['spend'] = row['spend']
			data_dict['installs'] = row['downloads']
			data_dict['impressions'] = row['impressions']
			data_dict['clicks'] = row['clicks']
			data_dict['unique_impressions'] = row['impressions']
			data_dict['unique_clicks'] = row['clicks']
			
			if bundle_id in ('com.weplaydots.twodots', 'com.weplaydots.twodotsandroid'):
				data_dict['app'] = 'Two Dots'
			
			if bundle_id == 'com.weplaydots.plus':
				data_dict['app'] = 'Dots & Co'
			
			if bundle_id == 'com.weplaydots.wilds':
				data_dict['app'] = 'Wilds'
			
			if platform == 'ios':
				data_dict['platform'] = 'iOS'
			
			if platform == 'android':
				data_dict['platform'] = 'Android'
			
			if bundle_id == None or bundle_id not in ('com.weplaydots.twodots', 'com.weplaydots.twodotsandroid', 'com.weplaydots.plus'):
				data_dict['app'] = parseCampaignName(campaign, parameter='app')
			
			if platform == None:
				data_dict['platform'] = parseCampaignName(campaign, parameter='platform')
 			
 			if country == None:
 				data_dict['country'] = parseCampaignName(campaign, parameter='country')
 			
 			if acquire_source not in self.ignore_networks:
				spend_data.append(data_dict)	
		
		return spend_data
