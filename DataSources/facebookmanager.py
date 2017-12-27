import datetime
import requests
import json
import sys
import time
import datetime
import facebookads
from datetime import timedelta
from facebookads.api import FacebookAdsApi
from facebookads.session import FacebookSession
from facebookads.adobjects.adsinsights import AdsInsights
from facebookads.adobjects.adaccount import AdAccount 
from facebookads.adobjects.ad import Ad 
from facebookads.adobjects.adset import AdSet
from facebookads.adobjects.adcreative import AdCreative
from facebookads.adobjects.user import User
from local_config import *
from campaignfunctions import *

class FacebookAPI:

	def __init__(self):
		session = FacebookSession(fb_app_id, fb_app_secret, fb_extended_token)
		api = FacebookAdsApi(session)
		FacebookAdsApi.set_default_api(api)
		self.me = User(fbid='me')
		self.getAccounts()

	def getAccounts(self):
		all_accounts = list(self.me.get_ad_accounts(
			fields=[AdAccount.Field.account_id, AdAccount.Field.name]))
		self.accounts = [account for account in all_accounts 
			if account['account_id'] in fb_account_ids.values()]

	def getSpendData(self, start_date, end_date, breakdown=None):
		self.setParameters(start_date, end_date, breakdown)
		self.spend_data = []
		for account in self.accounts:
			async_job = account.get_insights(params=self.params, async=True)
			status = async_job.remote_read()
			while status['async_percent_completion'] < 100:
				time.sleep(2)
				status = async_job.remote_read()
			time.sleep(1)
			result = async_job.get_result()
			self.account_name = account['name']
			self.account_id = account['account_id']
			self.compileResult(result)
		return self.spend_data

	def setParameters(self, start_date, end_date, breakdown):
		self.params = {'time_range': {'since': start_date, 'until': end_date}, 
			'fields': [
				AdsInsights.Field.campaign_name,
				AdsInsights.Field.campaign_id,
				AdsInsights.Field.adset_name,
				AdsInsights.Field.adset_id,
				AdsInsights.Field.ad_name,
				AdsInsights.Field.ad_id,
				AdsInsights.Field.spend,
				AdsInsights.Field.inline_link_clicks,
				AdsInsights.Field.unique_actions,
				AdsInsights.Field.impressions,
				AdsInsights.Field.unique_inline_link_clicks,
		 		AdsInsights.Field.reach,
		 		AdsInsights.Field.clicks,
		 		AdsInsights.Field.unique_clicks
		 	],
		'level': 'ad',
		'time_increment': 1}
		if breakdown != None:
			self.params['breakdowns'] = breakdown

	def compileResult(self, result):
		self.targeting_dict = {}
		self.creative_dict = {}
		for row in result:
			data_dict = {}
			data_dict['account_name'] = self.account_name
			data_dict['account_id'] = self.account_id
			data_dict['acquire_source'] = 'Facebook'
			data_dict['date'] = row['date_start']
			data_dict['campaign'] = row['campaign_name']
			data_dict['campaign_id'] = row['campaign_id']
			data_dict['adset'] = row['adset_name']
			data_dict['ad'] = row['ad_name']
			adset_id = row['adset_id']
			ad_id = row['ad_id']
			data_dict['adset_id'] = adset_id
			data_dict['ad_id'] = ad_id
			data_dict['spend'] = float(row['spend'])
			data_dict['clicks'] = int(row['inline_link_clicks'])
			data_dict['impressions'] = int(row['impressions'])
			data_dict['unique_impressions'] = int(row['reach'])
			data_dict['unique_clicks'] = int(row['unique_inline_link_clicks'])
			data_dict['installs'] = 0
			try: 
				data_dict['country'] = row['country']
			except KeyError:
				data_dict['country'] = parseCampaignName(row['campaign_name'], parameter='country')
			try:
				for action in row['unique_actions']:
					if action['action_type'] == 'mobile_app_install':
						data_dict['installs'] = action['value']
						break	
			except KeyError:
				pass
			try:
				self.targeting_dict[adset_id]
			except KeyError:
				self.setTargetingDict(adset_id)
			try:
				self.creative_dict[ad_id]
			except KeyError:
				self.setCreativeDict(ad_id)
			data_dict['title'] = self.creative_dict[ad_id]['title']
			data_dict['ad_copy'] = self.creative_dict[ad_id]['ad_copy']
			data_dict['age_max'] = self.targeting_dict[adset_id]['age_max']
			data_dict['age_min'] = self.targeting_dict[adset_id]['age_min']
			data_dict['gender'] = self.targeting_dict[adset_id]['gender']
			data_dict['user_os'] = self.targeting_dict[adset_id]['user_os']
			data_dict['app_install_state'] = self.targeting_dict[adset_id]['app_install_state']
			data_dict['marketing_type'] = self.targeting_dict[adset_id]['marketing_type']
			data_dict['event_type'] = self.targeting_dict[adset_id]['event_type']
			data_dict['platform'] = self.targeting_dict[adset_id]['platform']
			data_dict['placement'] = self.targeting_dict[adset_id]['placement']
			data_dict['app'] = self.targeting_dict[adset_id]['app']
			self.spend_data.append(data_dict)

	def setCreativeDict(self, ad_id):
		ad = Ad(ad_id)
		creative = ad.get_ad_creatives(fields=[
					AdCreative.Field.title,
					AdCreative.Field.body, 
					])
		creative = creative[0]
		title = creative.get('title', 'Unspecified')
		ad_copy = creative.get('body', 'Unspecified')
		self.creative_dict[ad_id] = \
			{
			'title': title,
			'ad_copy': ad_copy
			}
			
	def setTargetingDict(self, adset_id):
		adset = AdSet(adset_id)
		adset_read = adset.remote_read(fields=['targeting','promoted_object'])
		targeting = adset_read['targeting']
		age_max = targeting['age_max']
		age_min = targeting['age_min']
		user_os = targeting['user_os'][0]
		app_install_state = targeting['app_install_state']
		promoted_object = adset_read['promoted_object']
		store_url = promoted_object['object_store_url']
		app_id = promoted_object['application_id']
		app = fb_app_ids[app_id]
		
		if app_install_state == 'installed':
			marketing_type = 'Remarketing'
		
		if app_install_state == 'not_installed':
			marketing_type = 'UA'
		
		if 'itunes' in store_url:
			platform = 'iOS'
		
		if 'google' in store_url:
			platform = 'Android'
		
		placement = ''
		for row in targeting['publisher_platforms']:
			placement += row + '-'
		placement = placement[:-1]
		event_type = promoted_object.get('custom_event_type', 'Unspecified')
		try: 
			gender = targeting['genders'][0]
			if gender == 1:
				gender = 'M'
			if gender == 2:
				gender = 'F'
		except KeyError:
			gender = 'All'
		self.targeting_dict[adset_id] = \
			{
			'age_max': age_max, 
			'age_min': age_min,
			'gender': gender,
			'user_os': user_os,
			'app_install_state': app_install_state,
			'marketing_type': marketing_type,
			'event_type': event_type,
			'platform': platform,
			'placement': placement,
			'app': app
			}
