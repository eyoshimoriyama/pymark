import json
import requests
import datetime
import sys
import logging
from datetime import timedelta
from local_config import *
from campaignfunctions import *
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalyticsAPI:

    def __init__(self, app, platform, app_id):
        self.base_url = 'https://api.localytics.com/v1/query?'
        self.auth = (loc_api_key, loc_api_secret)
        self.app = app
        self.platform = platform
        self.app_id = app_id
        self.cross_promo_channels = '"Two Dots","Dots Classic","Two Dots Email","General Promo","Two Dots Settings Menu",' + \
            '"Recruit-Share","TDI Rewarded Video","TDI-Treasure Hunt","HO-DAC xPromo","HO-Dots Classic (Cross Promo)",'+ \
             '"HO-Share Link","HO-Two Dots Email","HO-Dots Organic","HO-TwoDots | Vanity URL","HO-Shaq","HO-Tarot Cards"'
    
    def getInstallData(self, start_date, end_date):
        url = self.base_url + 'app_id=' + self.app_id + \
            '&metrics=users&' + 'dimensions=day,country,channel,campaign,adset&' + \
            'conditions={"day":["between","' + start_date + '","' + end_date + \
            '"],"new_device":["==","t"]}'
        result = self.makeCall(url)
        install_data = self.compileResult(result)
        return install_data

    def getRevenueData(self, start_cohort, start_date, end_date):
        url = self.base_url + 'app_id=' + self.app_id + '&metrics=occurrences&' + \
            'dimensions=a:Amount (USD),day,campaign,birth_day,adset,country&' + \
            'conditions={"day":["between","' + start_date + '","' + end_date + '"],' + \
            '"birth_day":["between","' + start_cohort + '","' + end_date + '"],'
        
        if self.app == 'Two Dots':
            url += '"event_name":"Purchase Made","a:Transaction ID":["not_in","<null>","null",null],' + \
            '"a:State":["not_in","Pending Purchase","Restored Purchase"],' + \
            '"a:Type":["not_in","Subscription"],' 
        
        if self.app == 'Dots & Co':
            url = url.replace('Amount (USD)', 'AmountUSD')
            url += '"event_name":"PurchaseMade","a:IsValidPurchase":["in","True",null],' + \
            '"a:TransactionIdentifier":["not_in","<null>","null",null],'
        
        if self.app == 'Wilds':
            url = url.replace('Amount (USD)','Price_USD')
            url += '"event_name":"PurchaseMade","a:CompletionType":["in","Success"],'
        
        if self.platform == 'iOS':
            url += '"jailbroken":["==","No"],'
            url = url.replace('a:Transaction', 'n:Transaction')
        
        revenue_data = self.getFbRevenueData(url)
        revenue_data += self.getOtherRevenueData(url)
        return revenue_data

    def getFbRevenueData(self, url):
        revenue_data = []
        for channel in ['Facebook', 'Instagram', 'Facebook-External']:
            channel_url = url + '"channel":["in","' + channel + '"]}' 
            result = self.makeCall(channel_url)
            for row in result:
                row['channel'] = channel
            revenue_data += self.compileResult(result)
        return revenue_data

    def getOtherRevenueData(self, url):
        url = url.replace('adset','channel')
        url += '"channel":["not_in","Facebook","Facebook-External","Instagram"]}'
        result = self.makeCall(url)
        revenue_data = self.compileResult(result)
        return revenue_data

    def compileResult(self, result):
        data = []
        for row in result:
            data_dict = {}
            data_dict['app'] = self.app
            data_dict['platform'] = self.platform
            country = row['country']
            data_dict['country'] = country
            acquire_source = row['channel']
            data_dict['acquire_source'] = acquire_source
            campaign = row['campaign']
            data_dict['campaign'] = updateCampaignName(campaign, self.platform, acquire_source)
            
            if country == None:
                data_dict['country'] = 'Unspecified'
            
            if acquire_source == None:
                data_dict['acquire_source'] = 'Organic'
                data_dict['source'] = 'Organic'
            if acquire_source != None and acquire_source in self.cross_promo_channels:
                data_dict['source'] = 'Cross Promo'
            if acquire_source != None and acquire_source not in self.cross_promo_channels:
                data_dict['source'] = 'Paid'
            
            if campaign == None:
                data_dict['campaign'] = 'Unspecified'
            
            try:
                adset = row['adset']
            except KeyError:
                adset = 'Unspecified'
           
            if adset == None:
                adset = 'Unspecified'
            
            data_dict['adset'] = adset                
            try:
                data_dict['install_date'] = row['birth_day']
                data_dict['date'] = row['day']
            except KeyError:
                data_dict['install_date'] = row['day']
            try:
                data_dict['installs'] = int(row['users'])
            except KeyError:
                pass
            try:
                occurrences = row['occurrences']
                data_dict['occurrences'] = occurrences
            except KeyError:
                pass
            try:
                str_amount_usd = row['a:Amount (USD)']
            except KeyError:
                pass
            try:    
                str_amount_usd = row['a:AmountUSD']
            except KeyError:
                pass
            try:    
                str_amount_usd = row['a:Price_USD']
            except KeyError:
                pass
            try:
                str_amount_usd = str_amount_usd.replace(',', '.') 
                amount_usd = float(str_amount_usd)
                data_dict['amount_usd'] = amount_usd
                data_dict['revenue'] = amount_usd * occurrences
            except NameError:
                pass      
            data.append(data_dict)
        return data

    def makeCall(self, url):
        request = requests.get(url, auth=self.auth)
        try:
            result = request.json()['results']
        except (ValueError, KeyError):
            logger.error('Localytics call failed with error: %s', request.text)
            sys.exit()
        
        if len(result) == 0:
            logger.warn('No data returned, check your request')
        elif len(result) >= 49999:
            logger.warn("API call was over the limit. There may be missing data")
        else:
            logger.info('Localytics call succesful. %s rows returned', len(result))
        
        return result
