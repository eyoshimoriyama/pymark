import datetime
from slacker import Slacker
from local_config import *


class SlackManager():
	
	def __init__(self): 
		self.slack = Slacker(slack_api_token)

	def sendAdsRevenueUpdate(self, users, rows_deleted, rows_inserted):
		now = str(datetime.datetime.now())[:16]
		for user in users:
			self.slack.chat.post_message(user,\
			"Ads revenue script was completed at " + now + ". " + \
			str(rows_deleted) + " rows were deleted and " +\
			str(rows_inserted) + " rows were inserted.")

	def sendAdsRevenueUpdate(self, user, start_date, end_date, rows_deleted, rows_inserted):
		now = str(datetime.datetime.now())[:16]
		self.slack.chat.post_message(user, "*Ads Revenue Script Completed*")
		self.slack.chat.post_message(user, "Time Completed: " + now)
		self.slack.chat.post_message(user, "Date Range Updated: " + start_date + " - " + end_date)
		self.slack.chat.post_message(user, "Rows Deleted: " + str(rows_deleted))
		self.slack.chat.post_message(user, "Rows Inserted: " + str(rows_inserted))
