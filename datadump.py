import datetime
from local_config import *
from DataInserts.dbmanager import *

# Define date range to dump data
look_back_days = 7
today = datetime.datetime.now()
start_date = today - timedelta(days=look_back_days)

# Establish DB instance and dump data to MySQL
db = DBManager(destination='data-general')
db.dumpSpendData(start_date=start_date, end_date=today)
db.dumpLocalyticsData(start_date=start_date, end_date=today)
db.dumpTuneData(start_date=start_date, end_date=today)
