from EmailReports.reportmanager import *
from local_config import *

rm = ReportManager(source='data-general')
recepients = ['yoshi@weplaydots.com', 'lisa@weplaydots.com', 
'tony@weplaydots.com', 'haydon@weplaydots.com', 'kohta@weplaydots.com',
'jordyn@weplaydots.com', 'paul@weplaydots.com', 'chris@weplaydots.com']
rm.sendReport(recepients)
