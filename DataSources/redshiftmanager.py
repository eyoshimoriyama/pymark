import psycopg2
from local_config import *

def create_conn(*args,**kwargs):
 	config = kwargs['config']
 	try:
 		conn=psycopg2.connect(dbname=config['dbname'], host=config['host'], port=config['port'], user=config['user'], password=config['pwd'])
 	except Exception as err:
 	  print err.code, err
 	return conn

conn = create_conn(config=rs_configuration)
cursor = conn.cursor()
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
