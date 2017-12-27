# pymark

This repository automates the reporting and analysis of marketing data by integrating with 
various APIs including Facebook, Localytics, Tenjin, and Tune. 

### Main Scripts

To pull marketing data from the integrated APIs and insert it into the configured MySQL database run:

```python datadump.py```

To query the databse and perform LTV projections run:

```python chorotdatadump.py``` 

To send a marketing email report to the configure recipients run:

```python marketingreport.py```


### Python Modules
* facebook-python-ads-sdk - https://github.com/facebook/facebook-python-ads-sdk
* requests - https://pypi.python.org/pypi/requests/
* MySQL - https://pypi.python.org/pypi/MySQL-python/1.2.5
* psycopg2 - https://pypi.python.org/pypi/psycopg2
* numpy - https://pypi.python.org/pypi/numpy
