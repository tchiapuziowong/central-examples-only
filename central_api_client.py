from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from pprint import pprint
import json
import logging
from threading import Timer
from time import sleep
import sys

# influxdb client
from influxdb import InfluxDBClient

# Import Aruba Central Base
from pycentral.base import ArubaCentralBase
from pprint import pprint

"""
Global Variables
"""
# Collect metrics every 5 seconds
REPEAT_NSEC = 5
global central_obj
global influxdb_obj

class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.daemon = True
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False

def central_get_data():
    global central_obj
    central_get_apprf()
    # central_get_presence()

def central_get_apprf():
    global central_obj
    # Sample API call using 'ArubaCentralBase.command()'
    # GET groups from Aruba Central
    apiPath = "/apprf/datapoints/v2/topn_stats"
    apiMethod = "GET"
    sites = ["ATM-Demo"]
    sites_data = {}
    for site in sites:
        apiParams = {
            "count": 10,
            "site": site
        }
        base_resp = central_obj.command(apiMethod=apiMethod,
                                    apiPath=apiPath,
                                    apiParams=apiParams)
        #pprint(base_resp)
        sites_data[site] = base_resp['msg']
        sites_data[site]['name'] = site

    #pprint(sites_data)
    update_influxdb(sites_data)

def update_influxdb(sites_data):
    global influxdb_obj
    json_body = []
    for site in sites_data.keys():
        data = sites_data[site]
        timestamp = data['result']['app_cat'][0]['timestamp']
        field_data = {
            "measurement": "apprfData",
            "tags": {
                "topic": "apprf",
                "site": site
            },
            "time": timestamp,
            "fields": {}
        }
        field_tmp = {}
        for app_data in data['result']['app_cat']:
            field_tmp['name'] = app_data['name']
            field_tmp['percent_usage'] = app_data['percent_usage']
            field_data["fields"] = field_tmp
            json_body.append(field_data.copy())
    
    try:
        result = influxdb_obj.write_points(points=json_body, database='atm23')
        #print(streaming_data['topic'] + " Database write: "+ result)
        print(f'AppRF Database write: {result}')
        if result == False:
            print("DB push failed!!!")
    except Exception as err:
        print(err)

def establish_influx_conn(host='', port=8086, username='', password='', ssl=True, verify_ssl=True, database=''):
    global influxdb_obj
    host = 'localhost'
    port = 8086
    username = 'admin'
    password = 'Aruba#123!'
    ssl = False
    verify_ssl = False
    client = None
    database = 'atm23'

    try:
        # Create connection
        influxdb_obj = InfluxDBClient(host, port, username, password, ssl, verify_ssl)
        
        # Check if database exists or create a new database
        db_found = False
        db_list = influxdb_obj.get_list_database()
        for db in db_list:
          if database == db["name"]:
              db_found = True

        if db_found == False:
          influxdb_obj.create_database(database)
          influxdb_obj.alter_retention_policy(name='autogen', database=database, duration='INF')

        # Switch to an existing database
        influxdb_obj.switch_database(database)

        return influxdb_obj
      
    except Exception as err:
        print('Database connection error:', err)
        return None


if __name__ == "__main__":
    print ("starting...")
    
    # Establish InfluxDB connection
    influxdb_obj = establish_influx_conn()

    # Create Central Connection Object
    central_info = {
        "username": "aruba.test.central@gmail.com",
        "password": "H9yzxLgvkaTY@iS",
        "base_url": "https://internal-apigw.central.arubanetworks.com",
        "client_id": "LXz5Rpu7PfehOVeRsC1o4QCUmMJV1BHM",
        "client_secret": "oTYNKQjwdbADt5thH64rq3wbb6bolprq",
        "customer_id": "e2be0de9d75d4b52bbb3ec7fff8140d8",
        "token": {
            "access_token": "vw8DwhBLT0KqQv7ssMtHmMvXBrfLnBdc",
            "refresh_token":"pSrN8RLpXMD7gAYvxUDNmoLE5Zf1oUDJ"
        }
    }
    ssl_verify = True
    central_obj = ArubaCentralBase(central_info=central_info,
                            ssl_verify=ssl_verify)

    # replace prom_collector with function we want repeated
    rt = RepeatedTimer(REPEAT_NSEC, central_get_data()) # it auto-starts, no need of rt.start()

    try:
        while True:
            pass
    except:
        rt.stop() # better in a try/finally block to make sure the program ends!
        print("Goodbye!")
