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

def central_get_apprf(central_obj):
    # Sample API call using 'ArubaCentralBase.command()'
    # GET groups from Aruba Central
    apiPath = "/apprf/datapoints/v2/topn_stats"
    apiMethod = "GET"
    apiParams = {
        "count": 10
    }
    base_resp = central_obj.command(apiMethod=apiMethod,
                                apiPath=apiPath,
                                apiParams=apiParams)
    pprint(base_resp)

    #update_influxdb(base_resp)

def update_influxdb(data):
    field_data = {}
    timestamp = data['result']
    json_body = [{
        "measurement": "apprfData",
        "tags": {
            "topic": streaming_data['topic'],
            "customer_id": streaming_data['customer_id'],
            "type": "proximity"
        },
        "time": streaming_data['timestamp'],
        "fields": field_data
    }]

def establish_influx_conn(host='', port=8086, username='', password='', ssl=True, verify_ssl=True, database=''):
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
        client = InfluxDBClient(host, port, username, password, ssl, verify_ssl)
        
        # Check if database exists or create a new database
        db_found = False
        db_list = client.get_list_database()
        for db in db_list:
          if database == db["name"]:
              db_found = True

        if db_found == False:
          client.create_database(database)
          client.alter_retention_policy(name='autogen', database=database, duration='INF')

        # Switch to an existing database
        client.switch_database(database)
      
    except Exception as err:
        print('Database connection error:', err)

    return client

def prom_collector_update():
    switch_metrics = []
    global sdict
    for idx, switch in enumerate(sdict):
        if "conn" not in switch:
            sdict[idx]["conn"] = None
        if "conn" in switch and not switch["conn"]:
            print("Logging in to switch: {}".format(switch["switch_ip"]))
            sdict[idx]["conn"] = login(base_url=switch["url"], 
                                       username=switch["username"], 
                                       password=switch["password"])
        cx = sdict[idx]["conn"]

        try:
            sinfo = get_switch_info(s=cx, url=switch["url"])
            for key, val in sinfo.items():
                sdict[idx][key] = val
            switch_metrics.append(collect_switch_metrics(cx=cx, idx=idx, switch=switch, sdict=sdict))
        except Exception as err:
            cx = logout(s=cx, url=switch["url"])
            sdict[idx]["conn"] = login(base_url=switch["url"], 
                                       username=switch["username"], 
                                       password=switch["password"])
            raise err

    # Update Switch Metrics to Prometheus Database
    prom_update_db(switch_metrics)

if __name__ == "__main__":
    print ("starting...")

    db_conn = None
    # Establish InfluxDB connection
    db_conn = establish_influx_conn()

    # Create Central Connection Object
    central_info = {
        "base_url": "https://internal-apigw.central.arubanetworks.com",
        "client_id": "mAzkBKcYnX3R9Kj0cZ6H2E8C1Yv3vr1v",
        "client_secret": "P1bc7zrc0CkQRplsNdvgwr3s3NMFziVB",
        "customer_id": "e2be0de9d75d4b52bbb3ec7fff8140d8",
        "token": {
            "access_token": "CPB76coMvPmRnt20I5Ag466gVJ4s9zrZ"
        }
    }
    ssl_verify = True
    central = ArubaCentralBase(central_info=central_info,
                            ssl_verify=ssl_verify)
    exit()
    # replace prom_collector with function we want repeated
    rt = RepeatedTimer(REPEAT_NSEC, prom_collector_update) # it auto-starts, no need of rt.start()

    try:
        while True:
            pass
    except:
        rt.stop() # better in a try/finally block to make sure the program ends!
        print("Goodbye!")
