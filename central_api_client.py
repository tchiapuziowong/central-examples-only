from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from pprint import pprint
import json
import logging
from threading import Timer
from time import sleep, time
import sys
import time
import json
import pdb
import argparse
from statistics import mean
import math

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
# Collect REST APIs every N minutes
REST_INTERVAL_MINUTE = 1
REST_INTERVAL_SECONDS = REST_INTERVAL_MINUTE * 60

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
    central_sites_list = central_get_sites()
    global startTime
    startTime = int(time.time() - REST_INTERVAL_SECONDS)
    global endTime 
    endTime = int(time.time())
    scriptOption = {
        'automation': True
    }
    print('test1')
    if (scriptOption['automation']):
        print('test2')
        ap_list = get_aps(central_sites_list)  
        ap_list = get_aps_rf(ap_list)  
        ap_list = get_clients_per_site(ap_list)
        ap_list = central_get_top_n_apps(ap_list)
        print('test3')
        update_influxdb('apData', ap_list)
    else:
     #   central_get_apprf(central_sites_list)
        central_get_presence(central_sites_list)


from pycentral.monitoring import Sites
def central_get_sites():
    global central_obj
    s = Sites()
    site_list = s.get_sites(central_obj)
    pdb.set_trace()
    return [{'name': site['site_name'], 'id': site['site_id']} for site in site_list['msg']['sites']]

def get_aps(site_list):
    global central_obj
    sites = [site['name'] for site in site_list]
    apiPath = "/monitoring/v2/aps"
    apiMethod = "GET"
    apsObject = {}
    for site in sites:
        apiParams = {
                "fields": "status,model",
                "site": site,
                "calculate_total": True,
                "calculate_client_count": True,
                "show_resource_details": True,
                "limit": 1000
        }
        base_resp = central_obj.command(apiMethod=apiMethod,
                                    apiPath=apiPath,
                                    apiParams=apiParams)
        site_aps = base_resp['msg']['aps']
        site_aps_with_clients = [ap for ap in site_aps if ap['client_count'] > 0]
        # Only adding sites that have atleast 1 client associated with the APs
        if len(site_aps_with_clients) > 0:
            apsObject[site] = site_aps_with_clients

    return apsObject

def get_aps_rf(site_list):
    global central_obj, startTime, endTime
    # Sample API call using 'ArubaCentralBase.command()'
    # GET groups from Aruba Central
    for site in site_list.keys():
        for ap in site_list[site]:
            apiPath = f"/monitoring/v3/aps/{ap['serial']}/rf_summary"
            apiMethod = "GET"
            apiParams = {
                    "from_timestamp": startTime,
                    "to_timestamp": endTime
            }
            base_resp = central_obj.command(apiMethod=apiMethod,
                                            apiPath=apiPath,
                                            apiParams=apiParams)
            #pdb.set_trace()
            if (len(base_resp['msg']['samples']) > 0):
                ap['utilization'] = math.ceil(mean([sample['utilization'] for sample in base_resp['msg']['samples']]))
            else: 
                ap['utilization'] = 0
    
    return site_list

def get_clients_per_site(site_list):
    global central_obj
    apiPath = "/monitoring/v2/clients"
    apiMethod = "GET"
    for site in site_list.keys():
        for ap in site_list[site]:
            # Check if there was any client connected to the AP Before fetching detailed Client stats
            if (ap['client_count'] > 0):
                apiParams = {
                    "calculate_total": True,
                    "timerange": "3H",
                    "client_type": "WIRELESS",
                    "client_status": "CONNECTED",
                    "show_usage": True,
                    "show_manufacturer": True,
                    "show_signal_db": True,
                    "serial": ap['serial'],
                    "fields": "name,ip_address,username,os_type,associated_device,network,manufacturer,speed,usage,health,site,signal_strength,signal_db,snr"
                }
                base_resp = central_obj.command(apiMethod=apiMethod,
                                        apiPath=apiPath,
                                        apiParams=apiParams)
                if len(base_resp['msg']['clients']) > 0:
                    clients_list = base_resp['msg']['clients']
                    for client in clients_list:
                        client.pop('associated_device')
                    ap['clients'] = base_resp['msg']['clients']
    return site_list

def central_get_top_n_apps(site_list):
    global central_obj, startTime, endTime
    # Sample API call using 'ArubaCentralBase.command()'
    # GET groups from Aruba Central
    apiPath = "/apprf/v1/applications"
    apiMethod = "GET"
    for site in site_list.keys():
        for ap in site_list[site]:
            # Check if there was any client connected to the AP Before fetching detailed Client AppRF stats
            if (ap['client_count'] > 0):
                apiParams = {
                    "serial": ap['serial'],
                    "details": True,
                    "from_timestamp": startTime,
                    "to_timestamp": endTime
                }
                base_resp = central_obj.command(apiMethod=apiMethod,
                                            apiPath=apiPath,
                                            apiParams=apiParams)
                ap['top_n_apps'] = base_resp['msg']['result']['app_id']
    return site_list

def central_get_apprf(site_list):
    global central_obj
    # Sample API call using 'ArubaCentralBase.command()'
    # GET groups from Aruba Central
    apiPath = "/apprf/datapoints/v2/topn_stats"
    apiMethod = "GET"
    
    sites = [site['name'] for site in site_list]
    sites_data = {}
    for site in sites:
        if site != 'ATM-Demo':
            continue
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

    update_influxdb('apprf', sites_data)

def central_get_presence(site_list):
    global central_obj
    #siteList = [160, 431, 2, 130, 81, 124]
    presenceObj = {}
    startTime = int(time.time() - 300)
    endTime = int(time.time())
    sites = [site['id'] for site in site_list]
    for siteID in sites:
        apiPath = "/presence/v3/insights/sites/aggregates"
        apiMethod = "GET"
        apiParams = {
			"start_time": startTime,
			"end_time": endTime,
			"site_id": siteID
		}
        base_resp = central_obj.command(apiMethod=apiMethod,
                                apiPath=apiPath,
                                apiParams=apiParams)
        if (base_resp['code'] == 200):
            presenceData = base_resp['msg']['data'][0]
            presenceKeys = ['drawrate_count', 'dwelltime_count', 'loyal_visitors', 'new_visitors', 'passerby_count', 'total_connected', 'visitor_count', 'visits']
            emptySite = True
            for key in presenceKeys:
                if presenceData[key] != None:
                    emptySite = False
                    continue
            # pdb.set_trace()

            if (not emptySite):
                presenceObj[presenceData['site_name']] = presenceData
    #pdb.set_trace()
    update_influxdb('presence', presenceObj)


def update_influxdb(topic, sites_data):
    global influxdb_obj
    json_body = []
    #pdb.set_trace()
    if (topic == 'apprf'):
        pprint('**processing APPRF DATA****')
        for site in sites_data.keys():
            data = sites_data[site]
            if len(data['result']['app_cat']) == 0:
                continue
            #timestamp = int(data['result']['app_cat'][0]['timestamp'])
            #timestamp = time.time()
            field_data = {
                "measurement": "apprfData",
                "tags": {
                    "topic": "apprf",
                    "site": site
                },
                "fields": {}
            }
            field_tmp = {}
        for app_data in data['result']['app_cat']:
            field_data['tags']['app_cat'] = app_data['name']
            field_tmp['name'] = app_data['name']
            field_tmp['percent_usage'] = app_data['percent_usage']
            field_data["fields"] = field_tmp.copy()
            json_body.append(field_data.copy())
            result = influxdb_obj.write_points(points=json_body.copy(), database='atm23')
            #print(streaming_data['topic'] + " Database write: "+ result)
            print(f'{topic} Database write: {result}')
    elif (topic == 'presence'):
        for site_data in sites_data.values():
            site_name = site_data['site_name']
            site_data.pop('site_name')
            site_data.pop('store_name')
            #timestamp = data['result']['app_cat'][0]['timestamp']
            field_data = {
                "measurement": "presenceRESTData",
                "tags": {
                    "topic": "presence",
                    "site": site_name
                },
                "fields": site_data
            }
            json_body.append(field_data.copy())
    elif (topic == "apData"):
        entry_time = int(time.time())
        for [site_name, site_aps] in sites_data.items():
            #pdb.set_trace()
            for ap in site_aps:
                ap_fields = ['client_count', 'cpu_utilization', 'macaddr', 'mem_free', 'mem_total', 'model', 'name', 'serial', 'status', 'utilization']
                {k: ap[k] for k in ap.keys() & ap_fields}
                field_data = {
                    "measurement": "apData",
                    "tags": {
                        "topic": "apMonitoring",
                        "site": site_name,
                        "ap": ap['serial']
                    },
                    #"time": entry_time,
                    "fields": {k: ap[k] for k in ap.keys() & ap_fields}
                }
                json_body.append(field_data)
                for client in ap['clients']:
                    field_data = {
                        "measurement": "apData",
                        "tags": {
                            "topic": "apClients",
                            "site": site_name,
                            "ap": ap['serial'],
                            "client_mac": client['macaddr']
                        },
                        #"time": entry_time,
                        "fields": client
                    }
                    json_body.append(field_data)
                for application in ap['top_n_apps']:
                    field_data = {
                        "measurement": "apData",
                        "tags": {
                            "topic": "applicationData",
                            "site": site_name,
                            "ap": ap['serial'],
                            "appName": application['name']
                        },
                        #"time": entry_time,
                        "fields": application
                    }
                    json_body.append(field_data)

    #pdb.set_trace()
    try:
        result = influxdb_obj.write_points(points=json_body.copy(), database='atm23')
        #print(streaming_data['topic'] + " Database write: "+ result)
        print(f'{topic} Database write: {result}')
        if result == False:
            print("DB push failed!!!")
    except Exception as err:
        print(err)

def establish_influx_conn(host='', port=8086, username='', password='', ssl=True, verify_ssl=True, database=''):
    #import pdb
    global influxdb_obj
    host = 'localhost'
    port = 8086
    username = 'admin'
    password = 'Aruba#123!'
    ssl = False
    verify_ssl = False
    client = None
    database = 'atm23'
    #pdb.set_trace()
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
    
 #   parser = define_arguments()
#    args = parser.parse_args()

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
    
    central_get_data()
    # replace prom_collector with function we want repeated
    #rt = RepeatedTimer(REST_INTERVAL_SECONDS, central_get_data) # it auto-starts, no need of rt.start()

    try:
        while True:
            pass
    except:
        rt.stop() # better in a try/finally block to make sure the program ends!
        print("Goodbye!")
