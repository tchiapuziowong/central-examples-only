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

# influxdb client
from influxdb import InfluxDBClient

# Import Aruba Central Base
from pycentral.base import ArubaCentralBase
from pprint import pprint

global central_obj
global influxdb_obj

def central_get_location_mappings():
    global central_obj
    # Sample API call using 'ArubaCentralBase.command()'
    # GET groups from Aruba Central
    apiPaths = { "all_campuses": "/visualrf_api/v1/campus?limit=100",
                "campus_info": "/visualrf_api/v1/campus/{0}?limit=100",
                "building_info": "/visualrf_api/v1/building/{0}?limit=100&units=FEET"           
    }
    
    apiMethod = "GET"

    campuses = central_obj.command(apiMethod=apiMethod,
                                    apiPath=apiPaths["all_campuses"],
                                    apiParams={})
    
    campuses = campuses['msg']
    campus_dict = {}

    for campus in campuses['campus']:
        campus_dict[campus['campus_id']] = {}
        tmp_campus_api = apiPaths['campus_info'].format(campus['campus_id'])
        campus_info = central_obj.command(apiMethod=apiMethod,        
                                          apiPath=tmp_campus_api,
                                          apiParams={})
        campus_info = campus_info['msg']

        for building in campus_info['buildings']:
            tmp_building_api = apiPaths['building_info'].format(building['building_id'])
            building_info = central_obj.command(apiMethod=apiMethod,
                                                apiPath=tmp_building_api,
                                                apiParams={})
            building_info = building_info['msg']
            campus_dict[campus['campus_id']][building['building_id']] = {}
            campus_dict[campus['campus_id']][building['building_id']] = building_info['building'].copy()
            if 'floors' not in building_info.keys():
                continue
            campus_dict[campus['campus_id']][building['building_id']]['floors'] =  building_info['floors'].copy()

    update_influxdb(campus_dict, topic='location')

def central_get_apprf_mappings():
    global central_obj
    # Sample API call using 'ArubaCentralBase.command()'
    # GET groups from Aruba Central
    apiPaths = { "web_rep_score": "/apprf/v1/metainfo/iap/webreputation/id_to_name",
                "app_id": "/apprf/v1/metainfo/iap/application/id_to_name",
                "app_cat_id": "/apprf/v1/metainfo/iap/appcategory/id_to_name",
                "web_cat_id": "/apprf/v1/metainfo/iap/webcategory/id_to_name"                
    }
    
    apiMethod = "GET"

    category_mappings = {}
    for category in apiPaths.keys():
        base_resp = central_obj.command(apiMethod=apiMethod,
                                    apiPath=apiPaths[category],
                                    apiParams={})
        category_mappings[category] = base_resp['msg'].copy()

    update_influxdb(category_mappings)

def update_influxdb(category_mappings, topic='apprf'):
    global influxdb_obj
    json_body = []

    if topic == 'location':
        for campus in category_mappings.keys():
            field_data = {
                "measurement": "building_mapping",
                "tags": {
                    "campus_id": campus,
                },
                "fields": {}
            }
            for building in category_mappings[campus].keys():
                building_dict = category_mappings[campus][building]
                field_data['fields']['building_id'] = building_dict['building_id']
                if 'floors' in building_dict.keys():
                    floors_list = building_dict['floors']
                    building_dict.pop('floors')
                    for floor in floors_list:
                        field_data["tags"]["floor_id"] = floor["floor_id"]
                        field_data["fields"].update(floor)               
                        field_data["fields"].update(building_dict)
                        try:
                            result = influxdb_obj.write_points(points=[field_data.copy()], database='atm23')
                            print(f'Location Mapping Database write: {result}')
                            if result == False:
                                print("DB push failed!!!")
                        except Exception as err:
                            print(err)
                else:
                    field_data["fields"].update(building_dict)
                    try:
                        result = influxdb_obj.write_points(points=[field_data.copy()], database='atm23')
                        print(f'Location Mapping Database write: {result}')
                        if result == False:
                            print("DB push failed!!!")
                    except Exception as err:
                        print(err)

    elif topic == 'apprf':
        for category in category_mappings.keys():
            field_data = {
                "measurement": category+"_mapping",
                "tags": {
                    "topic": category
                },
                "fields": {}
            }
            for id in category_mappings[category].keys():
                field_data["fields"] = {'id': id,
                                        'value': category_mappings[category][id]}
                try:
                    result = influxdb_obj.write_points(points=[field_data.copy()], database='atm23')
                    #print(streaming_data['topic'] + " Database write: "+ result)
                    print(f'AppRF Mapping Database write: {result}')
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
    
    #central_get_apprf_mappings()
    central_get_location_mappings()
