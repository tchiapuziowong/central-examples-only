# MIT License
#
# Copyright (c) 2019 Aruba, a Hewlett Packard Enterprise company
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import re
import json
import csv
import sys
import threading
from pprint import pprint
from google.protobuf import json_format
from proto import streaming_pb2
from proto.monitoring_pb2 import MonitoringInformation
from proto.apprf_pb2 import apprf_session
from proto.presence_pb2 import presence_event
from proto.audit_pb2 import audit_message
from proto.location_pb2 import stream_location
from proto.security_pb2 import RapidsStreamingEvent

import base64
threadLock = threading.Lock()

class Decoder():
    def __init__(self, topic):
        self.event_decoder = self.get_message_decoder(topic)
        self.topic = topic

    def get_message_decoder(self, topic):
        """
        This function returns the decoder based on subscription topic of
        streaming API. The decoder decodes message based on compiled proto files.
        """
        decoder = None
        if topic == "apprf":
            decoder = apprf_session()
        elif topic == "audit":
            decoder = audit_message()
        elif topic == "location":
            decoder = stream_location()
        elif topic == "monitoring":
            decoder = MonitoringInformation()
        elif topic == "presence":
            decoder = presence_event()
        elif topic == "security":
            decoder = RapidsStreamingEvent()
        return decoder

    def decodeData(self, msg):
        """
        This function decodes the received streaming API data from protobuf
        to python dictionary using the compiled proto definition.

        Params:
            msg: Streaming API message in google protobuf format.
        Returns:
            stream_info (dict): A Python dictionary to represent received data.
        """
        stream_info = {}
        try:
            # Decode Streaming data
            stream_data = streaming_pb2.MsgProto()
            stream_data.ParseFromString(msg)
            stream_info = {
                "topic": stream_data.subject,
                "timestamp": stream_data.timestamp,
                "customer_id": stream_data.customer_id,
                "data": stream_data.data,
                "msp_ip": stream_data.msp_id
            }
        except Exception as e:
            raise e

        try:
            if stream_info:
                data_decoder = self.event_decoder
                data_decoder.ParseFromString(stream_info["data"])
                stream_info["data"] = json_format.MessageToDict(data_decoder, preserving_proto_field_name=True)
            return stream_info
        except Exception as e:
            print("Exception Received for customer " +
                  "%s: %s" % (self.topic, str(e)))

class presenceExport():
    def __init__(self, topic, export_type, db_conn):
        self.export_type = export_type
        self.subject = topic
        self.decoder = Decoder(topic)
        self.db_conn = db_conn

    def processor(self, data):
        """
        A function to process the received data and provide means for data
        transport/storage.
        """
        streaming_data = self.decoder.decodeData(data)
        #byte_mac = base64.b64decode(streaming_data['data']['sta_eth_mac']['addr'])
        #readable_mac = ':'.join('%02x' % byte for byte in byte_mac)
        #streaming_data['data']['pa_proximity_event'] = readable_mac
        
        # Add Your code here to process data and handle transport/storage
        field_data = None
       # pdb.set_trace()
        if (streaming_data['data']['event_type'] == "proximity"):
            for presence_event in streaming_data['data']['pa_proximity_event']['proximity']:

                ap_byte_mac = base64.b64decode(presence_event['ap_eth_mac']['addr']).decode()
                ap_mac = ":".join(ap_byte_mac[i:i+2] for i in range(0, len(ap_byte_mac), 2))
                sta_byte_mac = base64.b64decode(presence_event['sta_eth_mac']['addr']).decode()
                sta_mac = ":".join(sta_byte_mac[i:i+2] for i in range(0, len(sta_byte_mac), 2))

                field_data = {
                    "ap_eth_mac": ap_mac,
                    "device_id": presence_event['device_id'],
                    "rssi_val": presence_event['rssi_val'],
                    "sta_eth_mac": sta_mac
                }
                json_body = [{
                    "measurement": "presenceData",
                    "tags": {
                        "topic": streaming_data['topic'],
                        "customer_id": streaming_data['customer_id'],
                        "type": "proximity"
                    },
                    "time": streaming_data['timestamp'],
                    "fields": field_data
                }]
                try:
                    result = self.db_conn.write_points(points=json_body, database='atm23')
                    print(f"{streaming_data['topic']} ({streaming_data['data']['event_type']}) - Database write: + {result}")
                    if result == False:
                        print("DB push failed!!!")
                except Exception as err:
                    print(err)
        elif (streaming_data['data']['event_type'] == "rssi"):
            for presence_event in streaming_data['data']['pa_rssi_event']['rssi']:

                ap_byte_mac = base64.b64decode(presence_event['ap_eth_mac']['addr']).decode()
                ap_mac = ":".join(ap_byte_mac[i:i+2] for i in range(0, len(ap_byte_mac), 2))
                sta_byte_mac = base64.b64decode(presence_event['sta_eth_mac']['addr']).decode()
                sta_mac = ":".join(sta_byte_mac[i:i+2] for i in range(0, len(sta_byte_mac), 2))
                field_data = {
                    "ap_eth_mac": ap_mac,
                    "device_id": presence_event['device_id'],
                    "rssi_val": presence_event['rssi_val'],
                    "sta_eth_mac": sta_mac,
                    "noise_floor": presence_event['noise_floor']
                }
                json_body = [{
                    "measurement": "presenceData",
                    "tags": {
                        "topic": streaming_data['topic'],
                        "customer_id": streaming_data['customer_id'],
                        "type": "rssi"
                    },
                    "time": streaming_data['timestamp'],
                    "fields": field_data
                }]
                try:
                    result = self.db_conn.write_points(points=json_body, database='atm23')
                    #print(streaming_data['topic'] + streaming_data['data']['event_type'] + " Database write: "+ result)
                    print(f"{streaming_data['topic']} ({streaming_data['data']['event_type']}) - Database write: + {result}")
                    if result == False:
                        print("DB push failed!!!")
                except Exception as err:
                    print(err)
class securityExport():
    def __init__(self, topic, export_type, db_conn):
        self.export_type = export_type
        self.subject = topic
        self.decoder = Decoder(topic)
        self.db_conn = db_conn

    def processor(self, data):
        """
        A function to process the received data and provide means for data
        transport/storage.
        """
        streaming_data = self.decoder.decodeData(data)
        # Add Your code here to process data and handle transport/storage

class monitoringExport():
    def __init__(self, topic, export_type, db_conn):
        self.export_type = export_type
        self.subject = topic
        self.decoder = Decoder(topic)
        self.db_conn = db_conn

    def processor(self, data):
        """
        A function to process the received data and provide means for data
        transport/storage.
        """
        streaming_data = self.decoder.decodeData(data)
        # Add Your code here to process data and handle transport/storage

class locationExport():
    def __init__(self, topic, export_type, db_conn):
        self.export_type = export_type
        self.subject = topic
        self.decoder = Decoder(topic)
        self.db_conn = db_conn

    def processor(self, data):
        """
        A function to process the received data and provide means for data
        transport/storage.
        """
        streaming_data = self.decoder.decodeData(data)
        # Add Your code here to process data and handle transport/storage
        byte_mac = base64.b64decode(streaming_data['data']['sta_eth_mac']['addr'])       
        readable_mac = ':'.join('%02x' % byte for byte in byte_mac)
        streaming_data['data']['sta_eth_mac'] = readable_mac
        
        if self.db_conn and self.export_type == 'influxdb':
            ## push data to influx
            json_body = [{
                "measurement": "locationData",
                "tags": {
                    "topic": streaming_data['topic'],
                    "customer_id": streaming_data['customer_id']
                    },
                "time": streaming_data['timestamp'], 
                "fields": streaming_data['data']
                }]
            try:
                result = self.db_conn.write_points(points=json_body, database='atm23')
                #print(streaming_data['topic'] + " Database write: "+ result)
                print(f'{streaming_data["topic"]} Database write: {result}')
                if result == False:
                    print("DB push failed!!!")
            except Exception as err:
                print(err)

        # verify DB data
        #out = self.db_conn.query(query='SELECT * FROM "atm23"."autogen"', database='atm23')
        #print("Influx Data query!!!!!!!!!!!!!!!!!!!")
        #print(out)

class apprfExport():
    def __init__(self, topic, export_type, db_conn):
        self.export_type = export_type
        self.subject = topic
        self.decoder = Decoder(topic)
        self.db_conn = db_conn

    def processor(self, data):
        """
        A function to process the received data and provide means for data
        transport/storage.
        """
        streaming_data = self.decoder.decodeData(data)
        # Add Your code here to process data and handle transport/storage

        if self.db_conn and self.export_type == 'influxdb':
            field_dict = {'dest_url_prefix': streaming_data['data']['dest_url_prefix'],
                          }
            ## push data to influx
            json_body = [{
                "measurement": streaming_data['topic']+"Data",
                "tags": {
                    "topic": streaming_data['topic'],
                    "customer_id": streaming_data['customer_id']
                    },
                "time": streaming_data['timestamp'],
                "fields": field_dict
                }]
            try:
                result = self.db_conn.write_points(points=json_body, database='atm23')
                print(streaming_data['topic'] + " Database write: "+ result)
                if result == False:
                    print("DB push failed!!!")
            except Exception as err:
                print(err)

        print(streaming_data)
        # Add Your code here to process data and handle transport/storage

class auditExport():
    def __init__(self, topic, export_type, db_conn):
        self.export_type = export_type
        self.subject = topic
        self.decoder = Decoder(topic)
        self.db_conn = db_conn

    def processor(self, data):
        """
        A function to process the received data and provide means for data
        transport/storage.
        """
        streaming_data = self.decoder.decodeData(data)
        print(streaming_data)
        # Add Your code here to process data and handle transport/storage

class dataHandler ():
    def __init__(self, msg, class_inst_obj):
        self.msg = msg
        self.class_inst_obj = class_inst_obj

    def run(self):
        """
        This function runs the processor of mentioned class obj.
        """
        self.class_inst_obj.processor(self.msg)

class writeThread (threading.Thread):
    """
    A Class inherited from threading package in python to be used during
    streaming API data processing.
    """
    def __init__(self, msg, class_inst_obj):
        threading.Thread.__init__(self)
        self.msg = msg
        self.class_inst_obj = class_inst_obj

    def run(self):
        threadLock.acquire()
        # Add your task
        threadLock.release()
