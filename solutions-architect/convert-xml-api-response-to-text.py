#!/usr/bin/env python
# coding: utf-8

import xml.etree.ElementTree as ET

import os

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def get_details():
    connection_string = ''
    container_name = 'response-data'
    return connection_string, container_name

def get_clients_with_connection_string():
    connection_string, container_name = get_details()
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    return container_client

def read_from_blob(blob_file_path):
    container_client = get_clients_with_connection_string()
    blob_client = container_client.get_blob_client(blob_file_path)
    byte_data= blob_client.download_blob().readall()
    return byte_data

blob_file_path = "devices-list.xml"

byte_data = read_from_blob(blob_file_path)

connect_str = ''

blob_service_client2 = BlobServiceClient.from_connection_string(connect_str)

container_name2 = "response-data"

file_name2 = "devices-list.txt"

container_client2 = blob_service_client2.get_container_client(container_name2)

blob_client2 = blob_service_client2.get_blob_client(container_name2, file_name2)

root = ET.fromstring(byte_data)

line_to_write = 'name,udid,id,Display_Name\n'
    
for mobile_devices in root.iter('mobile_device'):
    
    name = mobile_devices.find('name').text
    
    udid = mobile_devices.find('udid').text
    
    id = mobile_devices.find('id').text
    
    Display_Name = mobile_devices.find('Display_Name').text
        
    line_to_write += '"'+ name + '","' + udid + '","' + id + '","' + Display_Name + '"' + os.linesep
    
    blob_client2.upload_blob(line_to_write, blob_type="BlockBlob",overwrite=True)

