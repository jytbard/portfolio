import os

import json 

import pandas as pd

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

import io

import csv

import datetime

import re

def reg(m):
    return m.group(1)

def escape_single_quote_in_json(s):

    rstr = ""

    escaped = False

    for c in s:
    
        if c == "'" and not escaped:
            c = "\s"  
        elif c == '"':
            c = '\\' + c
   
        escaped = (c == "\\")

        rstr += c 
    
    return rstr

connect_str = '***'
    
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = "***"

container_client = blob_service_client.get_container_client(container_name)

blob_list = container_client.list_blobs()

for blob in blob_list:
    
    if blob.name == '***.json':
    
        blob_name = blob.name

        blob_client = blob_service_client.get_blob_client("***", blob_name)

        streamdownloader = blob_client.download_blob()

        fileReader = json.loads(streamdownloader.readall())

        df=pd.DataFrame(fileReader)

        sql_stmt = ''

        for i in df['***']['***']:

            sql_stmt += "insert into \"***\".\"***\".\"***\" (***) (select PARSE_JSON('%s'))" % in_(json.dumps(i))+',\n'

new_string = re.sub("(.*)(.{1}$)", reg, sql_stmt)

file_name = "***.json"

blob_client = blob_service_client.get_blob_client("***", file_name)

blob_client.upload_blob(new_string, blob_type="BlockBlob", overwrite=True)