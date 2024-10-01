import requests

import json

import os

import pandas as pd

from datetime import timezone

import datetime 

import math

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

connect_str = ''

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = "staging-blob"

snapshot_time = datetime.datetime.now(timezone.utc)

data_source = '***'

url = '***'

headers = {'Authorization': 'Bearer ', 'user-agent':'***', 'Content-Type': 'application/json'} 
    

body_data = { 

    "PageConfig": { 

        "Filter": {} , 

        "Start":0, 

        "Limit":1 

    } 

} 

response = requests.post(url, headers=headers, json=body_data) 

data_dict = json.loads(response.text)

df1 = pd.DataFrame.from_dict(data_dict)

total_pages = math.ceil(df1['Result']['Total'] / 10000)

#print(total_pages)

for x in range(1, total_pages):
    
    if(x>1):
        
        start = 10000*(x-1)+1
        
    else:
        
        start = 0
    
    body_data2 = { 

    "PageConfig": { 

        "Filter": {} , 

        "Start":start,
                
        "Limit":10000

    } 

} 


    response2 = requests.post(url, headers=headers, json=body_data2) 
    
    data_dict = json.loads(response2.text)

    data_dict['data_source'] = data_source

    data_dict['snapshot_time'] = snapshot_time

    json_data1 = json.dumps(data_dict, default=str)
    
    file_name2 = '***-'+str(x)+'.json'

    container_client = blob_service_client.get_container_client(container_name)

    blob_client = blob_service_client.get_blob_client(container_name, file_name2)

    blob_client.upload_blob(json_data1, blob_type="BlockBlob",overwrite=True)