import os  

import json  

import pandas as pd 

import requests  

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient 

import io 

import csv 

from collections import ChainMap

connect_str = '***' 

blob_service_client = BlobServiceClient.from_connection_string(connect_str) 

container_name = "staging-blob" 

container_client = blob_service_client.get_container_client(container_name) 

blob_list = container_client.list_blobs() 

for blob in blob_list: 

    if blob.name == '***.json': 

        blob_name = blob.name  

        blob_client = blob_service_client.get_blob_client("staging-blob", blob_name) 

        streamdownloader = blob_client.download_blob() 
        
        fileReader = json.loads(streamdownloader.readall()) 

        df=pd.DataFrame(fileReader)   
        
        for index, row in df.iterrows():
            
            file_name3 = "***-" + str(row['id']) +  ".json" 
            
            ***_json = row.to_json()
            
            merged_raw_json = json.loads(***_json)
            
            *** = row['id']
    
            *** = row['sample_id']

            url2 = '***'+str(***)+'/***[]='+str(***)

            headers2 = {'Authorization': 'Bearer ***', 'Content-Type': 'application/json'} 

            res = requests.get(url2, headers=headers2) 
            
            abc_json = res.text
            
            url3 = '***'+str(***)+'/***/'+str(***)+'/***'

            res3 = requests.get(url3, headers=headers2) 

            xyz_json = res3.text
        
            merged_raw_json["abc"] = []
            
            merged_raw_json.update({"abc": abc_json})
            
            merged_raw_json["xyz"] = []
            
            merged_raw_json.update({"xyz": xyz_json})
                
            blob_client3 = blob_service_client.get_blob_client("***", file_name3) 

            blob_client3.upload_blob(json.dumps(merged_raw_json), blob_type="BlockBlob", overwrite=True)