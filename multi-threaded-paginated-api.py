import requests

import concurrent.futures

import string

import random

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


def make_post_request(url, payload):

    try:

        headers = {'Authorization': 'Bearer ***', 'user-agent':'***', 'Content-Type': 'application/json'} 

        response = requests.post(url, json=payload, headers=headers)
        
        return response.json()
    
    except Exception as e:

        return {"error": str(e)}

    
def multithreaded_post_requests(urls, payloads):

    results = []

    with concurrent.futures.ThreadPoolExecutor() as executor:

        future_to_url = {executor.submit(make_post_request, url, payload): url for url, payload in zip(urls, payloads)}

        for future in concurrent.futures.as_completed(future_to_url):

            url = future_to_url[future]

            try:

                data = future.result()

                results.append({url: data})

            except Exception as e:

                results.append({url: {"error": str(e)}})

    return results

if __name__ == "__main__":

    urls = ["***", "***"]

    payloads = [{ "PageConfig": { "Filter": {} , "Start":0, "Limit":10000 } } , { "PageConfig": { "Filter": {} , "Start":10001, "Limit":10000 } } ]  
    
    results = multithreaded_post_requests(urls, payloads)

    for result in results:
        
        res = ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=6))
        
        data_dict = json.dumps(result)

        file_name2 = '***-'+str(res)+'.json'

        container_client = blob_service_client.get_container_client(container_name)

        blob_client = blob_service_client.get_blob_client(container_name, file_name2)

        blob_client.upload_blob(data_dict, blob_type="BlockBlob",overwrite=True)