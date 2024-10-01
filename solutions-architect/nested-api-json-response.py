#!/usr/bin/env python
# coding: utf-8

# In[1]:

import os

import json 

import pandas as pd

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

import io

import csv

import datetime

connect_str = ''
    
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = "***"

container_client = blob_service_client.get_container_client(container_name)

blob_list = container_client.list_blobs()

for blob in blob_list:
    
    current_timestamp = datetime.datetime.now()
    
    blob_name = blob.name

    blob_client = blob_service_client.get_blob_client("***", blob_name)

    parent_id=blob_name.partition("***-")[2].partition("-***")[0]

    streamdownloader = blob_client.download_blob()

    fileReader = json.loads(streamdownloader.readall())


    output = io.StringIO()

    df=pd.DataFrame(fileReader['***']['***'])

    output = df.to_csv(encoding = "utf-8", quotechar='"', quoting=csv.QUOTE_ALL, index=False)

    file_name = "***-" + str(parent_id) +  ".csv"

    blob_client = blob_service_client.get_blob_client("***", file_name)

    blob_client.upload_blob(output, blob_type="BlockBlob", overwrite=True)



    output2 = io.StringIO()

    df2=pd.DataFrame(fileReader['***']['***'])

    output2 = df2.to_csv(encoding = "utf-8", quotechar='"', quoting=csv.QUOTE_ALL, index=False)

    file_name2 = "***-" + str(parent_id) +  ".csv"

    blob_client2 = blob_service_client.get_blob_client("***", file_name2)

    blob_client2.upload_blob(output2, blob_type="BlockBlob", overwrite=True)


    output3 = io.StringIO()

    df3=pd.DataFrame(fileReader['***']['***'])

    output3 = df3.to_csv(encoding = "utf-8", quotechar='"', quoting=csv.QUOTE_ALL, index=False)

    file_name3 = "***-" + str(parent_id) +  ".csv"

    blob_client3 = blob_service_client.get_blob_client("***", file_name3)

    blob_client3.upload_blob(output3, blob_type="BlockBlob", overwrite=True)


    output4 = io.StringIO()

    df4=pd.DataFrame(fileReader['***']['***'])

    output4 = df4.to_csv(encoding = "utf-8", quotechar='"', quoting=csv.QUOTE_ALL, index=False)

    file_name4 = "***-" + str(parent_id) +  ".csv"

    blob_client4 = blob_service_client.get_blob_client("***", file_name4)

    blob_client4.upload_blob(output4, blob_type="BlockBlob", overwrite=True)


    output5 = io.StringIO()

    df5=pd.DataFrame(fileReader['***']['***'])

    output5 = df5.to_csv(encoding = "utf-8", quotechar='"', quoting=csv.QUOTE_ALL, index=False)

    file_name5 = "***-" + str(parent_id) +  ".csv"

    blob_client5 = blob_service_client.get_blob_client("***", file_name5)

    blob_client5.upload_blob(output5, blob_type="BlockBlob", overwrite=True)



    output6 = io.StringIO()

    df6=pd.DataFrame(fileReader['***']['***'])

    output6 = df6.to_csv(encoding = "utf-8", quotechar='"', quoting=csv.QUOTE_ALL, index=False)

    file_name6 = "***-" + str(parent_id) +  ".csv"

    blob_client6 = blob_service_client.get_blob_client("***", file_name6)

    blob_client6.upload_blob(output6, blob_type="BlockBlob", overwrite=True)
	

    output7 = io.StringIO()

    df7=pd.DataFrame(fileReader['***']['***'])

    output7 = df7.to_csv(encoding = "utf-8", quotechar='"', quoting=csv.QUOTE_ALL, index=False)

    file_name7 = "***-" + str(parent_id) +  ".csv"

    blob_client7 = blob_service_client.get_blob_client("***", file_name7)

    blob_client7.upload_blob(output7, blob_type="BlockBlob", overwrite=True)


    output8 = io.StringIO()

    df8=pd.DataFrame(fileReader['***']['***'])

    output8 = df8.to_csv(encoding = "utf-8", quotechar='"', quoting=csv.QUOTE_ALL, index=False)

    file_name8 = "***-" + str(parent_id) +  ".csv"

    blob_client8 = blob_service_client.get_blob_client("***", file_name8)

    blob_client8.upload_blob(output8, blob_type="BlockBlob", overwrite=True)