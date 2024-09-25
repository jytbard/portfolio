import boto3
from elasticsearch import Elasticsearch
import json
import requests

bedrock = boto3.client('bedrock', region_name='****') 

def generate_response_with_bedrock(query, retrieved_documents):
    # Prepare the input prompt
    prompt = format_bedrock_prompt(query, retrieved_documents)

    # Send the prompt to the Bedrock API for text generation
    response = bedrock.invoke_model(
        modelId="****",  
        body={
            "prompt": prompt,
            "maxTokens": 200,  # Limit the response length
            "temperature": 0.7 
        }
    )
    
    # Extract and return the generated text
    return response["generated_text"]

def format_bedrock_prompt(query, retrieved_documents):
    # Combine retrieved documents and the user query into a single prompt
    context = "\n".join(retrieved_documents)
    prompt = f"Context:\n{context}\n\nUser Query:\n{query}\n\nAnswer the query based on the context above."
    return prompt

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Search function to retrieve relevant documents
def retrieve_documents(query):
    search_query = {
        "query": {
            "match": {
                "content": query
            }
        }
    }

    # Search for the relevant documents
    result = es.search(index="knowledge_base", body=search_query)

    # Extract and return the relevant documents
    documents = [hit["_source"]["content"] for hit in result["hits"]["hits"]]
    return documents

def rag_pipeline(query):
    # Step 1: Retrieve relevant documents from the knowledge base
    retrieved_documents = retrieve_documents(query)

    # Step 2: Generate response using Bedrock based on the query and retrieved context
    response = generate_response_with_bedrock(query, retrieved_documents)

    return response


TELEGRAM_API_URL = f"https://api.telegram.org/****/sendMessage"

BEDROCK_CLIENT = boto3.client('bedrock')

def lambda_handler(event, context):
    message = json.loads(event['body'])
    chat_id = message['message']['chat']['id']
    query = message['message']['text']

    # Retrieve documents related to the query
    retrieved_docs = retrieve_documents(query)

    # Generate response from Amazon Bedrock using RAG
    context = "\n".join(retrieved_docs)
    response_text = generate_response_with_bedrock(query, context)

    # Send the response back to the user
    send_message_to_telegram(chat_id, response_text)

    return {
        'statusCode': 200,
        'body': json.dumps('Message processed successfully!')
    }

def send_message_to_telegram(chat_id, text):
    payload = {
        'chat_id': chat_id,
        'text': text
    }
    requests.post(TELEGRAM_API_URL, json=payload)
