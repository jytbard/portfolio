from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_openai import OpenAI
import pandas as pd
import boto3
from io import StringIO


def query_agent(query):
   
    # Set your AWS credentials and region
    aws_access_key_id = '****'
    aws_secret_access_key = '****'
    aws_region = '****'

    # Set the S3 bucket and file path
    s3_bucket = '****'
    s3_file_path = 'patients-data.csv'

    # Create a session with boto3
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )

    # Create an S3 client
    s3 = session.client('s3')

    # Read the CSV file from S3 into a pandas dataframe
    s3_object = s3.get_object(Bucket=s3_bucket, Key=s3_file_path)
    s3_data = s3_object['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(s3_data))

    llm = OpenAI()
    
    # Create a Pandas DataFrame agent.
    agent = create_pandas_dataframe_agent(llm, df, verbose=True)

    #Python REPL: A Python shell used to evaluating and executing Python commands. 
    #It takes python code as input and outputs the result. The input python code can be generated from another tool in the LangChain
    return agent.invoke(query)