import json
import boto3
import urllib.request
import datetime

eventbridge = boto3.client('events')

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    date = datetime.date.today().strftime('%Y%m%d')
    

    def get_s3_object_count(bucket_name,key):
        key_count = s3.list_objects_v2(Bucket=bucket_name, Prefix=key)['KeyCount']
        return key_count
        
    ***_array = ['active','inactive']
    
    for element in ***_array:
        objCount = get_s3_object_count('***','***/'+element+'/***'+element+'_data.csv')
        if objCount != 0:
            file_response = s3.get_object(Bucket='***', Key='***/'+element+'/***'+element+'_***.csv')
            
            file_content = file_response['Body'].read().decode('utf-8')
            
            s3.put_object(Bucket='***', Key='***'+element+'/'+date+'/***'+element+'***.csv', Body=file_content)
        
        url = '***'
        
        with urllib.request.urlopen(url) as response:
            data = response.read()
            s3.put_object(Bucket='***', Key='***/'+element+'/***'+element+'***.csv', Body=data)
    
    # Send the event to the target rule
    response = eventbridge.put_events(
        Entries=[
            {
                'Source': 'arn:aws:lambda:us-east-1:***:function:copyGitHubS3',
                'DetailType': 'Lambda Function Execution Status Change',
                'Detail': json.dumps({'status': 'success'})
            }
        ]
    )
    print(response['Entries'])
    
    # Check the response and handle any errors
    if response['FailedEntryCount'] > 0:
        print(f"Failed to publish event: {response['Entries']}")
    else:
        print("Successfully published event to EventBridge bus")
    
    return {
        'statusCode': 200,
        'body': json.dumps('csv file copy from github to lambda has been completed!')
    }
