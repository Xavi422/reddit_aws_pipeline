import json
import urllib.parse
import boto3
import pandas as pd
from io import BytesIO

# create s3 client
s3 = boto3.client('s3')


def lambda_handler(event, context):
    # Get the bucket and object key from the event
    raw_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    key = urllib.parse.unquote_plus(key)
    
    # Download the raw file
    raw_file = s3.get_object(Bucket=raw_bucket, Key=key)
    raw_df = pd.read_csv(BytesIO(raw_file['Body'].read()))
    
    # perform transformations
    transformed_df = raw_df.copy()
    
    # create score column
    transformed_df['score'] = transformed_df['ups'] - transformed_df['downs']
    transformed_df = transformed_df.drop(columns=['ups','downs'])
    
    # convert utc datetime to human-readable
    transformed_df['created_utc'] = pd.to_datetime(transformed_df['created_utc'],unit='s')
    
    # Convert the transformed DataFrame back to CSV and upload to S3
    out_buffer = BytesIO()
    transformed_df.to_csv(out_buffer, index=False)
    out_buffer.seek(0)
    
    transformed_bucket = '-'.join(raw_bucket.split('-')[:-1] + ['transformed'])
    s3.put_object(Bucket=transformed_bucket, Key=key, Body=out_buffer)
    
    return {
        'statusCode': 200,
        'body': f'File {key} transformed and uploaded to {transformed_bucket} successfully.'
    }