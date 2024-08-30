from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd

from datetime import datetime, timedelta
import requests
import sys
import os
import logging


# set the parent folder of the dags folder to the first lookup location in the path so the interpreter can find modules
#  /opt/airflow folder in docker container
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import CLIENT_ID, CLIENT_SECRET, REDDIT_USER_NAME, REDDIT_PASSWORD, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

OAUTH_URL = 'https://oauth.reddit.com/'
USER_AGENT = 'RedditDE/0.0.1 by u/Xavi422'

# create connection to AWS
conn = Connection(
    conn_id = 'aws_default',
    conn_type = 'aws',
    login=AWS_ACCESS_KEY_ID,
    password=AWS_SECRET_ACCESS_KEY,
    extra={'region_name':'us-east-1'}
)

# generate connection to aws
env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
conn_uri = conn.get_uri()
os.environ[env_key] = conn_uri


# define yesterday and default args
yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'catchup': False
}



@dag(default_args=default_args, schedule=None)
def extract_data_dag():

    # get Reddit API access token
    @task()
    def get_access_token() -> str:
        try:
            # create auth object
            auth = requests.auth.HTTPBasicAuth(CLIENT_ID,CLIENT_SECRET)
            
            # post request data and headers
            data = {'grant_type':'password', 'username':REDDIT_USER_NAME, 'password':REDDIT_PASSWORD}
            headers = {'User-Agent':USER_AGENT}

            # post request
            res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
            
            if res.status_code != 200:
                raise Exception('Could not authenticate client. Check credentials')
            
            # retrieve access token
            access_token = res.json().get('access_token')
            logging.info('Access token retrieved successfully')
            return access_token
            
        except Exception as e:
            logging.error(f'Error retrieving access token -> {e}')
            raise
    
    
    @task()
    def extract_posts(access_token: str, endpoint: str, subreddit: str, limit = None) -> str:
        
        #list to store posts data
        posts = []
        # desired fields
        fields = ['name','title','link_flair_text','ups','downs','total_awards_received','url','created_utc']
        
        try:
            # headers for get request
            headers = {'User-Agent':USER_AGENT}
            headers['Authorization'] = f'bearer {access_token}'

            # parameters for get request
            after = None
            params = {'limit':limit,'after':after}

            # loop posts in batches of <limit>
            while True:
                # send get request
                res = requests.get(f'{OAUTH_URL}r/{subreddit}/{endpoint}', headers=headers, params=params)
                if res.status_code != 200:
                    break
                
                # get data from response
                res_data = res.json()
                after = res_data['data']['after']

                # store data in dataframe
                for post in res_data['data']['children']:
                    post_data = post['data']
                    extract = {key:post_data[key] for key in fields}
                    posts.append(extract)

                if after is None:
                    break
                
                params['after'] = after            
            
            # create dataframe from posts list
            df = pd.DataFrame(posts)

            # add record load timestamp to DataFrame
            timestamp_dt = datetime.now()
            timestamp = timestamp_dt.strftime('%Y-%m-%d %H:%M:%S')
            df['rec_load_timestamp'] = timestamp
            
            # write dataframe to csv
            filename = f"data/output/{subreddit}_{endpoint}_{timestamp_dt.date()}.csv"
            df.to_csv(filename, index=False)
            return filename
        
        except Exception as e:
            logging.error(f'Error extracting posts from Reddit -> {e}')
            raise

    
    @task()
    def write_to_s3(filename: str,bucket_name = 'omscs-reddit-raw'):
        try:
            # create S3 hook
            s3_hook = S3Hook(aws_conn_id='aws_default')
            
            # upload file to S3
            key = filename.split('/')[-1]
            s3_hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
            logging.info(f'{filename} uploaded to S3 bucket {bucket_name}')
        
        except Exception as e:
            logging.error(f'Error uploading {filename} to S3 bucket {bucket_name} -> {e}')
            raise

    
    filename = extract_posts(get_access_token(), 'new', 'OMSCS', 100)
    write_to_s3(filename, bucket_name='omscs-reddit-raw')

extract_data_dag()

