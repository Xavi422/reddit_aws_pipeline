from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.connection import Connection
import requests
import sys
import os
import logging
import pandas as pd

OAUTH_URL = 'https://oauth.reddit.com/'
USER_AGENT = 'RedditDE/0.0.1 by u/Xavi422'
# set the parent folder of the dags folder to the first lookup location in the path which the interpreter searches for modules
# this folder would be the airflow folder in the docker container since you mounted the volume to the dags folder
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import CLIENT_ID, CLIENT_SECRET, REDDIT_USER_NAME, REDDIT_PASSWORD, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# create connection to AWS
conn = Connection(
    conn_id = 'aws_default',
    conn_type = 'aws',
    login=AWS_ACCESS_KEY_ID,
    password=AWS_SECRET_ACCESS_KEY,
    extra={'region_name':'us-east-1'}
)

# generate connection
env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
conn_uri = conn.get_uri()
os.environ[env_key] = conn_uri


# define default args and constant yesterday
yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'catchup':False
}

# connect to Reddit and return Reddit instance
def get_access_token() -> str:
    try:
        # get access token
        auth = requests.auth.HTTPBasicAuth(CLIENT_ID,CLIENT_SECRET)
        # data for post request
        data = {'grant_type':'password', 'username':REDDIT_USER_NAME, 'password':REDDIT_PASSWORD}

        # headers for post request
        headers = {'User-Agent':USER_AGENT}

        # send post request
        res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
        
        if res.status_code != 200:
            raise Exception('Could not authenticate client. Check credentials')
        
        access_token = res.json().get('access_token')
        logging.info('Access token retrieved successfully')
        return access_token
        
    except Exception as e:
        raise

# extract posts from Reddit and store in dataframe
def extract_posts(access_token: str, subreddit: str, limit = None):
    # dataframe to store posts
    # used because the volume of data is not large and can fit in memory
    posts = pd.DataFrame()
    try:
        # headers for get request
        headers = {'User-Agent':USER_AGENT}
        headers['Authorization'] = f'bearer {access_token}'

        # parameters for get request
        after = None
        params = {'limit':limit,'after':after}

        while True:
            # send get request
            res = requests.get(f'{OAUTH_URL}r/{subreddit}/new', headers=headers, params=params)
            if res.status_code != 200:
                break
            
            # get data from response
            res_data = res.json()
            after = res_data['data']['after']

            # store data in dataframe
            for post in res_data['data']['children']:
                full_name = post['data']['name']
                title = post['data']['title']
                body = post['data']['selftext']
                flair = post['data']['link_flair_text']
                upvotes = post['data']['ups']
                downvotes = post['data']['downs']
                rewards_count = post['data']['total_awards_received']
                url = post['data']['url']
                created_at = post['data']['created_utc']
                
                posts = pd.concat([posts, pd.DataFrame.from_records([{'full_name':full_name,
                                                                      'title':title,'body':body,
                                                                      'flair':flair,'upvotes':upvotes,
                                                                      'downvotes':downvotes,'rewards_count':rewards_count,
                                                                      'url':url,'created_at':created_at}])])
            
            if after is None:
                break
            
            params['after'] = after
        
        return posts
    except Exception as e:
        raise

def write_to_s3():
    pass

# extract data from Reddit
@dag(default_args=default_args, schedule=None)
def extract_data_dag():

    @task(task_id = 'extract_data')
    def extract_data(subreddit='dataengineering', limit=10,file_name=f"reddit_{datetime.now().strftime('%Y%m%d')}"):
        posts = extract_posts(get_access_token(), subreddit, limit)
        print(len(posts))
        print(posts[:5])

    extract_data()


# instantiate the DAG 
extract_data_dag()

